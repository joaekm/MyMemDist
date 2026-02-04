"""
LLMService - Central tjänst för alla LLM-anrop.

Hybrid-arkitektur (OBJEKT-85):
- Anthropic Claude: Default för text-prompts (enrichment, validation, etc.)
- Google Gemini: Transkribering (endast Gemini hanterar lasten)

Hanterar:
- Provider-routing baserat på task_type eller explicit provider-parameter
- Modellval baserat på task_type (pro/fast/lite)
- Parallella anrop för moln-LLM (batch_generate)
- Sekventiella anrop för lokal modell
- Adaptiv throttling per provider
- Retry-logik med exponential backoff
- Centraliserad felhantering och logging
"""

import os
import logging
import yaml
import time
import threading
from typing import List, Dict, Any, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass

from .providers import AnthropicProvider, GeminiProvider, BaseProvider, ProviderResponse

LOGGER = logging.getLogger("LLMService")


@dataclass
class LLMResponse:
    """Resultat från LLM-anrop."""
    text: str
    success: bool
    error: Optional[str] = None
    model: Optional[str] = None
    tokens_used: Optional[int] = None


class AdaptiveThrottler:
    """
    Adaptiv throttling för API-anrop.

    Ökar gradvis anrop/sekund tills rate limit träffas,
    backar sedan och stabiliserar på en säker nivå.
    """

    def __init__(
        self,
        initial_rps: float = 1.0,
        min_rps: float = 0.5,
        max_rps: float = 20.0,
        increase_factor: float = 1.2,
        decrease_factor: float = 0.6,
        stabilize_after: int = 5
    ):
        self.current_rps = initial_rps
        self.min_rps = min_rps
        self.max_rps = max_rps
        self.increase_factor = increase_factor
        self.decrease_factor = decrease_factor
        self.stabilize_after = stabilize_after

        self._lock = threading.Lock()
        self._last_call_time = 0.0
        self._consecutive_successes = 0
        self._stabilized = False
        self._stable_rps = None

    def wait(self):
        """Vänta rätt tid mellan anrop."""
        with self._lock:
            now = time.time()
            min_interval = 1.0 / self.current_rps
            elapsed = now - self._last_call_time

            if elapsed < min_interval:
                sleep_time = min_interval - elapsed
                time.sleep(sleep_time)

            self._last_call_time = time.time()

    def report_success(self):
        """Rapportera lyckat anrop - öka hastigheten gradvis."""
        with self._lock:
            if self._stabilized:
                return  # Redan stabil, ändra inte

            self._consecutive_successes += 1

            if self._consecutive_successes >= self.stabilize_after:
                # Öka RPS
                old_rps = self.current_rps
                self.current_rps = min(self.current_rps * self.increase_factor, self.max_rps)

                if self.current_rps != old_rps:
                    LOGGER.debug(f"Throttle: ökar RPS {old_rps:.2f} → {self.current_rps:.2f}")

                self._consecutive_successes = 0

    def report_rate_limit(self):
        """Rapportera rate limit - backa och stabilisera."""
        with self._lock:
            old_rps = self.current_rps
            self.current_rps = max(self.current_rps * self.decrease_factor, self.min_rps)

            # Markera som stabil på denna nivå
            self._stabilized = True
            self._stable_rps = self.current_rps
            self._consecutive_successes = 0

            LOGGER.info(f"Throttle: rate limit! Backar {old_rps:.2f} → {self.current_rps:.2f} RPS (stabiliserad)")

    def report_error(self):
        """Rapportera annat fel - återställ success-räknare."""
        with self._lock:
            self._consecutive_successes = 0

    def get_stats(self) -> dict:
        """Hämta aktuell status."""
        with self._lock:
            return {
                "current_rps": self.current_rps,
                "stabilized": self._stabilized,
                "stable_rps": self._stable_rps
            }


class LLMService:
    """
    Central tjänst för LLM-anrop med batch-stöd.

    Hybrid-arkitektur:
    - Anthropic Claude: Default för text-prompts
    - Google Gemini: Transkribering (explicit provider="gemini")
    """

    _instance = None

    def __new__(cls):
        """Singleton pattern."""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized:
            return

        self.config = self._load_config()

        # Providers (OBJEKT-85)
        self.providers: Dict[str, BaseProvider] = {}
        self._init_providers()

        # Default provider
        ai_config = self.config.get('ai_engine', {})
        self.default_provider = ai_config.get('default_provider', 'anthropic')
        self.transcription_provider = ai_config.get('transcription_provider', 'gemini')

        # Modeller (för defaults)
        self.models = self._load_models()

        # Adaptiv throttling per provider (OBJEKT-85)
        self.throttlers: Dict[str, AdaptiveThrottler] = {}
        self._init_throttlers()

        # Retry-inställningar
        self.max_parallel = 30  # Max parallella anrop mot moln
        self.retry_attempts = 3
        self.retry_delay = 1.0  # Sekunder mellan retries

        self._initialized = True
        LOGGER.info(f"LLMService initialized - default: {self.default_provider}, transcription: {self.transcription_provider}")

    def _load_config(self) -> dict:
        """Ladda konfiguration via central config loader."""
        from services.utils.config_loader import get_config
        try:
            return get_config()
        except FileNotFoundError as e:
            LOGGER.error(f"Config-fil saknas: {e}")
            raise RuntimeError(f"HARDFAIL: Config saknas") from e

    def _init_providers(self):
        """Initiera LLM providers (OBJEKT-85)."""
        ai_config = self.config.get('ai_engine', {})

        # Anthropic (default för text-prompts)
        anthropic_key = ai_config.get('anthropic', {}).get('api_key')
        if anthropic_key:
            self.providers['anthropic'] = AnthropicProvider(api_key=anthropic_key)
            LOGGER.info("AnthropicProvider initialized")

        # Gemini (för transkribering)
        gemini_key = ai_config.get('gemini', {}).get('api_key')
        if gemini_key:
            self.providers['gemini'] = GeminiProvider(api_key=gemini_key)
            LOGGER.info("GeminiProvider initialized")

        if not self.providers:
            LOGGER.error("HARDFAIL: Ingen LLM provider kunde initieras!")

    def _init_throttlers(self):
        """Initiera throttlers per provider (OBJEKT-85)."""
        ai_config = self.config.get('ai_engine', {})

        # Anthropic throttler
        anthropic_throttle = ai_config.get('anthropic', {}).get('throttling', {})
        self.throttlers['anthropic'] = AdaptiveThrottler(
            initial_rps=anthropic_throttle.get('initial_rps', 10.0),
            min_rps=anthropic_throttle.get('min_rps', 0.5),
            max_rps=anthropic_throttle.get('max_rps', 30.0),
            increase_factor=anthropic_throttle.get('increase_factor', 1.2),
            decrease_factor=anthropic_throttle.get('decrease_factor', 0.5),
            stabilize_after=anthropic_throttle.get('stabilize_after', 5)
        )

        # Gemini throttler
        gemini_throttle = ai_config.get('gemini', {}).get('throttling', {})
        self.throttlers['gemini'] = AdaptiveThrottler(
            initial_rps=gemini_throttle.get('initial_rps', 10.0),
            min_rps=gemini_throttle.get('min_rps', 0.5),
            max_rps=gemini_throttle.get('max_rps', 30.0),
            increase_factor=gemini_throttle.get('increase_factor', 1.2),
            decrease_factor=gemini_throttle.get('decrease_factor', 0.5),
            stabilize_after=gemini_throttle.get('stabilize_after', 5)
        )

        LOGGER.debug(f"Throttlers initialized: {list(self.throttlers.keys())}")

    def _load_models(self) -> dict:
        """Ladda modellnamn från config (används för defaults)."""
        models = self.config.get('ai_engine', {}).get('models', {})
        return {
            'pro': models.get('model_pro', 'claude-sonnet-4-5-20250514'),
            'fast': models.get('model_fast', 'claude-sonnet-4-5-20250514'),
            'lite': models.get('model_lite', 'claude-haiku-4-5-20250514'),
            'transcribe': models.get('model_transcribe', 'models/gemini-2.5-flash'),
        }

    def _get_provider(self, provider_name: Optional[str] = None) -> Optional[BaseProvider]:
        """Hämta provider by name (default: default_provider)."""
        name = provider_name or self.default_provider
        provider = self.providers.get(name)
        if not provider:
            LOGGER.warning(f"Provider '{name}' inte tillgänglig, försöker fallback")
            # Fallback till första tillgängliga
            if self.providers:
                fallback_name = next(iter(self.providers))
                LOGGER.warning(f"Använder fallback provider: {fallback_name}")
                return self.providers[fallback_name]
        return provider

    def _get_throttler(self, provider_name: str) -> AdaptiveThrottler:
        """Hämta throttler för provider."""
        return self.throttlers.get(provider_name, self.throttlers.get(self.default_provider))

    def _is_rate_limit_error(self, error: Exception, provider: Optional[BaseProvider] = None) -> bool:
        """Kolla om felet är rate limiting."""
        if provider:
            return provider.is_rate_limit_error(error)
        # Fallback för bakåtkompatibilitet
        error_str = str(error).lower()
        return any(keyword in error_str for keyword in [
            "rate limit", "quota", "429", "resource exhausted",
            "too many requests", "rate_limit"
        ])

    def generate(
        self,
        prompt: str,
        provider: Optional[str] = None,
        model: Optional[str] = None
    ) -> LLMResponse:
        """
        Generera svar för en prompt.

        Args:
            prompt: Prompten att skicka
            provider: Provider ("anthropic" eller "gemini"). Default: default_provider från config.
            model: Modellnamn. Default: model_lite från config.

        Returns:
            LLMResponse med text eller fel
        """
        # Bestäm provider (OBJEKT-85)
        if provider is None:
            provider = self.default_provider

        # Bestäm modell
        if model is None:
            model = self.models.get('lite', 'claude-haiku-4-5-20250514')

        llm_provider = self._get_provider(provider)
        if not llm_provider:
            return LLMResponse(text="", success=False, error="Ingen LLM-provider tillgänglig")

        throttler = self._get_throttler(provider)

        for attempt in range(self.retry_attempts):
            # Vänta enligt throttling
            throttler.wait()

            try:
                response = llm_provider.generate(prompt=prompt, model=model)

                # Kontrollera att svaret inte är tomt
                if not response.success or not response.text or not response.text.strip():
                    LOGGER.warning(f"LLM returnerade tomt svar för modell {model} via {provider}")
                    return LLMResponse(
                        text="",
                        success=False,
                        error=response.error or "LLM returnerade tomt svar",
                        model=model
                    )

                # Rapportera framgång till throttler
                throttler.report_success()

                return LLMResponse(
                    text=response.text,
                    success=True,
                    model=model
                )

            except (ConnectionError, TimeoutError) as e:
                # Nätverksfel - retry
                throttler.report_error()
                LOGGER.warning(f"LLM nätverksfel ({provider}, försök {attempt + 1}/{self.retry_attempts}): {e}")
                if attempt < self.retry_attempts - 1:
                    time.sleep(self.retry_delay * (attempt + 1))
                    continue
                return LLMResponse(text="", success=False, error=str(e), model=model)

        return LLMResponse(text="", success=False, error="Max retries exceeded")

    def batch_generate(
        self,
        prompts: List[str],
        provider: Optional[str] = None,
        model: Optional[str] = None,
        parallel: bool = True
    ) -> List[LLMResponse]:
        """
        Generera svar för flera prompts.

        Args:
            prompts: Lista med prompts
            provider: Provider ("anthropic" eller "gemini")
            model: Modellnamn
            parallel: True = parallellt (moln), False = sekventiellt (lokal)

        Returns:
            Lista med LLMResponse i samma ordning som prompts
        """
        if not prompts:
            return []

        if not parallel:
            # Sekventiell körning (för lokal modell)
            return [self.generate(p, provider, model) for p in prompts]

        # Parallell körning med ThreadPoolExecutor
        results = [None] * len(prompts)

        with ThreadPoolExecutor(max_workers=self.max_parallel) as executor:
            # Skapa futures med index för att bevara ordning
            future_to_idx = {
                executor.submit(self.generate, prompt, provider, model): idx
                for idx, prompt in enumerate(prompts)
            }

            for future in as_completed(future_to_idx):
                idx = future_to_idx[future]
                results[idx] = future.result()  # generate() returnerar alltid LLMResponse

        return results

    def generate_simple(self, prompt: str, provider: Optional[str] = None, model: Optional[str] = None) -> str:
        """
        Enkel wrapper för bakåtkompatibilitet.
        Returnerar bara texten (tomsträng vid fel).
        """
        response = self.generate(prompt, provider, model)
        return response.text if response.success else ""

    def generate_multi_turn(
        self,
        messages: List[Dict[str, str]],
        provider: Optional[str] = None,
        model: Optional[str] = None
    ) -> LLMResponse:
        """
        Generera svar med multi-turn konversation.

        Args:
            messages: Lista med meddelanden, t.ex.:
                [
                    {"role": "user", "content": "..."},
                    {"role": "assistant", "content": "..."},
                    {"role": "user", "content": "..."},
                ]
            provider: Provider ("anthropic" eller "gemini"). Default: default_provider.
            model: Modellnamn. Default: model_lite.

        Returns:
            LLMResponse med text eller fel
        """
        if provider is None:
            provider = self.default_provider

        if model is None:
            model = self.models.get('lite')

        llm_provider = self._get_provider(provider)
        if not llm_provider:
            return LLMResponse(text="", success=False, error="Ingen LLM-provider tillgänglig")

        # Kolla att provider stödjer multi-turn
        if not hasattr(llm_provider, 'generate_multi_turn'):
            LOGGER.warning(f"Provider {provider} stödjer inte multi-turn, faller tillbaka på generate()")
            # Fallback: bygg ihop till en prompt
            full_prompt = "\n\n".join(
                f"{m['role'].upper()}:\n{m['content']}" for m in messages
            )
            return self.generate(full_prompt, provider, model)

        throttler = self._get_throttler(provider)

        for attempt in range(self.retry_attempts):
            throttler.wait()

            try:
                response = llm_provider.generate_multi_turn(messages=messages, model=model)

                if not response.success or not response.text or not response.text.strip():
                    LOGGER.warning(f"Multi-turn returnerade tomt svar via {provider}")
                    return LLMResponse(
                        text="",
                        success=False,
                        error=response.error or "LLM returnerade tomt svar",
                        model=model
                    )

                throttler.report_success()
                return LLMResponse(
                    text=response.text,
                    success=True,
                    model=model
                )

            except (ConnectionError, TimeoutError) as e:
                # Nätverksfel - retry
                throttler.report_error()
                LOGGER.warning(f"Multi-turn nätverksfel ({provider}, försök {attempt + 1}/{self.retry_attempts}): {e}")
                if attempt < self.retry_attempts - 1:
                    time.sleep(self.retry_delay * (attempt + 1))
                    continue
                return LLMResponse(text="", success=False, error=str(e), model=model)

        return LLMResponse(text="", success=False, error="Max retries exceeded")

    def get_throttle_stats(self, provider: Optional[str] = None) -> dict:
        """Hämta aktuell throttling-status för en eller alla providers."""
        if provider:
            throttler = self.throttlers.get(provider)
            return throttler.get_stats() if throttler else {}

        # Returnera stats för alla providers
        return {name: t.get_stats() for name, t in self.throttlers.items()}

    def get_provider_stats(self) -> dict:
        """Hämta status för alla providers (OBJEKT-85)."""
        return {
            "providers": list(self.providers.keys()),
            "default_provider": self.default_provider,
            "transcription_provider": self.transcription_provider,
            "throttlers": self.get_throttle_stats()
        }

# Bakåtkompatibilitet - alias för enkel import
def get_llm_service() -> LLMService:
    """Hämta singleton-instans av LLMService."""
    return LLMService()
