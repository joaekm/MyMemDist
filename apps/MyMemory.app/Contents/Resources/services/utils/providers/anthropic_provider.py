"""
AnthropicProvider - Anthropic Claude för text-prompts.

Default provider i hybrid-arkitekturen.
Används för entity extraction, critic, metadata, dreamer.
"""

import logging
from typing import Optional

import anthropic

from .base_provider import BaseProvider, ProviderResponse


LOGGER = logging.getLogger("AnthropicProvider")


class AnthropicProvider(BaseProvider):
    """Anthropic Claude provider för text-prompts."""

    def __init__(self, api_key: str):
        """
        Initiera Anthropic-klient.

        Args:
            api_key: Anthropic API-nyckel
        """
        self.client = anthropic.Anthropic(api_key=api_key)
        LOGGER.info("AnthropicProvider initialized")

    @property
    def name(self) -> str:
        return "anthropic"

    def generate(self, prompt: str, model: str) -> ProviderResponse:
        """
        Generera svar via Claude.

        Args:
            prompt: Prompten att skicka
            model: Modellnamn (t.ex. "claude-sonnet-4-5-20250514")

        Returns:
            ProviderResponse med text och status
        """
        try:
            response = self.client.messages.create(
                model=model,
                max_tokens=8192,
                messages=[{"role": "user", "content": prompt}]
            )

            # Extrahera text och token usage från response
            input_tok = getattr(response.usage, 'input_tokens', 0) if response.usage else 0
            output_tok = getattr(response.usage, 'output_tokens', 0) if response.usage else 0

            if response.content and len(response.content) > 0:
                text = response.content[0].text
                return ProviderResponse(
                    text=text,
                    success=True,
                    model=model,
                    input_tokens=input_tok,
                    output_tokens=output_tok
                )
            else:
                return ProviderResponse(
                    text="",
                    success=False,
                    error="Empty response from Claude",
                    model=model,
                    input_tokens=input_tok,
                    output_tokens=output_tok
                )

        except anthropic.RateLimitError as e:
            LOGGER.warning(f"Claude rate limit: {e}")
            return ProviderResponse(
                text="",
                success=False,
                error=f"Rate limit: {e}",
                model=model
            )

        except anthropic.APIError as e:
            LOGGER.warning(f"Claude API error: {e}")
            return ProviderResponse(
                text="",
                success=False,
                error=str(e),
                model=model
            )

        except Exception as e:
            LOGGER.warning(f"Claude generate error: {e}")
            return ProviderResponse(
                text="",
                success=False,
                error=str(e),
                model=model
            )

    def generate_multi_turn(self, messages: list, model: str) -> ProviderResponse:
        """
        Generera svar via Claude med multi-turn konversation.

        Args:
            messages: Lista med meddelanden, t.ex.:
                [
                    {"role": "user", "content": "..."},
                    {"role": "assistant", "content": "..."},
                    {"role": "user", "content": "..."},
                ]
            model: Modellnamn (t.ex. "claude-sonnet-4-5-20250514")

        Returns:
            ProviderResponse med text och status
        """
        try:
            response = self.client.messages.create(
                model=model,
                max_tokens=8192,
                messages=messages
            )

            input_tok = getattr(response.usage, 'input_tokens', 0) if response.usage else 0
            output_tok = getattr(response.usage, 'output_tokens', 0) if response.usage else 0

            if response.content and len(response.content) > 0:
                text = response.content[0].text
                return ProviderResponse(
                    text=text,
                    success=True,
                    model=model,
                    input_tokens=input_tok,
                    output_tokens=output_tok
                )
            else:
                return ProviderResponse(
                    text="",
                    success=False,
                    error="Empty response from Claude",
                    model=model,
                    input_tokens=input_tok,
                    output_tokens=output_tok
                )

        except anthropic.RateLimitError as e:
            LOGGER.warning(f"Claude rate limit: {e}")
            return ProviderResponse(
                text="",
                success=False,
                error=f"Rate limit: {e}",
                model=model
            )

        except anthropic.APIError as e:
            LOGGER.warning(f"Claude API error: {e}")
            return ProviderResponse(
                text="",
                success=False,
                error=str(e),
                model=model
            )

        except Exception as e:
            LOGGER.warning(f"Claude generate_multi_turn error: {e}")
            return ProviderResponse(
                text="",
                success=False,
                error=str(e),
                model=model
            )

    def is_rate_limit_error(self, error: Exception) -> bool:
        """Kolla om felet är rate limiting."""
        if isinstance(error, anthropic.RateLimitError):
            return True

        error_str = str(error).lower()
        return any(kw in error_str for kw in [
            "rate_limit",
            "rate limit",
            "429",
            "too_many_requests",
            "too many requests"
        ])
