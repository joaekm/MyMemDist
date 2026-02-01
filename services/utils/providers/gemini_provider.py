"""
GeminiProvider - Google Gemini för text-prompts.

Används som fallback i hybrid-arkitekturen.
Transkribering hanteras separat i transcriber.py.
"""

import logging
from typing import Optional

from google import genai
from google.genai import types

from .base_provider import BaseProvider, ProviderResponse


LOGGER = logging.getLogger("GeminiProvider")


class GeminiProvider(BaseProvider):
    """Google Gemini provider för text-prompts."""

    def __init__(self, api_key: str):
        """
        Initiera Gemini-klient.

        Args:
            api_key: Google AI API-nyckel
        """
        self.client = genai.Client(api_key=api_key)
        LOGGER.info("GeminiProvider initialized")

    @property
    def name(self) -> str:
        return "gemini"

    def generate(self, prompt: str, model: str) -> ProviderResponse:
        """
        Generera svar via Gemini.

        Args:
            prompt: Prompten att skicka
            model: Modellnamn (t.ex. "models/gemini-2.5-flash")

        Returns:
            ProviderResponse med text och status
        """
        try:
            response = self.client.models.generate_content(
                model=model,
                contents=[types.Content(
                    role="user",
                    parts=[types.Part.from_text(text=prompt)]
                )]
            )

            if response.text:
                return ProviderResponse(
                    text=response.text,
                    success=True,
                    model=model
                )
            else:
                return ProviderResponse(
                    text="",
                    success=False,
                    error="Empty response from Gemini",
                    model=model
                )

        except Exception as e:
            LOGGER.warning(f"Gemini generate error: {e}")
            return ProviderResponse(
                text="",
                success=False,
                error=str(e),
                model=model
            )

    def is_rate_limit_error(self, error: Exception) -> bool:
        """Kolla om felet är rate limiting."""
        error_str = str(error).lower()
        return any(kw in error_str for kw in [
            "rate limit",
            "quota",
            "429",
            "resource exhausted",
            "too many requests"
        ])
