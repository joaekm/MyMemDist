"""
BaseProvider - ABC för LLM providers.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional


@dataclass
class ProviderResponse:
    """Svar från en LLM provider."""
    text: str
    success: bool
    error: Optional[str] = None
    model: Optional[str] = None
    input_tokens: int = 0
    output_tokens: int = 0


class BaseProvider(ABC):
    """Abstrakt basklass för LLM providers."""

    @abstractmethod
    def generate(self, prompt: str, model: str) -> ProviderResponse:
        """
        Generera svar från text-prompt.

        Args:
            prompt: Prompten att skicka
            model: Modellnamn

        Returns:
            ProviderResponse med text och status
        """
        pass

    @abstractmethod
    def is_rate_limit_error(self, error: Exception) -> bool:
        """
        Identifiera om ett fel är rate limiting.

        Args:
            error: Exception att kolla

        Returns:
            True om det är rate limit-fel
        """
        pass

    @property
    @abstractmethod
    def name(self) -> str:
        """Provider-namn för loggning."""
        pass
