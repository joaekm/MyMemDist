"""
LLM Providers - Abstraktion för olika AI-providers.

Providers:
- GeminiProvider: Google Gemini (text-prompts, fallback)
- AnthropicProvider: Anthropic Claude (text-prompts, default)
"""

from .base_provider import BaseProvider, ProviderResponse
from .gemini_provider import GeminiProvider
from .anthropic_provider import AnthropicProvider

__all__ = [
    'BaseProvider',
    'ProviderResponse',
    'GeminiProvider',
    'AnthropicProvider',
]
