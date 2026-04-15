"""
Unified LLM + JSON pipeline.

Consolidates duplicated _call_llm_json() from enrichment and dreamer
into a single reusable function.

Usage:
    from services.utils.llm_json import call_llm_json, repair_truncated_json

    result = call_llm_json(llm_service, prompt, model_id, "enrich")
"""

import json
import logging
from typing import Optional, Dict

LOGGER = logging.getLogger(__name__)


def repair_truncated_json(text: str) -> str:
    """Try to repair truncated JSON by closing open structures.

    Scans backwards for the last '}', then balances brackets/braces.

    Raises:
        ValueError: If repair is impossible.
    """
    for i in range(len(text) - 1, 0, -1):
        if text[i] == "}":
            attempt = text[:i + 1]
            open_brackets = attempt.count("[") - attempt.count("]")
            open_braces = attempt.count("{") - attempt.count("}")
            suffix = "]" * open_brackets + "}" * open_braces
            try:
                json.loads(attempt + suffix)
                return attempt + suffix
            except json.JSONDecodeError:
                continue
    raise ValueError(f"Could not repair truncated JSON (len={len(text)})")


def call_llm_json(llm_service, prompt: str, model_id: str, step_name: str,
                   max_output_tokens: int = 32768) -> Optional[Dict]:
    """Call LLM and return parsed JSON result or None.

    Pipeline: generate -> clean markdown fences -> parse JSON -> repair if truncated.

    Args:
        llm_service: LLMService instance
        prompt: Full prompt text
        model_id: Model identifier (e.g. from llm_service.models['fast'])
        step_name: Name for logging (e.g. "enrich", "merge_eval")
        max_output_tokens: Max output tokens for LLM call

    Returns:
        Parsed dict, or None on failure.
    """
    response = llm_service.generate(
        prompt=prompt,
        model=model_id,
        max_tokens=max_output_tokens
    )

    if not response.success:
        LOGGER.error(f"LLM {step_name} failed: {response.error}")
        return None

    try:
        cleaned = response.text.replace("```json", "").replace("```", "").strip()
        result = json.loads(cleaned)
        return result
    except json.JSONDecodeError as e:
        LOGGER.error(f"LLM {step_name} JSON parse failed: {e}")
        try:
            repaired = repair_truncated_json(cleaned)
            result = json.loads(repaired)
            LOGGER.warning(f"LLM {step_name}: repaired truncated JSON")
            return result
        except (json.JSONDecodeError, ValueError):
            LOGGER.error(f"LLM {step_name}: could not repair JSON")
            return None
