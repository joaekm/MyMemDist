"""
Name matching utilities for entity resolution and merge discovery.

Pure functions for name normalization, tokenization, and quality assessment.
Extracted from dreamer.py for shared use across engines.

Usage:
    from services.utils.name_matching import fold_diacritics, tokenize, is_weak_name
"""

import re
import unicodedata


def fold_diacritics(s: str) -> str:
    """Fold diacritics: Ohlen -> ohlen, Bjorkengren -> bjorkengren."""
    return unicodedata.normalize('NFD', s).encode('ascii', 'ignore').decode().lower()


def tokenize(name: str) -> set:
    """Split name into lowercase folded tokens."""
    return set(fold_diacritics(name).split())


def is_single_token(name: str) -> bool:
    """Check if name is a single word."""
    return len(name.strip().split()) == 1


def is_weak_name(name: str) -> bool:
    """Identify UUIDs, generic placeholders, or single-token names."""
    patterns = [
        r'^[0-9a-f]{8}-[0-9a-f]{4}',
        r'^(Talare|Speaker) \d+$',
        r'^Unknown$',
        r'^Unit_.*$',
    ]
    if is_single_token(name):
        return True
    return any(re.match(p, name, re.I) for p in patterns)
