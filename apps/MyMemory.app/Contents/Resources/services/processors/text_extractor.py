#!/usr/bin/env python3
"""
Text Extractor - Pure file conversion module.

Converts various file formats (PDF, DOCX, TXT, MD, CSV) to plain text.
Part of the Collect & Normalize phase.

No knowledge of Lake, Graf, or Vector - just file → text conversion.
"""

import os
import logging

# Dependencies
try:
    import fitz  # pymupdf
    import docx
except ImportError as e:
    raise ImportError(f"Missing required libraries (pymupdf, python-docx): {e}")

LOGGER = logging.getLogger('TextExtractor')


def extract_text(filepath: str, extension: str = None) -> str:
    """
    Extract raw text from a file.

    Args:
        filepath: Path to the file
        extension: Optional file extension override (e.g. '.pdf')

    Returns:
        Extracted text content

    Raises:
        RuntimeError: If extraction fails
        FileNotFoundError: If file doesn't exist
    """
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"File not found: {filepath}")

    if not extension:
        extension = os.path.splitext(filepath)[1].lower()
    else:
        extension = extension.lower()

    raw_text = ""

    try:
        if extension == '.pdf':
            raw_text = _extract_pdf(filepath)
        elif extension in ['.txt', '.md', '.json', '.csv']:
            raw_text = _extract_plain_text(filepath)
        elif extension == '.docx':
            raw_text = _extract_docx(filepath)
        else:
            LOGGER.warning(f"Unsupported file type: {extension}, attempting plain text")
            raw_text = _extract_plain_text(filepath)

    except Exception as e:
        LOGGER.error(f"HARDFAIL: Text extraction failed for {filepath}: {e}")
        raise RuntimeError(f"Text extraction failed for {filepath}: {e}") from e

    return raw_text


def _extract_pdf(filepath: str) -> str:
    """Extract text from PDF using PyMuPDF."""
    text_parts = []
    with fitz.open(filepath) as doc:
        for page in doc:
            text_parts.append(page.get_text())
    return "\n".join(text_parts)


def _extract_plain_text(filepath: str) -> str:
    """Extract text from plain text files."""
    with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
        return f.read()


def _extract_docx(filepath: str) -> str:
    """Extract text from DOCX files."""
    doc = docx.Document(filepath)
    return "\n".join([p.text for p in doc.paragraphs])


def get_supported_extensions() -> list:
    """Return list of supported file extensions."""
    return ['.pdf', '.docx', '.txt', '.md', '.json', '.csv']
