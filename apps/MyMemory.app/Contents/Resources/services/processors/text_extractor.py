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
    import openpyxl
except ImportError as e:
    raise ImportError(
        f"Missing required libraries (pymupdf, python-docx, openpyxl): {e}"
    )

from services.utils.config_loader import get_config

LOGGER = logging.getLogger('TextExtractor')

_DOCUMENT_EXTENSIONS = None

def _get_document_extensions() -> set:
    """Load document_extensions from config (cached)."""
    global _DOCUMENT_EXTENSIONS
    if _DOCUMENT_EXTENSIONS is None:
        config = get_config()
        _DOCUMENT_EXTENSIONS = set(config.get('processing', {}).get('document_extensions', []))
    return _DOCUMENT_EXTENSIONS


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

    # Binära format kräver format-specifika extractors. Textbaserade format
    # läses direkt. Okänd extension HARDFAILar — tyst binärläsning har
    # tidigare gett junk-text som servern avvisat med 500.
    _BINARY_EXTRACTORS = {
        '.pdf': _extract_pdf,
        '.docx': _extract_docx,
        '.xlsx': _extract_xlsx,
        '.xls': _extract_xlsx,
    }

    try:
        if extension in _BINARY_EXTRACTORS:
            raw_text = _BINARY_EXTRACTORS[extension](filepath)
        elif extension in _get_document_extensions():
            raw_text = _extract_plain_text(filepath)
        else:
            raise RuntimeError(
                f"Unsupported file type: {extension} "
                f"(not in document_extensions config, no binary extractor). "
                f"Lägg till extension i my_mem_config.yaml processing.document_extensions "
                f"eller implementera extractor i text_extractor.py."
            )
    except RuntimeError:
        raise
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


def _extract_xlsx(filepath: str) -> str:
    """Extract text from XLSX/XLS spreadsheets.

    Konkatenerar alla worksheets. Varje rad blir tab-separerade celler,
    tomma celler blir tomma strängar. Formler läses som beräknade värden
    (data_only=True). Bevarar enkel struktur så LLM kan läsa tabeller.
    """
    wb = openpyxl.load_workbook(filepath, data_only=True, read_only=True)
    sheets = []
    try:
        for sheet in wb.worksheets:
            rows = []
            for row in sheet.iter_rows(values_only=True):
                cells = ["" if v is None else str(v) for v in row]
                rows.append("\t".join(cells))
            if rows:
                sheets.append(f"# {sheet.title}\n" + "\n".join(rows))
    finally:
        wb.close()
    return "\n\n".join(sheets)


def get_supported_extensions() -> list:
    """Return list of supported file extensions (from config)."""
    return sorted(_get_document_extensions())
