"""
Content date extraction for ingestion documents.

Extracts timestamp_content from document text using multiple strategies:
collector headers, transcriber formats, filename patterns.
"""

import datetime
import logging
import re

from services.engines.ingestion._shared import HEADER_SCAN_CHARS

LOGGER = logging.getLogger(__name__)

# Timestamp patterns
STANDARD_TIMESTAMP_PATTERN = re.compile(r'^DATUM_TID:\s+(.+)$', re.MULTILINE)
TRANSCRIBER_DATE_PATTERN = re.compile(r'^DATUM:\s+(\d{4}-\d{2}-\d{2})$', re.MULTILINE)
TRANSCRIBER_START_PATTERN = re.compile(r'^START:\s+(\d{2}:\d{2})$', re.MULTILINE)
RICH_TRANSCRIBER_PATTERN = re.compile(
    r'^\*\*Tid:\*\*\s+(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})', re.MULTILINE
)


def extract_content_date(text: str, filename: str = None) -> str:
    """
    Extract timestamp_content - when the content actually happened.

    Extraction priority:
    1. DATUM_TID header (from collectors: Slack, Calendar, Gmail) -> ISO string
    2. Rich Transcriber format **Tid:** YYYY-MM-DD HH:MM:SS -> ISO string
    3. Legacy Transcriber format DATUM + START -> combined to ISO string
    4. Filename date pattern (YYYY-MM-DD, YYYYMMDD_HHMM, etc.) -> ISO string
    5. Otherwise -> "UNKNOWN"

    Returns:
        ISO format string or "UNKNOWN"
    """
    header_section = text[:HEADER_SCAN_CHARS]

    # 1. Try DATUM_TID (collectors: Slack, Calendar, Gmail)
    match = STANDARD_TIMESTAMP_PATTERN.search(header_section)
    if match:
        ts_str = match.group(1).strip()
        try:
            dt = datetime.datetime.fromisoformat(ts_str)
            LOGGER.debug(f"extract_content_date: DATUM_TID -> {dt.isoformat()}")
            return dt.isoformat()
        except ValueError:
            LOGGER.warning(f"extract_content_date: Invalid DATUM_TID '{ts_str}'")

    # 2. Try Rich Transcriber format (**Tid:** YYYY-MM-DD HH:MM:SS)
    rich_match = RICH_TRANSCRIBER_PATTERN.search(header_section)
    if rich_match:
        ts_str = rich_match.group(1).strip()
        try:
            dt = datetime.datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S")
            LOGGER.debug(f"extract_content_date: Rich Transcriber -> {dt.isoformat()}")
            return dt.isoformat()
        except ValueError:
            LOGGER.warning(f"extract_content_date: Invalid Rich Transcriber format '{ts_str}'")

    # 3. Try legacy Transcriber format (DATUM + START)
    date_match = TRANSCRIBER_DATE_PATTERN.search(header_section)
    start_match = TRANSCRIBER_START_PATTERN.search(header_section)

    if date_match and start_match:
        date_str = date_match.group(1)
        time_str = start_match.group(1)
        try:
            dt = datetime.datetime.strptime(f"{date_str} {time_str}", "%Y-%m-%d %H:%M")
            LOGGER.debug(f"extract_content_date: Transcriber -> {dt.isoformat()}")
            return dt.isoformat()
        except ValueError:
            LOGGER.warning(f"extract_content_date: Invalid Transcriber format '{date_str} {time_str}'")
    elif date_match:
        date_str = date_match.group(1)
        try:
            dt = datetime.datetime.strptime(f"{date_str} 12:00", "%Y-%m-%d %H:%M")
            LOGGER.debug(f"extract_content_date: Transcriber (date only) -> {dt.isoformat()}")
            return dt.isoformat()
        except ValueError as e:
            LOGGER.debug(f"extract_content_date: Could not parse date '{date_str}': {e}")

    # 4. Try filename date extraction
    if filename:
        from services.utils.date_service import GenericFilenameExtractor
        extractor = GenericFilenameExtractor()
        if extractor.can_extract(filename):
            dt = extractor.extract(filename)
            if dt:
                LOGGER.debug(f"extract_content_date: Filename -> {dt.isoformat()}")
                return dt.isoformat()

    # 5. No source found
    LOGGER.info(f"extract_content_date: No date source -> UNKNOWN ({filename or 'no filename'})")
    return "UNKNOWN"
