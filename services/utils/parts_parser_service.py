"""
Parts Parser Service - Extrahera delar från strukturerade dokument.

Använder samma mönster som audio_service.py:
- Dataclasses för strukturerad data
- Funktioner för logik
- Ingen klasshierarki

Stödjer för närvarande:
- Rich Transcriber-format (## Del N: Titel + **Tid:** HH:MM:SS - HH:MM:SS)

Framtida utökningar kan läggas till som separata extract_*_parts() funktioner.
"""

import re
import logging
from dataclasses import dataclass
from typing import List, Optional

LOGGER = logging.getLogger("PartsParserService")


@dataclass
class DocumentPart:
    """En del av ett dokument."""
    part_number: int
    title: str
    content: str
    time_start: Optional[str] = None  # HH:MM:SS (för transkript)
    time_end: Optional[str] = None    # HH:MM:SS (för transkript)
    summary: Optional[str] = None     # Ingress/sammanfattning


# Strikt mönster som matchar exakt transcriber-output:
# ## Del N: Titel
# **Tid:** HH:MM:SS - HH:MM:SS
_TRANSCRIPT_PART_PATTERN = re.compile(
    r'^## Del (\d+):\s*(.+)\n'
    r'\*\*Tid:\*\*\s*(\d{1,2}:\d{2}:\d{2})\s*-\s*(\d{1,2}:\d{2}:\d{2})',
    re.MULTILINE
)


def has_transcript_parts(text: str) -> bool:
    """
    Snabbkoll om texten har Rich Transcriber Del-struktur.

    Använd denna för att avgöra om extract_transcript_parts() ska anropas.
    """
    return bool(_TRANSCRIPT_PART_PATTERN.search(text))


def extract_transcript_parts(text: str) -> List[DocumentPart]:
    """
    Extrahera delar från Rich Transcriber-format.

    Kräver EXAKT matchning av transcriber-output:
    ## Del N: Titel
    **Tid:** HH:MM:SS - HH:MM:SS
    *Sammanfattning* (valfritt)
    [transkripttext]

    Returnerar tom lista om inga delar hittas.
    """
    parts = []
    matches = list(_TRANSCRIPT_PART_PATTERN.finditer(text))

    if not matches:
        return parts

    for i, match in enumerate(matches):
        part_number = int(match.group(1))
        title = match.group(2).strip()
        time_start = match.group(3)
        time_end = match.group(4)

        # Hitta start och slut för denna del (efter tid-raden)
        start_pos = match.end()
        end_pos = matches[i + 1].start() if i + 1 < len(matches) else len(text)

        part_content = text[start_pos:end_pos].strip()

        # Extrahera sammanfattning (kursiv text, första raden efter tid)
        summary = ""
        summary_match = re.match(r'^\s*\*([^*]+)\*', part_content)
        if summary_match:
            summary = summary_match.group(1).strip()

        # Transkripttext: allt efter sammanfattningen
        transcript_lines = []
        past_summary = False
        for line in part_content.split('\n'):
            line_stripped = line.strip()
            # Hoppa över sammanfattning (första kursiva raden)
            if not past_summary and line_stripped.startswith('*') and line_stripped.endswith('*'):
                past_summary = True
                continue
            if line_stripped:
                transcript_lines.append(line)
                past_summary = True

        content = '\n'.join(transcript_lines).strip()

        parts.append(DocumentPart(
            part_number=part_number,
            title=title,
            content=content,
            time_start=time_start,
            time_end=time_end,
            summary=summary
        ))

    LOGGER.debug(f"Extraherade {len(parts)} delar från transkript")
    return parts


def build_chunk_text(part: DocumentPart) -> str:
    """
    Bygg sökbar text för en del.

    Inkluderar:
    - Del-titel
    - Tidsintervall (om finns)
    - Sammanfattning (om finns)
    - Innehåll (trunkerat för embedding-kvalitet)
    """
    lines = []

    lines.append(f"Del {part.part_number}: {part.title}")

    if part.time_start and part.time_end:
        lines.append(f"Tid: {part.time_start} - {part.time_end}")

    if part.summary:
        lines.append(f"Sammanfattning: {part.summary}")

    # Begränsa innehåll för embedding-kvalitet
    content = part.content
    if len(content) > 2000:
        content = content[:2000] + "..."

    if content:
        lines.append(content)

    return '\n'.join(lines)
