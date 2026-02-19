"""
Parts Parser Service - Extrahera delar från strukturerade dokument.

Använder samma mönster som audio_service.py:
- Dataclasses för strukturerad data
- Funktioner för logik
- Ingen klasshierarki

Stödjer:
- Rich Transcriber-format (## Del N: Titel + **Tid:** HH:MM:SS - HH:MM:SS)
- Slack Log-chunkning (meddelande-timestamps)
- Email Thread-chunkning (mail-gränser)
- Generisk dokument-chunkning (rubriker/paragrafer/fixed-size)
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


# ============================================================
# Chunkning för icke-transkript dokument (OBJEKT-100)
# ============================================================

# Regex: Slack-meddelande med timestamp [HH:MM]
_SLACK_MSG_PATTERN = re.compile(r'^\[(\d{2}:\d{2})\]\s+', re.MULTILINE)

# Regex: Email-gränser
_EMAIL_BOUNDARY_PATTERNS = [
    re.compile(r'^On .+wrote:\s*$', re.MULTILINE),
    re.compile(r'^\*From:\*\s+', re.MULTILINE),
    re.compile(r'^From:\s+.+@', re.MULTILINE),
]

# Regex: Markdown-rubriker
_HEADING_PATTERN = re.compile(r'^(#{1,4})\s+(.+)', re.MULTILINE)

# Min storlek för att behålla en chunk (filtrerar bort signaturer, tomma segment)
_MIN_CHUNK_SIZE = 50


def _merge_short_chunks(parts: List['DocumentPart']) -> List['DocumentPart']:
    """Slå ihop chunks som är under _MIN_CHUNK_SIZE med nästa (eller föregående)."""
    if not parts:
        return parts

    merged = []
    i = 0
    while i < len(parts):
        p = parts[i]
        if len(p.content) < _MIN_CHUNK_SIZE:
            # Försök slå ihop med nästa
            if i + 1 < len(parts):
                nxt = parts[i + 1]
                parts[i + 1] = DocumentPart(
                    part_number=nxt.part_number,
                    title=f"{p.time_start or p.title} - {nxt.time_end or nxt.title}" if p.time_start else nxt.title,
                    content=p.content + '\n\n' + nxt.content,
                    time_start=p.time_start or nxt.time_start,
                    time_end=nxt.time_end or p.time_end,
                )
            elif merged:
                # Slå ihop med föregående
                prev = merged[-1]
                merged[-1] = DocumentPart(
                    part_number=prev.part_number,
                    title=f"{prev.time_start or prev.title} - {p.time_end or p.title}" if prev.time_start else prev.title,
                    content=prev.content + '\n\n' + p.content,
                    time_start=prev.time_start or p.time_start,
                    time_end=p.time_end or prev.time_end,
                )
            else:
                merged.append(p)
        else:
            merged.append(p)
        i += 1

    # Renumrera
    for i, p in enumerate(merged):
        p.part_number = i + 1

    return merged


def _strip_metadata_header(text: str) -> str:
    """Strippa METADATA-header (allt mellan första och sista ===-block)."""
    lines = text.split('\n')
    first_eq = None
    last_eq = None
    for i, line in enumerate(lines):
        if line.startswith('=' * 10):
            if first_eq is None:
                first_eq = i
            last_eq = i

    if first_eq is not None and last_eq is not None and last_eq > first_eq:
        remaining = lines[last_eq + 1:]
        return '\n'.join(remaining).strip()

    return text


def _split_fixed_size(text: str, chunk_size: int, overlap: int) -> List[str]:
    """Generisk fixed-size splitter med overlap. Bryter vid ordgränser."""
    if len(text) <= chunk_size:
        return [text]

    chunks = []
    start = 0
    while start < len(text):
        end = start + chunk_size

        # Bryt vid ordgräns
        if end < len(text):
            break_pos = text.rfind(' ', start + chunk_size // 2, end)
            if break_pos > start:
                end = break_pos

        chunk = text[start:end].strip()
        if chunk:
            chunks.append(chunk)
        start = end - overlap

    return chunks


def chunk_slack_log(text: str, chunk_size: int, overlap: int) -> List[DocumentPart]:
    """
    Chunka Slack-logg baserat på meddelande-timestamps.

    Identifierar [HH:MM]-mönster och grupperar meddelanden (inkl trådsvar)
    i chunks av ca chunk_size tecken.
    """
    body = _strip_metadata_header(text)
    if not body:
        return []

    # Hitta alla top-level meddelanden (ej trådsvar med ↳)
    msg_starts = []
    for match in _SLACK_MSG_PATTERN.finditer(body):
        line_start = body.rfind('\n', 0, match.start()) + 1
        prefix = body[line_start:match.start()]
        if '↳' not in prefix:
            msg_starts.append((match.start(), match.group(1)))

    if not msg_starts:
        return []

    # Bygg meddelande-block (meddelande + trådsvar)
    messages = []
    for i, (start, timestamp) in enumerate(msg_starts):
        end = msg_starts[i + 1][0] if i + 1 < len(msg_starts) else len(body)
        msg_text = body[start:end].strip()
        messages.append((timestamp, msg_text))

    # Gruppera meddelanden i chunks
    parts = []
    current_texts = []
    current_size = 0
    first_time = None
    last_time = None

    for timestamp, msg_text in messages:
        if first_time is None:
            first_time = timestamp

        # Om ett enstaka meddelande > chunk_size: splitta det
        if len(msg_text) > chunk_size and not current_texts:
            sub_chunks = _split_fixed_size(msg_text, chunk_size, overlap)
            for j, sub in enumerate(sub_chunks):
                parts.append(DocumentPart(
                    part_number=len(parts) + 1,
                    title=f"{timestamp}",
                    content=sub,
                    time_start=timestamp,
                    time_end=timestamp,
                ))
            first_time = None
            continue

        # Om tillägg överskrider chunk_size: spara och börja ny
        if current_size + len(msg_text) > chunk_size and current_texts:
            parts.append(DocumentPart(
                part_number=len(parts) + 1,
                title=f"{first_time} - {last_time}",
                content='\n\n'.join(current_texts),
                time_start=first_time,
                time_end=last_time,
            ))
            current_texts = []
            current_size = 0
            first_time = timestamp

        current_texts.append(msg_text)
        current_size += len(msg_text)
        last_time = timestamp

    # Sista chunken
    if current_texts:
        last_content = '\n\n'.join(current_texts)
        # Om sista chunken är för kort och det finns en föregående: slå ihop
        if len(last_content) < _MIN_CHUNK_SIZE and parts:
            prev = parts[-1]
            parts[-1] = DocumentPart(
                part_number=prev.part_number,
                title=f"{prev.time_start} - {last_time}",
                content=prev.content + '\n\n' + last_content,
                time_start=prev.time_start,
                time_end=last_time,
            )
        else:
            parts.append(DocumentPart(
                part_number=len(parts) + 1,
                title=f"{first_time} - {last_time}",
                content=last_content,
                time_start=first_time,
                time_end=last_time,
            ))

    # Post-process: slå ihop korta chunks med nästa eller föregående
    parts = _merge_short_chunks(parts)

    LOGGER.debug(f"Slack chunkning: {len(parts)} delar")
    return parts


def chunk_email_thread(text: str, chunk_size: int, overlap: int) -> List[DocumentPart]:
    """
    Chunka email-tråd baserat på mail-gränser.

    Detekterar 'On ... wrote:', '*From:*' och 'From: ..@' mönster.
    Varje mail i tråden blir en chunk. Stora mail fixed-size-splittas.
    """
    body = _strip_metadata_header(text)
    if not body:
        return []

    # Hitta alla email-gränser
    boundaries = []
    for pattern in _EMAIL_BOUNDARY_PATTERNS:
        for match in pattern.finditer(body):
            boundaries.append(match.start())

    boundaries = sorted(set(boundaries))

    if not boundaries:
        raw_chunks = _split_fixed_size(body, chunk_size, overlap)
        return [
            DocumentPart(part_number=i + 1, title=f"Del {i + 1}", content=chunk)
            for i, chunk in enumerate(raw_chunks)
            if len(chunk) >= _MIN_CHUNK_SIZE
        ]

    # Splitta på gränserna
    segments = []
    first_segment = body[:boundaries[0]].strip()
    if first_segment:
        segments.append(("", first_segment))

    for i, boundary in enumerate(boundaries):
        end = boundaries[i + 1] if i + 1 < len(boundaries) else len(body)
        segment = body[boundary:end].strip()
        if segment:
            # Extrahera avsändare
            sender = ""
            from_match = re.search(r'(?:From:|from:|\*From:\*)\s*(.+?)(?:\n|<)', segment)
            if from_match:
                sender = from_match.group(1).strip()
            else:
                wrote_match = re.search(r'On .+?(\w[\w\s.]+)<', segment)
                if wrote_match:
                    sender = wrote_match.group(1).strip()
            segments.append((sender, segment))

    # Bygg chunks, splitta stora segment
    parts = []
    for sender, segment in segments:
        title = sender if sender else f"Del {len(parts) + 1}"

        if len(segment) < _MIN_CHUNK_SIZE:
            continue

        if len(segment) > chunk_size:
            sub_chunks = _split_fixed_size(segment, chunk_size, overlap)
            for j, sub in enumerate(sub_chunks):
                if len(sub) < _MIN_CHUNK_SIZE:
                    continue
                suffix = f" ({j + 1}/{len(sub_chunks)})" if len(sub_chunks) > 1 else ""
                parts.append(DocumentPart(
                    part_number=len(parts) + 1,
                    title=f"{title}{suffix}",
                    content=sub,
                ))
        else:
            parts.append(DocumentPart(
                part_number=len(parts) + 1,
                title=title,
                content=segment,
            ))

    LOGGER.debug(f"Email chunkning: {len(parts)} delar")
    return parts


def chunk_generic_document(text: str, chunk_size: int, overlap: int) -> List[DocumentPart]:
    """
    Chunka generiskt dokument: rubriker → paragrafer → fixed-size.

    Försöker splitta på markdown-rubriker först.
    Faller tillbaka på paragrafer (dubbla newlines).
    Sista fallback: fixed-size med overlap.
    """
    body = _strip_metadata_header(text)
    if not body:
        return []

    # Steg 1: Försök splitta på markdown-rubriker
    headings = list(_HEADING_PATTERN.finditer(body))

    if headings:
        segments = []
        # Prefix-text innan första rubriken
        if headings[0].start() > 0:
            prefix = body[:headings[0].start()].strip()
            if prefix and len(prefix) >= _MIN_CHUNK_SIZE:
                segments.append(("Inledning", prefix))

        for i, match in enumerate(headings):
            start = match.start()
            end = headings[i + 1].start() if i + 1 < len(headings) else len(body)
            title = match.group(2).strip()[:60]
            content = body[start:end].strip()
            segments.append((title, content))
    else:
        # Steg 2: Splitta på paragrafer (dubbla newlines)
        raw_segments = re.split(r'\n\s*\n', body)
        segments = [
            (f"Del {i + 1}", seg.strip())
            for i, seg in enumerate(raw_segments)
            if seg.strip() and len(seg.strip()) >= _MIN_CHUNK_SIZE
        ]

    if not segments:
        # Steg 3: Sista fallback — ren fixed-size
        raw_chunks = _split_fixed_size(body, chunk_size, overlap)
        return [
            DocumentPart(part_number=i + 1, title=f"Del {i + 1}", content=chunk)
            for i, chunk in enumerate(raw_chunks)
            if len(chunk) >= _MIN_CHUNK_SIZE
        ]

    # Sammanslå små + splitta stora
    merged = []
    current_title = None
    current_content = []
    current_size = 0

    for title, content in segments:
        # Stora segment: splitta
        if len(content) > chunk_size:
            if current_content:
                merged.append((current_title, '\n\n'.join(current_content)))
                current_content = []
                current_size = 0
                current_title = None

            sub_chunks = _split_fixed_size(content, chunk_size, overlap)
            for j, sub in enumerate(sub_chunks):
                if len(sub) < _MIN_CHUNK_SIZE:
                    continue
                suffix = f" ({j + 1})" if len(sub_chunks) > 1 else ""
                merged.append((f"{title}{suffix}", sub))
            continue

        # Om tillägg överskrider chunk_size: spara och börja ny
        if current_size + len(content) > chunk_size and current_content:
            merged.append((current_title, '\n\n'.join(current_content)))
            current_content = []
            current_size = 0
            current_title = None

        # Filtrera bort minimala segment
        if len(content) < _MIN_CHUNK_SIZE:
            continue

        if current_title is None:
            current_title = title
        current_content.append(content)
        current_size += len(content)

    if current_content:
        merged.append((current_title, '\n\n'.join(current_content)))

    parts = [
        DocumentPart(part_number=i + 1, title=title, content=content)
        for i, (title, content) in enumerate(merged)
    ]

    LOGGER.debug(f"Dokument chunkning: {len(parts)} delar")
    return parts


def chunk_document(raw_text: str, source_type: str, chunk_size: int,
                   chunk_overlap: int, chunk_threshold: int) -> Optional[List[DocumentPart]]:
    """
    Dispatch: välj chunkning-strategi baserat på source_type.

    Returnerar None om dokumentet inte ska chunkas (för kort, Calendar Event,
    Transcript, eller om chunkningen bara ger 0-1 delar).
    """
    if source_type in ("Calendar Event", "Transcript"):
        return None

    if len(raw_text) <= chunk_threshold:
        return None

    if source_type == "Slack Log":
        parts = chunk_slack_log(raw_text, chunk_size, chunk_overlap)
    elif source_type == "Email Thread":
        parts = chunk_email_thread(raw_text, chunk_size, chunk_overlap)
    else:
        parts = chunk_generic_document(raw_text, chunk_size, chunk_overlap)

    if not parts or len(parts) <= 1:
        return None

    return parts


def build_overview_chunk_text(filename: str, ctx_summary: str, rel_summary: str,
                              raw_text: str, overview_content_chars: int) -> str:
    """
    Bygg text för overview-chunk (part_0).

    Innehåller filnamn, AI-genererad summary/relationer, samt de första
    N tecknen av råtext. Gör dokumentet sökbart på hög nivå.
    """
    lines = [f"FILENAME: {filename}"]
    if ctx_summary:
        lines.append(f"SUMMARY: {ctx_summary}")
    if rel_summary:
        lines.append(f"RELATIONS: {rel_summary}")

    content_preview = raw_text[:overview_content_chars].strip() if raw_text else ""
    if content_preview:
        lines.append(f"\nCONTENT:\n{content_preview}")

    return '\n'.join(lines)
