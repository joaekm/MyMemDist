"""
Rich Transcriber - 8-stegs pipeline för ljudtranskribering.

Pipeline:
1. Metadata från fil (ffprobe)
2. Transkribering (chunked, parallell)
3. Kalendermatch (±window_minutes)
4. Berikningsfrågor (LLM)
5. Kontext-berikning (Graf + Vektor)
6. Speaker-mapping (LLM, per chunk, parallellt)
7. Delstrukturering (LLM)
8. Transkriptionsfil (ren markdown till Assets/Transcripts/)

Output: Ren markdown utan YAML frontmatter till Assets/Transcripts/.
Ingestion Engine hämtar filen och hanterar Lake/Graf/Vektor.

Ersätter tidigare 2-pass transcriber. Löser trunkeringsproblem genom chunking.
"""

import os
import sys

# Tysta HuggingFace tokenizer fork-varning (måste sättas innan import)
os.environ["TOKENIZERS_PARALLELISM"] = "false"

import time
import yaml
import json
import logging
import re
import tempfile
import threading
import zoneinfo
import concurrent.futures
from datetime import datetime
from pathlib import Path
from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any

# Lägg till projektroten i sys.path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

try:
    from google.genai import types
except ImportError:
    print("[CRITICAL] Saknar bibliotek 'google-genai'.")
    exit(1)

from services.utils.audio_service import (
    get_audio_info, calculate_chunks, extract_audio_segment,
    detect_silence_regions, seconds_to_timestamp,
    AudioInfo, ChunkInfo
)
from services.utils.date_service import get_timestamp
from services.utils.llm_service import LLMService, AdaptiveThrottler
from services.utils.providers import GeminiProvider
from services.utils.graph_service import GraphService
from services.utils.vector_service import vector_scope
from services.utils.metadata_service import generate_semantic_metadata, get_owner_name
from services.utils.terminal_status import status as terminal_status, service_status


# =============================================================================
# CONFIG
# =============================================================================

from services.utils.config_loader import get_config, get_prompts, get_expanded_paths

_raw_config = get_config()
CONFIG = dict(_raw_config)
CONFIG['paths'] = get_expanded_paths()
PROMPTS = get_prompts()

# Expandera sökvägar
for k, v in CONFIG.get('paths', {}).items():
    CONFIG['paths'][k] = os.path.expanduser(v)

# Tidszon
TZ_NAME = CONFIG.get('system', {}).get('timezone', 'UTC')
try:
    SYSTEM_TZ = zoneinfo.ZoneInfo(TZ_NAME)
except Exception as e:
    print(f"[CRITICAL] HARDFAIL: Ogiltig timezone '{TZ_NAME}': {e}")
    exit(1)

# Sökvägar
RECORDINGS_FOLDER = CONFIG['paths']['asset_recordings']
TRANSCRIPTS_FOLDER = CONFIG['paths']['asset_transcripts']
LAKE_FOLDER = CONFIG['paths']['lake_store']
FAILED_FOLDER = CONFIG['paths']['asset_failed']
GRAPH_PATH = CONFIG['paths']['graph_db']
LOG_FILE = CONFIG.get('logging', {}).get('system_log', '~/MyMemory/Logs/system.log')
LOG_FILE = os.path.expanduser(LOG_FILE)

# Skapa mappar
os.makedirs(RECORDINGS_FOLDER, exist_ok=True)
os.makedirs(TRANSCRIPTS_FOLDER, exist_ok=True)
os.makedirs(FAILED_FOLDER, exist_ok=True)

# Transcriber-specifika config
TRANSCRIBER_CONFIG = CONFIG.get('transcriber', {})
CHUNK_SIZE_BYTES = TRANSCRIBER_CONFIG.get('chunk_size_bytes', 10000000)  # 10MB default
CHUNK_OVERLAP_SECONDS = TRANSCRIBER_CONFIG.get('chunk_overlap_seconds', 15)
SILENCE_THRESHOLD_DB = TRANSCRIBER_CONFIG.get('silence_threshold_db', -35)
SILENCE_MIN_DURATION = TRANSCRIBER_CONFIG.get('silence_min_duration', 0.3)
NORMALIZE_AUDIO = TRANSCRIBER_CONFIG.get('normalize_audio', True)
CALENDAR_WINDOW_MINUTES = TRANSCRIBER_CONFIG.get('calendar_window_minutes', 30)
PART_PREVIEW_LIMIT = TRANSCRIBER_CONFIG.get('part_preview_limit', 500)
MAX_FILE_WORKERS = TRANSCRIBER_CONFIG.get('max_file_workers', 2)
MAX_CHUNK_WORKERS = TRANSCRIBER_CONFIG.get('max_chunk_workers', 10)

# Generella config
AI_CONFIG = CONFIG.get('ai_engine', {})
MAX_WORKERS = AI_CONFIG.get('max_workers', 5)
MAX_RETRIES = AI_CONFIG.get('max_retries', 3)
RETRY_DELAY = AI_CONFIG.get('retry_delay_seconds', 2)
GEMINI_POLL_INTERVAL = AI_CONFIG.get('gemini_poll_interval_seconds', 1)

SEARCH_CONFIG = CONFIG.get('search', {})
MAX_ENTITY_QUERIES = SEARCH_CONFIG.get('max_entity_queries', 5)
MAX_SEMANTIC_QUERIES = SEARCH_CONFIG.get('max_semantic_queries', 3)
RESULTS_PER_QUERY = SEARCH_CONFIG.get('results_per_query', 2)

PROCESSING_CONFIG = CONFIG.get('processing', {})
LLM_CONTEXT_LIMIT = PROCESSING_CONFIG.get('llm_context_limit', 5000)
ENRICHMENT_TEXT_LIMIT = PROCESSING_CONFIG.get('enrichment_text_limit', 2000)
MAX_KEYWORDS = PROCESSING_CONFIG.get('max_keywords', 10)

# Media-extensions
MEDIA_EXTENSIONS = CONFIG.get('processing', {}).get('audio_extensions', [])

# Access level
ACCESS_LEVEL = CONFIG.get('security', {}).get('default_access_level', 5)

# Logging
log_dir = os.path.dirname(LOG_FILE)
os.makedirs(log_dir, exist_ok=True)

root_logger = logging.getLogger()
root_logger.setLevel(logging.INFO)
for handler in root_logger.handlers[:]:
    root_logger.removeHandler(handler)
file_handler = logging.FileHandler(LOG_FILE)
file_handler.setFormatter(logging.Formatter('%(asctime)s - TRANS - %(levelname)s - %(message)s'))
root_logger.addHandler(file_handler)

for _name in ['httpx', 'httpcore', 'google', 'google_genai', 'anyio', 'watchdog']:
    logging.getLogger(_name).setLevel(logging.WARNING)

LOGGER = logging.getLogger('MyMem_Transcriber')

# Process state
UUID_PATTERN = re.compile(r'_([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})$')
PROCESSED_FILES = set()
PROCESS_LOCK = threading.Lock()

# Separata executors för filer och chunks (OBJEKT-85)
FILE_EXECUTOR = concurrent.futures.ThreadPoolExecutor(max_workers=MAX_FILE_WORKERS)
CHUNK_EXECUTOR = concurrent.futures.ThreadPoolExecutor(max_workers=MAX_CHUNK_WORKERS)

# Gemini Throttler för transkribering (steg 2)
GEMINI_THROTTLE_CONFIG = CONFIG.get('ai_engine', {}).get('gemini', {}).get('throttling', {})
GEMINI_THROTTLER = AdaptiveThrottler(
    initial_rps=GEMINI_THROTTLE_CONFIG.get('initial_rps', 10.0),
    max_rps=GEMINI_THROTTLE_CONFIG.get('max_rps', 30.0),
    min_rps=GEMINI_THROTTLE_CONFIG.get('min_rps', 0.5),
    increase_factor=GEMINI_THROTTLE_CONFIG.get('increase_factor', 1.2),
    decrease_factor=GEMINI_THROTTLE_CONFIG.get('decrease_factor', 0.5),
    stabilize_after=GEMINI_THROTTLE_CONFIG.get('stabilize_after', 5)
)

# Progress tracking för stora köer
_progress = {"total": 0, "completed": 0, "failed": 0, "current_file": ""}

# LLM Service singleton
_llm_service = None

# Gemini Provider singleton (för transkribering)
_gemini_provider = None


def _get_llm_service() -> LLMService:
    global _llm_service
    if _llm_service is None:
        _llm_service = LLMService()
    return _llm_service


def _get_gemini_client():
    """Hämta Gemini SDK-klient för transkribering."""
    global _gemini_provider
    if _gemini_provider is None:
        config = get_config()
        api_key = config.get('ai_engine', {}).get('gemini', {}).get('api_key')
        if not api_key:
            raise RuntimeError("HARDFAIL: Gemini API-nyckel saknas i config")
        _gemini_provider = GeminiProvider(api_key=api_key)
    return _gemini_provider.client


# =============================================================================
# DATACLASSES
# =============================================================================

@dataclass
class AudioMetadata:
    """Steg 1: Metadata från fil."""
    filepath: str
    filename: str
    unit_id: str
    recording_datetime: datetime
    duration_seconds: float
    bytes_per_second: float
    total_bytes: int


@dataclass
class TranscriptChunk:
    """En transkriberad chunk."""
    index: int
    text: str
    start_seconds: float
    end_seconds: float


@dataclass
class CalendarMatch:
    """Steg 3: Kalendermatch från Lake."""
    title: str
    event_datetime: datetime
    participants: List[str]
    source_file: str = ""


@dataclass
class EnrichmentQueries:
    """Steg 4: Sökfrågor genererade från transkribering."""
    person_queries: List[str] = field(default_factory=list)
    org_queries: List[str] = field(default_factory=list)
    project_queries: List[str] = field(default_factory=list)
    semantic_queries: List[str] = field(default_factory=list)


@dataclass
class ContextPackage:
    """Steg 5: Destillerad kontext."""
    when: str
    who: List[str]
    affiliations: List[str]
    topic: str
    context: str
    relations_summary: str = ""


@dataclass
class TranscriptPart:
    """En del av transkriberingen (Steg 7)."""
    id: int
    title: str
    ingress: str
    timestamp_start: str
    timestamp_end: str
    speakers: List[str]
    topics: List[str]
    content: str = ""


# =============================================================================
# HELPERS
# =============================================================================

def _kort(filnamn: str, max_len: int = 25) -> str:
    """Korta ner filnamn för loggning."""
    if len(filnamn) <= max_len:
        return filnamn
    return "..." + filnamn[-(max_len-3):]


def _parse_json_response(text: str) -> dict | list | None:
    """Parsa JSON från LLM-svar, hanterar markdown code blocks."""
    text = text.strip()
    if text.startswith("```"):
        lines = text.split("\n")
        lines = [l for l in lines if not l.strip().startswith("```")]
        text = "\n".join(lines)
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return None


def _get_prompt(section: str, key: str) -> str:
    """Hämta prompt från config."""
    return PROMPTS.get(section, {}).get(key, '')


def _deduplicate_overlap(
    chunks: List[TranscriptChunk],
    chunk_infos: List[ChunkInfo]
) -> List[TranscriptChunk]:
    """
    Ta bort duplicerad text från overlap-zoner.

    Deterministisk strategi (ingen LLM): för varje chunk med overlap,
    beräkna hur stor andel av chunken som är overlap (tid-proportionellt)
    och ta bort motsvarande andel ord från chunk-starten.

    Args:
        chunks: Transkriberade chunks med text
        chunk_infos: ChunkInfo med overlap-metadata

    Returns:
        Chunks med trimmad text (inga dubbletter)
    """
    if len(chunks) != len(chunk_infos):
        LOGGER.warning(f"Overlap-dedup: chunks ({len(chunks)}) != chunk_infos ({len(chunk_infos)}), hoppar över")
        return chunks

    result = []
    for chunk, info in zip(chunks, chunk_infos):
        if info.overlap_seconds <= 0 or chunk.index == 0:
            result.append(chunk)
            continue

        # Beräkna overlap-andel av total chunk-tid
        chunk_duration = info.end_time_seconds - info.start_time_seconds
        if chunk_duration <= 0:
            result.append(chunk)
            continue

        overlap_ratio = info.overlap_seconds / chunk_duration

        # Ta bort motsvarande andel ord från starten
        words = chunk.text.split()
        words_to_remove = int(len(words) * overlap_ratio)

        if words_to_remove > 0 and words_to_remove < len(words):
            trimmed_text = ' '.join(words[words_to_remove:])
            result.append(TranscriptChunk(
                index=chunk.index,
                text=trimmed_text,
                start_seconds=chunk.start_seconds,
                end_seconds=chunk.end_seconds
            ))
            LOGGER.debug(
                f"Overlap-dedup chunk {chunk.index}: tog bort {words_to_remove}/{len(words)} ord "
                f"(overlap={info.overlap_seconds:.1f}s, ratio={overlap_ratio:.2f})"
            )
        else:
            result.append(chunk)

    return result


# =============================================================================
# STEG 1: METADATA
# =============================================================================

def step1_extract_metadata(audio_path: str) -> AudioMetadata:
    """Extrahera metadata från ljudfil."""
    filename = os.path.basename(audio_path)
    base_name = os.path.splitext(filename)[0]

    # Extrahera UUID
    match = UUID_PATTERN.search(base_name)
    if not match:
        raise ValueError(f"HARDFAIL: Fil saknar UUID: {filename}")
    unit_id = match.group(1)

    # Extrahera datum via date_service (har fallback till mtime)
    recording_dt = get_timestamp(audio_path)

    # Gör datetime timezone-aware
    if recording_dt.tzinfo is None:
        recording_dt = recording_dt.replace(tzinfo=SYSTEM_TZ)

    # Hämta audio-info via ffprobe
    audio_info = get_audio_info(audio_path)

    return AudioMetadata(
        filepath=audio_path,
        filename=filename,
        unit_id=unit_id,
        recording_datetime=recording_dt,
        duration_seconds=audio_info.duration_seconds,
        bytes_per_second=audio_info.bytes_per_second,
        total_bytes=audio_info.total_bytes
    )


# =============================================================================
# STEG 2: TRANSKRIBERING
# =============================================================================

def step2_transcribe(audio_metadata: AudioMetadata) -> List[TranscriptChunk]:
    """Transkribera ljudfil i chunks."""
    gemini_client = _get_gemini_client()
    kort_namn = _kort(audio_metadata.filename)

    prompt = _get_prompt('transcriber', 'pass1_raw')
    if not prompt:
        raise ValueError("HARDFAIL: 'pass1_raw' prompt saknas")

    config = get_config()
    model = config.get('ai_engine', {}).get('models', {}).get('model_transcribe')
    if not model:
        raise ValueError("HARDFAIL: 'model_transcribe' saknas i config")

    # Beräkna chunks - behåll källfilens format
    source_ext = os.path.splitext(audio_metadata.filepath)[1].lstrip('.').lower() or "m4a"
    audio_info = AudioInfo(
        filepath=audio_metadata.filepath,
        duration_seconds=audio_metadata.duration_seconds,
        total_bytes=audio_metadata.total_bytes,
        bytes_per_second=audio_metadata.bytes_per_second,
        format=source_ext
    )

    # Silence detection för intelligent chunking
    silence_regions = detect_silence_regions(
        audio_metadata.filepath,
        threshold_db=SILENCE_THRESHOLD_DB,
        min_duration=SILENCE_MIN_DURATION
    )
    LOGGER.info(f"{kort_namn} → {len(silence_regions)} tysta regioner detekterade")

    chunk_infos = calculate_chunks(
        audio_info,
        chunk_size_bytes=CHUNK_SIZE_BYTES,
        overlap_seconds=CHUNK_OVERLAP_SECONDS,
        silence_regions=silence_regions
    )

    LOGGER.info(f"{kort_namn} → Delas upp i {len(chunk_infos)} chunks (overlap={CHUNK_OVERLAP_SECONDS}s)")

    with tempfile.TemporaryDirectory() as temp_dir:
        # Fas 1: Extrahera chunks med ffmpeg
        chunk_paths = []
        for chunk_info in chunk_infos:
            chunk_filename = f"chunk_{chunk_info.chunk_index}.{source_ext}"
            chunk_path = os.path.join(temp_dir, chunk_filename)

            extract_audio_segment(
                input_path=audio_metadata.filepath,
                output_path=chunk_path,
                start_seconds=chunk_info.start_time_seconds,
                end_seconds=chunk_info.end_time_seconds,
                normalize=NORMALIZE_AUDIO
            )
            chunk_paths.append((chunk_info, chunk_path))

        LOGGER.debug(f"{kort_namn} → Extraherade {len(chunk_paths)} segment")

        # Fas 2: Ladda upp alla filer till Gemini parallellt (med throttling)
        def upload_chunk(chunk_data):
            chunk_info, chunk_path = chunk_data
            chunk_filename = f"chunk_{chunk_info.chunk_index}.{source_ext}"

            # Throttle upload-anrop
            GEMINI_THROTTLER.wait()

            try:
                upload_file = gemini_client.files.upload(
                    file=chunk_path,
                    config={"display_name": chunk_filename}
                )
                while upload_file.state.name == "PROCESSING":
                    time.sleep(GEMINI_POLL_INTERVAL)
                    upload_file = gemini_client.files.get(name=upload_file.name)
                if upload_file.state.name != "ACTIVE":
                    GEMINI_THROTTLER.report_error()
                    raise RuntimeError(f"HARDFAIL: Fil ej aktiv: {upload_file.state.name}")

                GEMINI_THROTTLER.report_success()
                return (chunk_info, upload_file)

            except Exception as e:
                if "rate" in str(e).lower() or "429" in str(e) or "quota" in str(e).lower():
                    GEMINI_THROTTLER.report_rate_limit()
                else:
                    GEMINI_THROTTLER.report_error()
                raise

        # Använd CHUNK_EXECUTOR för parallell upload
        futures = [CHUNK_EXECUTOR.submit(upload_chunk, cp) for cp in chunk_paths]
        uploaded_files = [f.result() for f in futures]

        LOGGER.debug(f"{kort_namn} → Alla filer uppladdade")

        # Fas 3: Transkribera alla chunks parallellt (med throttling)
        def transcribe_chunk(upload_data):
            chunk_info, upload_file = upload_data

            for attempt in range(MAX_RETRIES):
                # Throttle transkriberings-anrop
                GEMINI_THROTTLER.wait()

                try:
                    response = gemini_client.models.generate_content(
                        model=model,
                        contents=[types.Content(role="user", parts=[
                            types.Part.from_uri(file_uri=upload_file.uri, mime_type=upload_file.mime_type),
                            types.Part.from_text(text=prompt)
                        ])],
                        config=types.GenerateContentConfig(response_mime_type="text/plain")
                    )

                    if response.text is not None:
                        GEMINI_THROTTLER.report_success()
                        return (chunk_info, response.text, upload_file)

                    GEMINI_THROTTLER.report_error()
                    LOGGER.warning(f"Chunk {chunk_info.chunk_index}: response.text=None, retry {attempt + 1}/{MAX_RETRIES}")

                except (ConnectionError, TimeoutError) as e:
                    GEMINI_THROTTLER.report_error()
                    LOGGER.warning(f"Chunk {chunk_info.chunk_index}: Network error {e}, retry {attempt + 1}/{MAX_RETRIES}")
                    time.sleep(RETRY_DELAY * (attempt + 1))
                    continue

                time.sleep(RETRY_DELAY * (attempt + 1))

            raise RuntimeError(f"HARDFAIL: Chunk {chunk_info.chunk_index} returnerade None efter {MAX_RETRIES} försök")

        # Använd CHUNK_EXECUTOR för parallell transkribering
        futures = [CHUNK_EXECUTOR.submit(transcribe_chunk, uf) for uf in uploaded_files]
        results = [f.result() for f in futures]

        # Fas 4: Bygg TranscriptChunks och rensa upp
        transcript_chunks = []
        for chunk_info, chunk_text, upload_file in results:
            transcript_chunks.append(TranscriptChunk(
                index=chunk_info.chunk_index,
                text=chunk_text,
                start_seconds=chunk_info.start_time_seconds,
                end_seconds=chunk_info.end_time_seconds
            ))
            # Rensa upp Gemini-fil (best effort, cleanup failure är OK)
            try:
                gemini_client.files.delete(name=upload_file.name)
            except Exception:  # noqa: FALLBACK_DOCUMENTED - cleanup failure accepteras
                pass

        transcript_chunks.sort(key=lambda c: c.index)

    # Deduplicera overlap-text
    total_before = sum(len(c.text) for c in transcript_chunks)
    transcript_chunks = _deduplicate_overlap(transcript_chunks, chunk_infos)
    total_after = sum(len(c.text) for c in transcript_chunks)

    LOGGER.info(
        f"{kort_namn} → Transkribering klar: {len(transcript_chunks)} chunks, "
        f"{total_after} tecken (dedup: {total_before - total_after} tecken borttagna)"
    )

    return transcript_chunks


# =============================================================================
# STEG 3: KALENDERMATCH
# =============================================================================

def step3_find_calendar_match(recording_dt: datetime, recording_duration_minutes: float = 0) -> Optional[CalendarMatch]:
    """Sök kalenderhändelse i Lake.

    Score-algoritm (lägre = bättre):
    - Starttids-diff: minuter från inspelningsstart
    - Duration-diff: skillnad mellan möteslängd och inspelningslängd (max 60p)
    - Block-möten: +50p straff
    - Inga deltagare: +100p straff
    - Accepterade deltagare: -2p per accepterad (bonus)
    """
    lake_path = Path(LAKE_FOLDER)
    date_str = recording_dt.strftime("%Y-%m-%d")

    calendar_files = list(lake_path.glob(f"Calendar_{date_str}_*.md"))

    if not calendar_files:
        LOGGER.debug(f"Ingen kalenderfil för {date_str}")
        return None

    target_minutes = recording_dt.hour * 60 + recording_dt.minute

    for cal_file in calendar_files:
        try:
            content = cal_file.read_text()

            # Sök möten: ## HH:MM-HH:MM: Titel
            pattern = r'## (\d{2}):(\d{2})-(\d{2}):(\d{2}): ([^\n]+)'

            candidates = []
            for match in re.finditer(pattern, content):
                start_hour, start_minute = int(match.group(1)), int(match.group(2))
                end_hour, end_minute = int(match.group(3)), int(match.group(4))
                title = match.group(5).strip()

                meeting_start_minutes = start_hour * 60 + start_minute
                meeting_end_minutes = end_hour * 60 + end_minute
                meeting_duration = meeting_end_minutes - meeting_start_minutes

                start_diff = abs(meeting_start_minutes - target_minutes)

                if start_diff <= CALENDAR_WINDOW_MINUTES:
                    participants, accepted_count = _extract_participants_with_status(content, match.end())

                    is_block = title.lower().startswith("block:")
                    has_participants = len(participants) > 0

                    # Duration-matchning: hur väl matchar möteslängd inspelningslängd?
                    duration_diff = abs(meeting_duration - recording_duration_minutes) if recording_duration_minutes > 0 else 0
                    duration_penalty = min(duration_diff, 60)  # Max 60p straff

                    # Acceptans-bonus: fler som tackat ja = troligare rätt möte
                    acceptance_bonus = -accepted_count * 2

                    score = (
                        start_diff +                              # Tidsdiff
                        duration_penalty +                        # Längd-diff
                        (50 if is_block else 0) +                 # Block-straff
                        (100 if not has_participants else 0) +    # Inga deltagare-straff
                        acceptance_bonus                          # Acceptans-bonus
                    )

                    candidates.append({
                        'title': title,
                        'participants': participants,
                        'accepted_count': accepted_count,
                        'meeting_duration': meeting_duration,
                        'score': score,
                        'source': str(cal_file)
                    })

            if candidates:
                candidates.sort(key=lambda x: x['score'])
                best = candidates[0]

                LOGGER.info(f"Kalendermatch: {best['title']} (score={best['score']}, accepted={best['accepted_count']}, duration={best['meeting_duration']}min)")
                return CalendarMatch(
                    title=best['title'],
                    event_datetime=recording_dt,
                    participants=best['participants'],
                    source_file=best['source']
                )

        except Exception as e:  # noqa: FALLBACK_DOCUMENTED - kalendermatch är optional berikning
            LOGGER.warning(f"Kalenderfel: {e}")
            continue

    return None


def _extract_participants_with_status(content: str, start_pos: int) -> tuple[List[str], int]:
    """Extrahera deltagare från kalenderinnehåll med RSVP-status.

    Returns:
        tuple: (participants list, accepted_count)
    """
    next_meeting = content.find("\n## ", start_pos)
    section = content[start_pos:next_meeting] if next_meeting != -1 else content[start_pos:]

    participants = []
    accepted_count = 0
    pattern = r'\*\*Deltagare:\*\*\s*([^\n]+)'
    match = re.search(pattern, section)

    if match:
        parts = match.group(1).split(',')
        for part in parts:
            part = part.strip()

            # Kolla RSVP-status i parentesen
            is_accepted = '(accepterat)' in part.lower()
            if is_accepted:
                accepted_count += 1

            paren_pos = part.find('(')
            raw_name = part[:paren_pos].strip() if paren_pos > 0 else part.strip()

            if (raw_name and len(raw_name) > 2 and
                not raw_name.startswith("Stockholm") and
                not raw_name.startswith("DOTAB") and
                not any(c.isdigit() for c in raw_name)):

                if '.' in raw_name and '@' not in raw_name:
                    name_parts = raw_name.split('.')
                    formatted = ' '.join(p.capitalize() for p in name_parts)
                    participants.append(formatted)
                else:
                    participants.append(raw_name)

    return list(dict.fromkeys(participants)), accepted_count


# =============================================================================
# STEG 4: BERIKNINGSFRÅGOR
# =============================================================================

def step4_create_enrichment_queries(
    chunks: List[TranscriptChunk],
    calendar_match: Optional[CalendarMatch]
) -> EnrichmentQueries:
    """LLM analyserar transkribering och skapar sökfrågor."""
    llm = _get_llm_service()

    transcript_text = "\n\n".join(c.text for c in chunks)

    calendar_info = ""
    if calendar_match:
        calendar_info = f"""
KALENDERINFO:
- Titel: {calendar_match.title}
- Deltagare: {', '.join(calendar_match.participants)}
"""

    prompt_template = _get_prompt('transcriber', 'enrichment_queries')
    if not prompt_template:
        raise ValueError("HARDFAIL: 'enrichment_queries' prompt saknas i services_prompts.yaml")

    prompt = prompt_template.format(
        calendar_info=calendar_info,
        context_limit=LLM_CONTEXT_LIMIT,
        transcript_text=transcript_text[:LLM_CONTEXT_LIMIT]
    )

    # Anthropic Sonnet för enrichment (OBJEKT-85)
    response = llm.generate(prompt, provider='anthropic', model=llm.models['fast'])

    if not response.success:
        raise RuntimeError(f"HARDFAIL: LLM-anrop misslyckades i steg 4: {response.error}")

    data = _parse_json_response(response.text)
    if not data:
        raise RuntimeError(f"HARDFAIL: JSON-parsning misslyckades i steg 4")

    return EnrichmentQueries(
        person_queries=data.get('person_queries', []),
        org_queries=data.get('org_queries', []),
        project_queries=data.get('project_queries', []),
        semantic_queries=data.get('semantic_queries', [])
    )


# =============================================================================
# STEG 5: KONTEXT-BERIKNING
# =============================================================================

def step5_enrich_context(
    audio_metadata: AudioMetadata,
    chunks: List[TranscriptChunk],
    calendar_match: Optional[CalendarMatch],
    queries: EnrichmentQueries
) -> ContextPackage:
    """Hämta rådata från Graf + Vektor, destillera till ContextPackage."""
    llm = _get_llm_service()

    graph_data = {
        'persons': {},
        'organizations': {},
        'projects': {},
        'shared_events': []
    }

    # Graf-uppslagning
    if os.path.exists(GRAPH_PATH):
        graph = None
        try:
            graph = GraphService(GRAPH_PATH, read_only=True)

            # Personer
            person_node_ids = {}
            for name in queries.person_queries[:MAX_ENTITY_QUERIES]:
                node_id = graph.find_node_by_name("Person", name)
                if node_id:
                    node = graph.get_node(node_id)
                    if node:
                        props = node.get('properties', {})
                        person_node_ids[name] = node_id

                        person_data = {
                            'node_id': node_id,
                            'person_type': props.get('person_type', ''),
                            'node_context': props.get('node_context', ''),
                            'organizations': [],
                            'roles': [],
                            'events_attended': []
                        }

                        edges = graph.get_edges_from(node_id)
                        for edge in edges:
                            edge_type = edge.get('type')
                            target_id = edge.get('target')
                            target = graph.get_node(target_id)

                            if not target:
                                continue

                            target_props = target.get('properties', {})
                            target_name = target_props.get('name', '')

                            if edge_type == 'BELONGS_TO':
                                org_info = {
                                    'name': target_name,
                                    'job_title': edge.get('properties', {}).get('job_title', ''),
                                    'job_function': edge.get('properties', {}).get('job_function', '')
                                }
                                person_data['organizations'].append(org_info)
                            elif edge_type == 'HAS_ROLE':
                                person_data['roles'].append(target_name)
                            elif edge_type == 'ATTENDED':
                                event_info = {
                                    'event_id': target_id,
                                    'name': target_name,
                                    'event_type': target_props.get('event_type', ''),
                                    'node_context': target_props.get('node_context', '')
                                }
                                person_data['events_attended'].append(event_info)

                        graph_data['persons'][name] = person_data

            # Organisationer
            for org_name in queries.org_queries[:MAX_ENTITY_QUERIES]:
                node_id = graph.find_node_by_name("Organization", org_name)
                if node_id:
                    node = graph.get_node(node_id)
                    if node:
                        props = node.get('properties', {})
                        org_data = {
                            'node_id': node_id,
                            'org_type': props.get('org_type', ''),
                            'node_context': props.get('node_context', ''),
                            'business_relations': []
                        }

                        edges = graph.get_edges_from(node_id)
                        for edge in edges:
                            if edge.get('type') == 'HAS_BUSINESS_RELATION':
                                target = graph.get_node(edge.get('target'))
                                if target:
                                    rel_info = {
                                        'target': target.get('properties', {}).get('name', ''),
                                        'relation_type': edge.get('properties', {}).get('relation_type', ''),
                                        'relation_status': edge.get('properties', {}).get('relation_status', '')
                                    }
                                    org_data['business_relations'].append(rel_info)

                        graph_data['organizations'][org_name] = org_data

            # Projekt
            for proj_name in queries.project_queries[:MAX_ENTITY_QUERIES]:
                node_id = graph.find_node_by_name("Project", proj_name)
                if node_id:
                    node = graph.get_node(node_id)
                    if node:
                        props = node.get('properties', {})
                        graph_data['projects'][proj_name] = {
                            'node_id': node_id,
                            'project_status': props.get('project_status', ''),
                            'project_type': props.get('project_type', ''),
                            'node_context': props.get('node_context', '')
                        }

        except Exception as e:  # noqa: FALLBACK_DOCUMENTED - graf-berikning är optional
            LOGGER.warning(f"Graf-berikning misslyckades: {e}")
        finally:
            if graph is not None:
                try:
                    graph.close()
                except Exception:  # noqa: FALLBACK_DOCUMENTED - close failure accepteras
                    pass

    # Vektor-sökning
    LOGGER.info("Steg 5: Startar vektor-sökning")
    vector_results = []
    try:
        with vector_scope(exclusive=False, timeout=10.0) as vector:
            for query in queries.semantic_queries[:MAX_SEMANTIC_QUERIES]:
                results = vector.search(query, limit=RESULTS_PER_QUERY)
                for doc in results:
                    vector_results.append({
                        'query': query,
                        'distance': doc.get('distance'),
                        'metadata': doc.get('metadata') or {}
                    })
        LOGGER.info(f"Steg 5: Vektor-sökning klar, {len(vector_results)} träffar")
    except TimeoutError:
        LOGGER.warning("Steg 5: Vektor-sökning skipped (lock timeout 10s)")
    except (OSError, RuntimeError, ValueError) as e:  # noqa: FALLBACK_DOCUMENTED - vektor-sökning är optional
        LOGGER.warning(f"Vektor-sökning misslyckades: {e}")

    # Bygg rik rådata
    persons_summary = []
    for name, data in graph_data['persons'].items():
        orgs = [f"{o['name']} ({o['job_title']})" if o['job_title'] else o['name']
                for o in data.get('organizations', [])]
        persons_summary.append({
            'name': name,
            'organizations': orgs,
            'roles': data.get('roles', []),
            'person_type': data.get('person_type', '')
        })

    orgs_summary = []
    for name, data in graph_data['organizations'].items():
        rels = [f"{r['relation_type']} till {r['target']}" for r in data.get('business_relations', [])]
        orgs_summary.append({
            'name': name,
            'org_type': data.get('org_type', ''),
            'business_relations': rels
        })

    raw_data = f"""
FILMETADATA:
- Datum/tid: {audio_metadata.recording_datetime}
- Duration: {int(audio_metadata.duration_seconds // 60)} minuter

KALENDER:
- Titel: {calendar_match.title if calendar_match else 'Ingen kalendermatch'}
- Deltagare: {', '.join(calendar_match.participants) if calendar_match else 'Okända'}

PERSONER (från graf):
{json.dumps(persons_summary, indent=2, ensure_ascii=False) if persons_summary else 'Inga'}

ORGANISATIONER (från graf):
{json.dumps(orgs_summary, indent=2, ensure_ascii=False) if orgs_summary else 'Inga'}

PROJEKT (från graf):
{json.dumps(list(graph_data['projects'].keys()), ensure_ascii=False) if graph_data['projects'] else 'Inga'}

VEKTOR-TRÄFFAR:
{json.dumps([{'query': v['query'], 'source': v['metadata'].get('source_type', '')} for v in vector_results[:5]], indent=2, ensure_ascii=False) if vector_results else 'Inga träffar'}

TRANSKRIBERING (utdrag):
{chunks[0].text[:ENRICHMENT_TEXT_LIMIT] if chunks else ''}
"""

    prompt_template = _get_prompt('transcriber', 'context_distill')
    if not prompt_template:
        raise ValueError("HARDFAIL: 'context_distill' prompt saknas i services_prompts.yaml")

    distill_prompt = prompt_template.format(raw_data=raw_data)

    # Anthropic Sonnet för enrichment (OBJEKT-85)
    LOGGER.info("Steg 5: Startar LLM context distill")
    response = llm.generate(distill_prompt, provider='anthropic', model=llm.models['fast'])
    LOGGER.info("Steg 5: LLM context distill klar")

    if not response.success:
        raise RuntimeError(f"HARDFAIL: LLM-anrop misslyckades i steg 5: {response.error}")

    data = _parse_json_response(response.text)
    if not data:
        raise RuntimeError(f"HARDFAIL: JSON-parsning misslyckades i steg 5")

    return ContextPackage(
        when=data.get('when', ''),
        who=data.get('who', []),
        affiliations=data.get('affiliations', []),
        topic=data.get('topic', ''),
        context=data.get('context', ''),
        relations_summary=data.get('relations_summary', '')
    )


# =============================================================================
# STEG 6: SPEAKER-MAPPING
# =============================================================================

def step6_map_speakers(
    chunks: List[TranscriptChunk],
    context: ContextPackage
) -> List[Dict[str, str]]:
    """Mappa [Talare X] till namn PER CHUNK (parallellt)."""
    if not context.who:
        LOGGER.debug("Inga kända deltagare → behåller [Talare X]")
        return [{} for _ in chunks]

    llm = _get_llm_service()

    prompt_template = _get_prompt('transcriber', 'speaker_mapping')
    if not prompt_template:
        raise ValueError("HARDFAIL: 'speaker_mapping' prompt saknas i services_prompts.yaml")

    def map_single_chunk(chunk_data: tuple) -> tuple:
        i, chunk = chunk_data

        prompt = prompt_template.format(
            participants=', '.join(context.who),
            affiliations=', '.join(context.affiliations),
            transcript_text=chunk.text
        )

        # Anthropic Sonnet för enrichment (OBJEKT-85)
        response = llm.generate(prompt, provider='anthropic', model=llm.models['fast'])

        if not response.success:
            LOGGER.warning(f"Speaker-mapping misslyckades för chunk {i}")
            return (i, {})

        mapping = _parse_json_response(response.text)
        if not mapping or not isinstance(mapping, dict):
            return (i, {})

        return (i, mapping)

    chunk_data = [(i, chunk) for i, chunk in enumerate(chunks)]

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        results = list(executor.map(map_single_chunk, chunk_data))

    results.sort(key=lambda x: x[0])
    mappings = [mapping for _, mapping in results]

    return mappings


def apply_speaker_mapping(chunks: List[TranscriptChunk], mappings: List[Dict[str, str]]) -> List[str]:
    """Applicera speaker-mapping PER CHUNK."""
    mapped_chunks = []

    for chunk, mapping in zip(chunks, mappings):
        text = chunk.text

        for speaker_label, name in mapping.items():
            if name and name != "Okänd":
                text = text.replace(f"[{speaker_label}]:", f"**{name}:**")
                text = text.replace(f"{speaker_label}:", f"**{name}:**")
                text = text.replace(f"[{speaker_label}]", f"**{name}**")

        # Normalisera radbrytningar före talarmarkering
        text = re.sub(r'\n*(\*\*[^*]+:\*\*)', r'\n\n\1', text)
        text = text.lstrip('\n')

        mapped_chunks.append(text)

    return mapped_chunks


# =============================================================================
# STEG 7: DELSTRUKTURERING
# =============================================================================

def step7_structure_parts(
    chunks: List[TranscriptChunk],
    mapped_chunks: List[str],
    audio_metadata: AudioMetadata,
    context: ContextPackage
) -> List[TranscriptPart]:
    """Generera metadata (titel, ingress, topics) för varje chunk."""
    llm = _get_llm_service()

    chunks_for_prompt = []
    for i, (chunk, mapped_text) in enumerate(zip(chunks, mapped_chunks)):
        chunks_for_prompt.append({
            "id": i + 1,
            "start": seconds_to_timestamp(chunk.start_seconds),
            "end": seconds_to_timestamp(chunk.end_seconds),
            "text_preview": mapped_text[:PART_PREVIEW_LIMIT]
        })

    prompt_template = _get_prompt('transcriber', 'part_structuring')
    if not prompt_template:
        raise ValueError("HARDFAIL: 'part_structuring' prompt saknas i services_prompts.yaml")

    prompt = prompt_template.format(
        num_parts=len(chunks),
        participants=', '.join(context.who) if context.who else 'Okända',
        topic=context.topic,
        parts_json=json.dumps(chunks_for_prompt, indent=2, ensure_ascii=False)
    )

    # Anthropic Sonnet för enrichment (OBJEKT-85)
    response = llm.generate(prompt, provider='anthropic', model=llm.models['fast'])

    if not response.success:
        raise RuntimeError(f"HARDFAIL: LLM-anrop misslyckades i steg 7: {response.error}")

    raw_parts = _parse_json_response(response.text)
    if not raw_parts or not isinstance(raw_parts, list):
        raise RuntimeError(f"HARDFAIL: JSON-parsning misslyckades i steg 7")

    parts = []
    for i, chunk in enumerate(chunks):
        raw = raw_parts[i] if i < len(raw_parts) else {}

        part = TranscriptPart(
            id=i + 1,
            title=raw.get('title', f'Del {i+1}'),
            ingress=raw.get('ingress', ''),
            timestamp_start=seconds_to_timestamp(chunk.start_seconds),
            timestamp_end=seconds_to_timestamp(chunk.end_seconds),
            speakers=raw.get('speakers', []),
            topics=raw.get('topics', []),
            content=mapped_chunks[i]
        )
        parts.append(part)

    return parts


# =============================================================================
# STEG 8: TRANSKRIPTIONSFIL
# =============================================================================

def step8_write_transcript(
    audio_metadata: AudioMetadata,
    context: ContextPackage,
    parts: List[TranscriptPart],
    calendar_match: Optional[CalendarMatch]
) -> str:
    """
    Generera ren markdown-transkription till Assets/Transcripts/.

    Ingen YAML frontmatter - Ingestion Engine hanterar Lake/Graf/Vektor.
    Transcriber producerar endast högkvalitativ text.
    """
    # Titel
    meeting_title = calendar_match.title if calendar_match else context.topic

    # Duration formaterad
    dur_min = int(audio_metadata.duration_seconds // 60)

    # Bygg ren markdown
    md_parts = []

    # Header
    md_parts.append(f"# {meeting_title or audio_metadata.filename}\n\n")
    md_parts.append(f"**Tid:** {audio_metadata.recording_datetime.strftime('%Y-%m-%d %H:%M:%S')}, duration {dur_min} min\n")
    if context.who:
        md_parts.append(f"**Deltagare:** {', '.join(context.who)}\n")
    if context.context:
        md_parts.append(f"\n*{context.context}*\n")
    md_parts.append("\n---\n\n")

    # Delar
    for part in parts:
        md_parts.append(f"## Del {part.id}: {part.title}\n")
        md_parts.append(f"**Tid:** {part.timestamp_start} - {part.timestamp_end}\n\n")
        if part.ingress:
            md_parts.append(f"*{part.ingress}*\n\n")
        md_parts.append(f"{part.content}\n\n")

    # Skriv ENDAST till Transcripts-mappen
    base_name = os.path.splitext(audio_metadata.filename)[0]
    txt_file = os.path.join(TRANSCRIPTS_FOLDER, f"{base_name}.txt")

    with open(txt_file, 'w', encoding='utf-8') as f:
        f.write("".join(md_parts))

    LOGGER.info(f"Transkription skapad: {txt_file}")

    return txt_file


# =============================================================================
# HUVUDPROCESS
# =============================================================================

def process_audio(filväg: str, filnamn: str):
    """Kör hela 8-stegspipelinen för en ljudfil."""
    kort_namn = _kort(filnamn)

    with PROCESS_LOCK:
        if filnamn in PROCESSED_FILES or filnamn.startswith("temp_"):
            return
        PROCESSED_FILES.add(filnamn)

    # Kolla om redan transkriberad (transkriptionsfil i Transcripts)
    base_name = os.path.splitext(filnamn)[0]
    transcript_file = os.path.join(TRANSCRIPTS_FOLDER, f"{base_name}.txt")
    if os.path.exists(transcript_file):
        LOGGER.debug(f"Redan transkriberad: {kort_namn}")
        with PROCESS_LOCK:
            PROCESSED_FILES.discard(filnamn)
        return

    LOGGER.info(f"Starting: {filnamn}")
    terminal_status("transcriber", filnamn, "processing")
    start_time = time.time()

    try:
        # STEG 1: Metadata
        audio_metadata = step1_extract_metadata(filväg)

        # STEG 2: Transkribering
        chunks = step2_transcribe(audio_metadata)

        # STEG 3: Kalendermatch
        recording_duration_minutes = audio_metadata.duration_seconds / 60
        calendar_match = step3_find_calendar_match(audio_metadata.recording_datetime, recording_duration_minutes)

        # STEG 4: Berikningsfrågor
        queries = step4_create_enrichment_queries(chunks, calendar_match)

        # STEG 5: Kontext-berikning
        context = step5_enrich_context(audio_metadata, chunks, calendar_match, queries)

        # STEG 6: Speaker-mapping
        mappings = step6_map_speakers(chunks, context)
        mapped_chunks = apply_speaker_mapping(chunks, mappings)

        # STEG 7: Delstrukturering
        parts = step7_structure_parts(chunks, mapped_chunks, audio_metadata, context)

        # STEG 8: Transkriptionsfil (till Assets/Transcripts/)
        transcript_file = step8_write_transcript(audio_metadata, context, parts, calendar_match)

        total_time = int(time.time() - start_time)
        dur_min = int(audio_metadata.duration_seconds // 60)
        dur_sec = int(audio_metadata.duration_seconds % 60)
        detail = f"{dur_min}:{dur_sec:02d}"

        LOGGER.info(f"Completed: {kort_namn} ({total_time}s)")
        terminal_status("transcriber", filnamn, "done", detail=detail)

    except Exception as e:  # noqa: FALLBACK_DOCUMENTED - watchdog ska fortsätta köra vid filfel
        LOGGER.error(f"HARDFAIL {filnamn}: {e}")
        terminal_status("transcriber", filnamn, "failed", str(e)[:50])

        # Flytta till failed (best effort)
        try:
            dest = os.path.join(FAILED_FOLDER, filnamn)
            if not os.path.exists(dest):
                import shutil
                shutil.move(filväg, dest)
        except Exception:  # noqa: FALLBACK_DOCUMENTED - move failure accepteras
            pass

    finally:
        with PROCESS_LOCK:
            PROCESSED_FILES.discard(filnamn)


# =============================================================================
# WATCHDOG
# =============================================================================

class AudioHandler(FileSystemEventHandler):
    def _handle_new_file(self, filepath):
        """Hantera ny ljudfil (från on_created eller on_moved)."""
        fname = os.path.basename(filepath)
        ext = os.path.splitext(fname)[1].lower()
        if ext in MEDIA_EXTENSIONS:
            base = os.path.splitext(fname)[0]
            if UUID_PATTERN.search(base):
                FILE_EXECUTOR.submit(process_audio_with_progress, filepath, fname)

    def on_created(self, event):
        if event.is_directory:
            return
        self._handle_new_file(event.src_path)

    def on_moved(self, event):
        """Hanterar atomic rename (t.ex. från rode_collector tmp/ → Recordings/)."""
        if event.is_directory:
            return
        self._handle_new_file(event.dest_path)


def process_audio_with_progress(filväg: str, filnamn: str):
    """Wrapper med progress-tracking."""
    global _progress

    _progress["current_file"] = filnamn

    try:
        process_audio(filväg, filnamn)
        with PROCESS_LOCK:
            _progress["completed"] += 1

    except Exception:
        with PROCESS_LOCK:
            _progress["failed"] += 1
        raise

    finally:
        # Logga progress
        if _progress["total"] > 0:
            pct = int((_progress["completed"] / _progress["total"]) * 100)
            LOGGER.info(f"Progress: {_progress['completed']}/{_progress['total']} ({pct}%) - {_progress['failed']} failed")
            terminal_status("transcriber", f"Progress: {_progress['completed']}/{_progress['total']} ({pct}%)", "info")


if __name__ == "__main__":
    # Räkna och processa befintliga filer
    pending_files = []
    if os.path.exists(RECORDINGS_FOLDER):
        for f in os.listdir(RECORDINGS_FOLDER):
            ext = os.path.splitext(f)[1].lower()
            if ext in MEDIA_EXTENSIONS:
                base = os.path.splitext(f)[0]
                if not f.startswith("temp_") and UUID_PATTERN.search(base):
                    # Kolla transkriptionsfil i Transcripts (inte Lake)
                    transcript_file = os.path.join(TRANSCRIPTS_FOLDER, f"{base}.txt")
                    if not os.path.exists(transcript_file):
                        pending_files.append(f)

    # Sätt total för progress tracking
    _progress["total"] = len(pending_files)
    _progress["completed"] = 0
    _progress["failed"] = 0

    if pending_files:
        LOGGER.info(f"Starting transcription of {len(pending_files)} files")

    # Submit till FILE_EXECUTOR (begränsad parallellism)
    for f in pending_files:
        FILE_EXECUTOR.submit(process_audio_with_progress, os.path.join(RECORDINGS_FOLDER, f), f)

    service_status("Transcriber", "started")

    observer = Observer()
    observer.schedule(AudioHandler(), RECORDINGS_FOLDER, recursive=False)
    observer.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        FILE_EXECUTOR.shutdown(wait=False)
        CHUNK_EXECUTOR.shutdown(wait=False)
        observer.stop()
    observer.join()
