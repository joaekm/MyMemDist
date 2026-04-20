"""
Audio Service - Hjälpfunktioner för ljudfilshantering.

Använder ffprobe/ffmpeg för:
- Hämta metadata (duration, storlek, format)
- Silence detection för intelligent chunking
- Chunking av stora ljudfiler vid tysta partier
- Extrahera segment med valfri normalisering

Alla parametrar läses från config.
"""

import os
import subprocess
import json
import re
import logging
from dataclasses import dataclass, field
from typing import List, Optional
from datetime import datetime

# Logger
LOGGER = logging.getLogger('MyMem_AudioService')

# Sökfönster för silence-baserad chunking (sekunder runt ideal klipppunkt)
_SILENCE_SEARCH_WINDOW = 30.0


def _find_ffmpeg_binary(name: str) -> str:
    """
    Hitta ffmpeg/ffprobe binär.

    Prioriteringsordning:
    1. Runtime/.ffmpeg/ (lokal installation)
    2. PATH (systeminstallation via homebrew etc.)
    """
    # Lokal installation i Runtime
    local_path = os.path.expanduser(
        f"~/Library/Application Support/MyMemory/Runtime/.ffmpeg/{name}"
    )
    if os.path.isfile(local_path) and os.access(local_path, os.X_OK):
        return local_path

    # Fallback till PATH
    return name


def _get_ffprobe() -> str:
    """Returnerar sökväg till ffprobe."""
    return _find_ffmpeg_binary('ffprobe')


def _get_ffmpeg() -> str:
    """Returnerar sökväg till ffmpeg."""
    return _find_ffmpeg_binary('ffmpeg')


@dataclass
class AudioInfo:
    """Metadata om en ljudfil."""
    filepath: str
    duration_seconds: float
    total_bytes: int
    bytes_per_second: float
    format: str
    sample_rate: Optional[int] = None
    channels: Optional[int] = None


@dataclass
class SilenceRegion:
    """En tyst region i en ljudfil."""
    start: float      # Starttid i sekunder
    end: float         # Sluttid i sekunder
    duration: float    # Längd i sekunder


@dataclass
class ChunkInfo:
    """Information om en chunk."""
    chunk_index: int
    start_byte: int
    end_byte: int
    start_time_seconds: float
    end_time_seconds: float
    start_timestamp: str  # "HH:MM:SS"
    end_timestamp: str    # "HH:MM:SS"
    overlap_seconds: float = 0.0  # Hur mycket overlap med föregående chunk


def seconds_to_timestamp(seconds: float) -> str:
    """Konvertera sekunder till HH:MM:SS format."""
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    secs = int(seconds % 60)
    return f"{hours:02d}:{minutes:02d}:{secs:02d}"


def get_audio_info(filepath: str) -> AudioInfo:
    """
    Hämta metadata om ljudfil via ffprobe.

    Args:
        filepath: Sökväg till ljudfilen

    Returns:
        AudioInfo med duration, storlek, etc.

    Raises:
        FileNotFoundError: Om filen inte finns
        RuntimeError: Om ffprobe misslyckas
    """
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"HARDFAIL: Ljudfil finns inte: {filepath}")

    try:
        result = subprocess.run(
            [
                _get_ffprobe(),
                '-v', 'quiet',
                '-print_format', 'json',
                '-show_format',
                '-show_streams',
                filepath
            ],
            capture_output=True,
            text=True,
            timeout=30
        )

        if result.returncode != 0:
            raise RuntimeError(f"HARDFAIL: ffprobe fel: {result.stderr}")

        data = json.loads(result.stdout)

        # Hämta format-info
        format_info = data.get('format', {})
        duration = float(format_info.get('duration', 0))
        format_name = format_info.get('format_name', 'unknown')

        if duration <= 0:
            raise RuntimeError(f"HARDFAIL: Ogiltig duration: {duration}")

        # Hämta stream-info (för sample rate och channels)
        sample_rate = None
        channels = None
        for stream in data.get('streams', []):
            if stream.get('codec_type') == 'audio':
                sample_rate = int(stream.get('sample_rate', 0)) or None
                channels = int(stream.get('channels', 0)) or None
                break

        # Filstorlek
        total_bytes = os.path.getsize(filepath)

        # Bytes per sekund
        bytes_per_second = total_bytes / duration

        LOGGER.debug(f"AudioInfo: {filepath} - {duration:.0f}s, {total_bytes / 1_000_000:.1f}MB")

        return AudioInfo(
            filepath=filepath,
            duration_seconds=duration,
            total_bytes=total_bytes,
            bytes_per_second=bytes_per_second,
            format=format_name,
            sample_rate=sample_rate,
            channels=channels
        )

    except subprocess.TimeoutExpired:
        raise RuntimeError(f"HARDFAIL: ffprobe timeout för {filepath}")
    except json.JSONDecodeError as e:
        raise RuntimeError(f"HARDFAIL: Kunde inte parsa ffprobe-output: {e}")


def detect_silence_regions(
    filepath: str,
    threshold_db: float = -35.0,
    min_duration: float = 0.3
) -> List[SilenceRegion]:
    """
    Detektera tysta regioner i en ljudfil med ffmpeg silencedetect.

    Args:
        filepath: Sökväg till ljudfilen
        threshold_db: Tröskelvärde i dB (lägre = känsligare)
        min_duration: Minsta tystnadslängd i sekunder

    Returns:
        Lista med SilenceRegion sorterad på starttid

    Raises:
        FileNotFoundError: Om filen inte finns
        RuntimeError: Om ffmpeg misslyckas
    """
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"HARDFAIL: Ljudfil finns inte: {filepath}")

    try:
        result = subprocess.run(
            [
                _get_ffmpeg(),
                '-i', filepath,
                '-af', f'silencedetect=n={threshold_db}dB:d={min_duration}',
                '-f', 'null',
                '-'
            ],
            capture_output=True,
            text=True,
            timeout=120
        )

        # silencedetect skriver till stderr (normalt ffmpeg-beteende)
        stderr = result.stderr

        # Parsa silence_start och silence_end/silence_duration
        starts = re.findall(r'silence_start:\s*([\d.]+)', stderr)
        ends = re.findall(r'silence_end:\s*([\d.]+)\s*\|\s*silence_duration:\s*([\d.]+)', stderr)

        regions = []
        for i, start_str in enumerate(starts):
            start = float(start_str)
            if i < len(ends):
                end = float(ends[i][0])
                duration = float(ends[i][1])
            else:
                # Sista tystnad som inte avslutas (fil slutar med tystnad)
                end = start + min_duration
                duration = min_duration

            regions.append(SilenceRegion(start=start, end=end, duration=duration))

        LOGGER.debug(f"Silence detection: {len(regions)} tysta regioner i {filepath}")

        return regions

    except subprocess.TimeoutExpired:
        raise RuntimeError(f"HARDFAIL: ffmpeg silence detection timeout för {filepath}")


def _find_best_silence(
    target_time: float,
    silence_regions: List[SilenceRegion],
    search_window: float = _SILENCE_SEARCH_WINDOW
) -> Optional[float]:
    """
    Hitta den bästa klipppunkten (mittpunkt av närmaste tystnad) nära target_time.

    Args:
        target_time: Ideal klipppunkt i sekunder
        silence_regions: Lista med tysta regioner
        search_window: Sökfönster i sekunder runt target_time (±)

    Returns:
        Mittpunkt av närmaste tystnad, eller None om ingen hittas inom fönstret
    """
    best_point = None
    best_distance = float('inf')

    for region in silence_regions:
        midpoint = (region.start + region.end) / 2.0

        distance = abs(midpoint - target_time)
        if distance <= search_window and distance < best_distance:
            best_distance = distance
            best_point = midpoint

    return best_point


def calculate_chunks(
    audio_info: AudioInfo,
    chunk_size_bytes: int,
    overlap_seconds: float = 0.0,
    silence_regions: Optional[List[SilenceRegion]] = None
) -> List[ChunkInfo]:
    """
    Beräkna chunk-gränser för en ljudfil med silence-aware splitting.

    Chunkar vid tysta partier nära ideal-gränsen. Om silence_regions ges
    söks närmaste tystnad inom ±30s av varje ideal klipppunkt. Lägger till
    konfigurerbart overlap mellan chunks.

    Args:
        audio_info: Metadata om ljudfilen
        chunk_size_bytes: Önskad chunk-storlek i bytes (avgör ideal tidslängd)
        overlap_seconds: Sekunder overlap med föregående chunk
        silence_regions: Tysta regioner från detect_silence_regions()

    Returns:
        Lista med ChunkInfo för varje chunk
    """
    if chunk_size_bytes <= 0:
        raise ValueError(f"HARDFAIL: Ogiltig chunk_size_bytes: {chunk_size_bytes}")

    # Beräkna ideal chunk-längd i sekunder
    ideal_chunk_seconds = chunk_size_bytes / audio_info.bytes_per_second
    total_duration = audio_info.duration_seconds

    # Om filen är kortare än en chunk: returnera hela filen
    if total_duration <= ideal_chunk_seconds:
        return [ChunkInfo(
            chunk_index=0,
            start_byte=0,
            end_byte=audio_info.total_bytes,
            start_time_seconds=0.0,
            end_time_seconds=total_duration,
            start_timestamp=seconds_to_timestamp(0.0),
            end_timestamp=seconds_to_timestamp(total_duration),
            overlap_seconds=0.0
        )]

    chunks = []
    chunk_index = 0
    current_time = 0.0

    while current_time < total_duration:
        # Starttid (med overlap bakåt, utom för första chunk)
        if chunk_index == 0:
            start_time = 0.0
            chunk_overlap = 0.0
        else:
            start_time = max(0.0, current_time - overlap_seconds)
            chunk_overlap = current_time - start_time

        # Ideal sluttid
        ideal_end = current_time + ideal_chunk_seconds

        # Om vi är nära slutet: ta resten
        if ideal_end >= total_duration - (ideal_chunk_seconds * 0.2):
            end_time = total_duration
        else:
            # Sök tystnad nära ideal sluttid
            if silence_regions:
                silence_point = _find_best_silence(ideal_end, silence_regions)
                end_time = silence_point if silence_point is not None else ideal_end
            else:
                end_time = ideal_end

        # Beräkna byte-positioner (proportionellt)
        start_byte = int(start_time * audio_info.bytes_per_second)
        end_byte = int(end_time * audio_info.bytes_per_second)
        end_byte = min(end_byte, audio_info.total_bytes)

        chunks.append(ChunkInfo(
            chunk_index=chunk_index,
            start_byte=start_byte,
            end_byte=end_byte,
            start_time_seconds=start_time,
            end_time_seconds=end_time,
            start_timestamp=seconds_to_timestamp(start_time),
            end_timestamp=seconds_to_timestamp(end_time),
            overlap_seconds=chunk_overlap
        ))

        # Nästa chunk startar vid sluttid (utan overlap — overlap läggs på vid start)
        current_time = end_time
        chunk_index += 1

    LOGGER.info(
        f"Beräknade {len(chunks)} chunks för {audio_info.filepath} "
        f"(silence={'ja' if silence_regions else 'nej'}, overlap={overlap_seconds}s)"
    )

    return chunks


def extract_audio_segment(
    input_path: str,
    output_path: str,
    start_seconds: float,
    end_seconds: float,
    normalize: bool = False
) -> str:
    """
    Extrahera ett segment från en ljudfil med ffmpeg.

    Args:
        input_path: Sökväg till källfilen
        output_path: Sökväg för output-filen
        start_seconds: Starttid i sekunder
        end_seconds: Sluttid i sekunder
        normalize: Om True, normalisera ljud med EBU R128 loudnorm

    Returns:
        Sökväg till den extraherade filen

    Raises:
        RuntimeError: Om ffmpeg misslyckas
    """
    start_ts = seconds_to_timestamp(start_seconds)
    end_ts = seconds_to_timestamp(end_seconds)

    if normalize:
        # Re-encoding med EBU R128 loudnorm
        cmd = [
            _get_ffmpeg(),
            '-y',
            '-i', input_path,
            '-ss', start_ts,
            '-to', end_ts,
            '-af', 'loudnorm=I=-16:TP=-1.5',
            '-c:a', 'aac',
            '-b:a', '128k',
            output_path
        ]
        timeout = 120  # Re-encoding tar längre
    else:
        # Stream copy (snabb, ingen omkodning)
        cmd = [
            _get_ffmpeg(),
            '-y',
            '-i', input_path,
            '-ss', start_ts,
            '-to', end_ts,
            '-c', 'copy',
            output_path
        ]
        timeout = 60

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout
        )

        if result.returncode != 0:
            raise RuntimeError(f"HARDFAIL: ffmpeg fel: {result.stderr}")

        if not os.path.exists(output_path):
            raise RuntimeError(f"HARDFAIL: ffmpeg skapade ingen fil: {output_path}")

        return output_path

    except subprocess.TimeoutExpired:
        raise RuntimeError("HARDFAIL: ffmpeg timeout vid extrahering av segment")


def extract_datetime_from_filename(filename: str) -> Optional[datetime]:
    """
    Extrahera datum och tid från filnamn.

    Stödda format:
    - Inspelning_YYYYMMDD_HHMM_xxx.m4a → datetime
    - recording_YYYY-MM-DD_HH-MM.m4a → datetime
    - Endast YYYYMMDD → datetime (00:00)

    Args:
        filename: Filnamn att parsa

    Returns:
        datetime eller None om ingen matchning
    """
    # Format 1: Inspelning_YYYYMMDD_HHMM
    pattern1 = r'Inspelning_(\d{4})(\d{2})(\d{2})_(\d{2})(\d{2})'
    match = re.search(pattern1, filename)
    if match:
        year, month, day, hour, minute = map(int, match.groups())
        return datetime(year, month, day, hour, minute)

    # Format 2: recording_YYYY-MM-DD_HH-MM
    pattern2 = r'recording_(\d{4})-(\d{2})-(\d{2})_(\d{2})-(\d{2})'
    match = re.search(pattern2, filename)
    if match:
        year, month, day, hour, minute = map(int, match.groups())
        return datetime(year, month, day, hour, minute)

    # Format 3: Bara datum YYYYMMDD
    pattern3 = r'(\d{4})(\d{2})(\d{2})'
    match = re.search(pattern3, filename)
    if match:
        year, month, day = map(int, match.groups())
        return datetime(year, month, day, 0, 0)

    return None


def format_duration(seconds: float) -> str:
    """Formatera duration som HH:MM:SS."""
    return seconds_to_timestamp(seconds)
