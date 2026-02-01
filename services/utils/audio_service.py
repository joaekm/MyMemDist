"""
Audio Service - Hjälpfunktioner för ljudfilshantering.

Använder ffprobe/ffmpeg för:
- Hämta metadata (duration, storlek, format)
- Chunking av stora ljudfiler
- Extrahera segment för parallell transkribering

Alla parametrar läses från config.
"""

import os
import subprocess
import json
import re
import logging
from dataclasses import dataclass
from typing import List, Optional
from datetime import datetime

# Logger
LOGGER = logging.getLogger('MyMem_AudioService')


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
class ChunkInfo:
    """Information om en chunk."""
    chunk_index: int
    start_byte: int
    end_byte: int
    start_time_seconds: float
    end_time_seconds: float
    start_timestamp: str  # "HH:MM:SS"
    end_timestamp: str    # "HH:MM:SS"


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
                'ffprobe',
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


def calculate_chunks(audio_info: AudioInfo, chunk_size_bytes: int) -> List[ChunkInfo]:
    """
    Beräkna chunk-gränser för en ljudfil.

    Args:
        audio_info: Metadata om ljudfilen
        chunk_size_bytes: Önskad chunk-storlek i bytes

    Returns:
        Lista med ChunkInfo för varje chunk
    """
    if chunk_size_bytes <= 0:
        raise ValueError(f"HARDFAIL: Ogiltig chunk_size_bytes: {chunk_size_bytes}")

    chunks = []
    current_byte = 0
    chunk_index = 0

    while current_byte < audio_info.total_bytes:
        start_byte = current_byte
        end_byte = min(start_byte + chunk_size_bytes, audio_info.total_bytes)

        # Beräkna tider baserat på bytes
        start_time = start_byte / audio_info.bytes_per_second
        end_time = end_byte / audio_info.bytes_per_second

        chunks.append(ChunkInfo(
            chunk_index=chunk_index,
            start_byte=start_byte,
            end_byte=end_byte,
            start_time_seconds=start_time,
            end_time_seconds=end_time,
            start_timestamp=seconds_to_timestamp(start_time),
            end_timestamp=seconds_to_timestamp(end_time)
        ))

        current_byte = end_byte
        chunk_index += 1

    LOGGER.debug(f"Beräknade {len(chunks)} chunks för {audio_info.filepath}")

    return chunks


def extract_audio_segment(
    input_path: str,
    output_path: str,
    start_seconds: float,
    end_seconds: float
) -> str:
    """
    Extrahera ett segment från en ljudfil med ffmpeg.

    Använder stream copy (ingen omkodning) för snabbhet.

    Args:
        input_path: Sökväg till källfilen
        output_path: Sökväg för output-filen
        start_seconds: Starttid i sekunder
        end_seconds: Sluttid i sekunder

    Returns:
        Sökväg till den extraherade filen

    Raises:
        RuntimeError: Om ffmpeg misslyckas
    """
    start_ts = seconds_to_timestamp(start_seconds)
    end_ts = seconds_to_timestamp(end_seconds)

    try:
        result = subprocess.run(
            [
                'ffmpeg',
                '-y',  # Overwrite output
                '-i', input_path,
                '-ss', start_ts,
                '-to', end_ts,
                '-c', 'copy',  # Stream copy, ingen omkodning
                output_path
            ],
            capture_output=True,
            text=True,
            timeout=60
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
