#!/usr/bin/env python3
"""
Meeting Transcriber Daemon

Lyssnar på Icecast-stream och transkriberar till chunks via Gemini API.
Startas som subprocess av mymem_mcp.py.

Flöde:
  Icecast → ffmpeg → 30 sek WAV-buffer → Gemini API → chunk.txt
"""

import sys
import asyncio
import signal
import wave
import io
import os
import logging
from pathlib import Path
from datetime import datetime

import yaml
from google import genai
from google.genai import types


# --- CONFIG ---
# Lägg till project root till path för imports
import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from services.utils.config_loader import get_config

CONFIG = get_config()
MEETING_CONFIG = CONFIG.get('meeting_transcriber', {})

# Paths
BUFFER_DIR = Path(os.path.expanduser(MEETING_CONFIG.get('buffer_dir', '~/MyMemory/MeetingBuffer')))
CHUNKS_DIR = BUFFER_DIR / "chunks"

# Icecast
ICECAST_URL = MEETING_CONFIG.get('icecast_url', 'http://localhost:8000/meeting')

# API
GOOGLE_API_KEY = CONFIG.get('ai_engine', {}).get('api_key')

# Audio settings
SAMPLE_RATE = 16000  # 16kHz
CHANNELS = 1         # Mono
SAMPLE_WIDTH = 2     # 16-bit (2 bytes)

# Buffring
BUFFER_SECONDS = MEETING_CONFIG.get('buffer_seconds', 30)


class BatchTranscriber:
    def __init__(self):
        self.client = genai.Client(api_key=GOOGLE_API_KEY)
        self.chunk_count = 0
        self.running = True
        self.ffmpeg_process = None

        # Skapa output-mapp
        CHUNKS_DIR.mkdir(parents=True, exist_ok=True)

        # Beräkna bytes per buffer
        self.bytes_per_buffer = SAMPLE_RATE * CHANNELS * SAMPLE_WIDTH * BUFFER_SECONDS

    async def run(self):
        """Huvudloop: buffra audio, transkribera, skriv chunks."""
        self._log(f"Startar Meeting Transcriber")
        self._log(f"Icecast URL: {ICECAST_URL}")
        self._log(f"Output: {CHUNKS_DIR}")
        self._log(f"Buffer: {BUFFER_SECONDS} sekunder per batch")

        await self._start_ffmpeg()

        if not self.ffmpeg_process:
            self._log("ERROR: Kunde inte starta ffmpeg")
            return

        self._log("Lyssnar på mötet...")

        try:
            while self.running:
                # Buffra audio
                audio_data = await self._buffer_audio()

                if not audio_data:
                    break

                # Transkribera
                transcript = await self._transcribe(audio_data)

                if transcript:
                    self._write_chunk(transcript)

        except asyncio.CancelledError:
            self._log("Avbryter...")
        finally:
            await self._cleanup_ffmpeg()

    async def _start_ffmpeg(self):
        """Starta ffmpeg-processen."""
        ffmpeg_cmd = [
            "ffmpeg",
            "-reconnect", "1",
            "-reconnect_streamed", "1",
            "-reconnect_delay_max", "5",
            "-i", ICECAST_URL,
            "-f", "s16le",
            "-acodec", "pcm_s16le",
            "-ar", str(SAMPLE_RATE),
            "-ac", str(CHANNELS),
            "-"
        ]

        self._log("Startar ffmpeg...")

        try:
            self.ffmpeg_process = await asyncio.create_subprocess_exec(
                *ffmpeg_cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.DEVNULL
            )
        except FileNotFoundError:
            logging.error("ffmpeg not found - install with 'brew install ffmpeg'")
        except OSError as e:
            logging.error(f"Could not start ffmpeg: {e}")

    async def _cleanup_ffmpeg(self):
        """Stäng ner ffmpeg."""
        if self.ffmpeg_process:
            try:
                self.ffmpeg_process.terminate()
                await asyncio.wait_for(self.ffmpeg_process.wait(), timeout=5)
            except (ProcessLookupError, asyncio.TimeoutError):
                pass

    async def _buffer_audio(self) -> bytes:
        """Buffra BUFFER_SECONDS sekunder av audio."""
        buffer = bytearray()
        target_bytes = self.bytes_per_buffer

        while len(buffer) < target_bytes and self.running:
            try:
                chunk = await asyncio.wait_for(
                    self.ffmpeg_process.stdout.read(4096),
                    timeout=5.0
                )
            except asyncio.TimeoutError:
                continue

            if not chunk:
                self._log("Stream avslutad")
                return None

            buffer.extend(chunk)

        return bytes(buffer)

    async def _transcribe(self, audio_data: bytes) -> str:
        """Skicka audio till Gemini och få transkription."""
        # Konvertera PCM till WAV i minnet
        wav_buffer = io.BytesIO()
        with wave.open(wav_buffer, 'wb') as wav_file:
            wav_file.setnchannels(CHANNELS)
            wav_file.setsampwidth(SAMPLE_WIDTH)
            wav_file.setframerate(SAMPLE_RATE)
            wav_file.writeframes(audio_data)

        wav_bytes = wav_buffer.getvalue()

        try:
            response = self.client.models.generate_content(
                model="gemini-2.0-flash",
                contents=[
                    "Transkribera detta ljudklipp på svenska. Skriv ENDAST vad som sägs, inga kommentarer.",
                    types.Part.from_bytes(data=wav_bytes, mime_type="audio/wav")
                ]
            )

            transcript = response.text.strip() if response.text else ""
            return transcript

        except (ConnectionError, TimeoutError) as e:
            logging.warning(f"Network error during transcription: {e}")
            return ""
        except ValueError as e:
            logging.warning(f"Invalid response from Gemini: {e}")
            return ""

    def _write_chunk(self, transcript: str):
        """Skriv transkription som chunk-fil."""
        self.chunk_count += 1
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = CHUNKS_DIR / f"{timestamp}_chunk{self.chunk_count:02d}.txt"

        filename.write_text(transcript)
        self._log(f"Chunk #{self.chunk_count}: {filename.name}")

    def stop(self):
        """Stoppa transkribering."""
        self.running = False

    def _log(self, msg: str):
        """Logga till stderr (stdout används inte för att undvika MCP-konflikt)."""
        timestamp = datetime.now().strftime("%H:%M:%S")
        print(f"[{timestamp}] TRANSCRIBER: {msg}", file=sys.stderr)


async def main():
    if not GOOGLE_API_KEY:
        print("ERROR: API-nyckel saknas i config/my_mem_config.yaml", file=sys.stderr)
        sys.exit(1)

    transcriber = BatchTranscriber()

    # Hantera signaler
    def signal_handler():
        transcriber.stop()

    loop = asyncio.get_event_loop()
    loop.add_signal_handler(signal.SIGINT, signal_handler)
    loop.add_signal_handler(signal.SIGTERM, signal_handler)

    try:
        await transcriber.run()
    except asyncio.CancelledError:
        pass  # Normal shutdown
    except KeyboardInterrupt:
        pass  # Ctrl+C
    finally:
        print(f"Avslutad. Totalt {transcriber.chunk_count} chunks.", file=sys.stderr)


if __name__ == "__main__":
    asyncio.run(main())
