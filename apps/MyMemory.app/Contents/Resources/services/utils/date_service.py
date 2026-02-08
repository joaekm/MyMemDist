"""
DateService - Central datumhantering för MyMemory (OBJEKT-50)

Prioritetsordning för datumextraktion:
1. Frontmatter (timestamp_ingestion) - mest pålitligt
2. Filnamn (YYYY-MM-DD i valfritt filnamn) - Slack, Calendar, Mail, etc.
3. PDF-metadata (CreationDate) - för PDF-filer
4. Filsystem (birthtime/mtime) - alltid försöker sist

HARDFAIL om inget fungerar - fil ska då flyttas till Failed-mappen.

Användning:
    from services.utils.date_service import get_date, get_timestamp

    date_str = get_date(filepath)        # "2025-12-11"
    timestamp = get_timestamp(filepath)  # datetime object
"""

import os
import re
import yaml
import logging
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Optional

LOGGER = logging.getLogger('DateService')

# Ladda config för MIN_YEAR
from services.utils.config_loader import get_config

def _load_validation_config() -> dict:
    """Ladda validation-config."""
    try:
        config = get_config()
        return config.get('validation', {})
    except FileNotFoundError:
        LOGGER.warning("Config saknas, använder defaults")
        return {}

VALIDATION_CONFIG = _load_validation_config()
DATE_MIN_YEAR = VALIDATION_CONFIG.get('min_year', 2015)

# === ABSTRACT BASE ===

class DateExtractor(ABC):
    """Abstrakt basklass för datumextraktion."""
    
    @abstractmethod
    def can_extract(self, filepath: str) -> bool:
        """Returnerar True om denna extractor kan hantera filen."""
        pass
    
    @abstractmethod
    def extract(self, filepath: str) -> Optional[datetime]:
        """Extraherar datum, returnerar None om det inte går."""
        pass
    
    @property
    @abstractmethod
    def name(self) -> str:
        """Namn på extractorn för loggning."""
        pass


# === EXTRACTORS ===

class FrontmatterExtractor(DateExtractor):
    """
    Läser timestamp_ingestion från YAML frontmatter.

    Förväntat format i .md-filer:
    ---
    timestamp_ingestion: '2025-12-11T14:30:00+01:00'
    ---

    Validerar att datumet är rimligt (>= MIN_YEAR).
    """

    PATTERN = re.compile(r"timestamp_ingestion:\s*['\"]?([^'\"\n]+)")
    MIN_YEAR = DATE_MIN_YEAR  # Datum äldre än detta anses korrupt
    
    @property
    def name(self) -> str:
        return "frontmatter"
    
    def can_extract(self, filepath: str) -> bool:
        return filepath.lower().endswith('.md')
    
    def extract(self, filepath: str) -> Optional[datetime]:
        with open(filepath, 'r', encoding='utf-8') as f:
            # Läs bara första 2000 tecken (frontmatter är i början)
            content = f.read(2000)

        match = self.PATTERN.search(content)
        if not match:
            return None

        ts_str = match.group(1).strip()
        result = None

        # Försök parsa ISO-format
        # Hantera både med och utan timezone
        for fmt in [
            '%Y-%m-%dT%H:%M:%S.%f%z',
            '%Y-%m-%dT%H:%M:%S%z',
            '%Y-%m-%dT%H:%M:%S.%f',
            '%Y-%m-%dT%H:%M:%S',
            '%Y-%m-%d'
        ]:
            try:
                result = datetime.strptime(ts_str[:26].replace('+02:00', '+0200').replace('+01:00', '+0100'), fmt)
                break
            except ValueError:
                # Förväntat: prova nästa format
                continue

        # Fallback: försök fromisoformat
        if not result:
            try:
                result = datetime.fromisoformat(ts_str)
            except ValueError:
                LOGGER.debug(f"Kunde inte parsa datum: {ts_str[:30]}")
                return None

        # Validera att datumet är rimligt
        if result and result.year >= self.MIN_YEAR:
            return result

        LOGGER.debug(f"Frontmatter-datum {result.year if result else 'None'} för gammalt i {os.path.basename(filepath)}")
        return None


class GenericFilenameExtractor(DateExtractor):
    """
    Extraherar datum från filnamn med olika datumformat.

    Stödda format (i prioritetsordning):
    1. YYYY-MM-DD (med bindestreck): Slack_kanal_2025-12-11_uuid.txt
    2. YYYYMMDD_HHMM (inspelningar): Inspelning_20251208_0958_uuid.m4a
    3. YYYYMMDD (kompakt): rapport_20240615_final.pdf

    Validerar att året är rimligt (>= MIN_YEAR).
    """

    # Mönster i prioritetsordning (mer specifikt först)
    PATTERN_DASH = re.compile(r'(\d{4})-(\d{2})-(\d{2})')  # YYYY-MM-DD
    PATTERN_COMPACT_TIME = re.compile(r'(\d{4})(\d{2})(\d{2})_(\d{2})(\d{2})')  # YYYYMMDD_HHMM
    PATTERN_COMPACT = re.compile(r'[_\-](\d{4})(\d{2})(\d{2})[_\-]')  # _YYYYMMDD_ eller -YYYYMMDD-
    MIN_YEAR = DATE_MIN_YEAR

    @property
    def name(self) -> str:
        return "filename"

    def can_extract(self, filepath: str) -> bool:
        basename = os.path.basename(filepath)
        return (bool(self.PATTERN_DASH.search(basename)) or
                bool(self.PATTERN_COMPACT_TIME.search(basename)) or
                bool(self.PATTERN_COMPACT.search(basename)))

    def extract(self, filepath: str) -> Optional[datetime]:
        basename = os.path.basename(filepath)

        # 1. Försök YYYY-MM-DD (t.ex. Slack_kanal_2025-12-11_uuid.txt)
        match = self.PATTERN_DASH.search(basename)
        if match:
            year, month, day = int(match.group(1)), int(match.group(2)), int(match.group(3))
            if 1 <= month <= 12 and 1 <= day <= 31 and year >= self.MIN_YEAR:
                return datetime(year, month, day)

        # 2. Försök YYYYMMDD_HHMM (t.ex. Inspelning_20251208_0958_uuid.m4a)
        match = self.PATTERN_COMPACT_TIME.search(basename)
        if match:
            year = int(match.group(1))
            month = int(match.group(2))
            day = int(match.group(3))
            hour = int(match.group(4))
            minute = int(match.group(5))
            if 1 <= month <= 12 and 1 <= day <= 31 and 0 <= hour <= 23 and 0 <= minute <= 59 and year >= self.MIN_YEAR:
                return datetime(year, month, day, hour, minute)

        # 3. Försök _YYYYMMDD_ (generellt kompakt format)
        match = self.PATTERN_COMPACT.search(basename)
        if match:
            year, month, day = int(match.group(1)), int(match.group(2)), int(match.group(3))
            if 1 <= month <= 12 and 1 <= day <= 31 and year >= self.MIN_YEAR:
                return datetime(year, month, day)

        LOGGER.debug(f"Kunde inte parsa datum från {basename}")
        return None


class PDFExtractor(DateExtractor):
    """
    Läser CreationDate från PDF-metadata.
    
    Kräver: pymupdf (fitz)
    """
    
    @property
    def name(self) -> str:
        return "pdf_metadata"
    
    def can_extract(self, filepath: str) -> bool:
        return filepath.lower().endswith('.pdf')
    
    def extract(self, filepath: str) -> Optional[datetime]:
        try:
            import fitz  # type: ignore
            
            with fitz.open(filepath) as doc:
                metadata = doc.metadata
                if not metadata:
                    return None
                    
                creation_date_str = metadata.get('creationDate', '')
                
                # PDF-format: D:YYYYMMDDHHmmSS+TZ
                # Exempel: D:20231215143052+01'00'
                if creation_date_str and creation_date_str.startswith('D:'):
                    date_part = creation_date_str[2:16]  # YYYYMMDDHHMMSS
                    try:
                        return datetime.strptime(date_part, '%Y%m%d%H%M%S')
                    except ValueError:
                        LOGGER.debug(f"Kunde inte parsa PDF-datum med tid: {date_part}")
                        # Försök bara datum
                        try:
                            return datetime.strptime(date_part[:8], '%Y%m%d')
                        except ValueError:
                            LOGGER.debug(f"Kunde inte parsa PDF-datum: {date_part[:8]}")
                            
        except ImportError:
            LOGGER.warning("pymupdf (fitz) inte installerat - kan inte läsa PDF-metadata")
            return None


class FilesystemExtractor(DateExtractor):
    """
    Fallback: använder filsystemets metadata.
    
    Prioritet:
    1. st_birthtime (om tillgängligt och rimligt)
    2. st_mtime (modifieringsdatum)
    
    Validering: Datum äldre än MIN_YEAR anses korrupt.
    """
    
    MIN_YEAR = DATE_MIN_YEAR  # Äldre anses korrupt (1984-datum etc.)
    
    @property
    def name(self) -> str:
        return "filesystem"
    
    def can_extract(self, filepath: str) -> bool:
        return os.path.exists(filepath)
    
    def extract(self, filepath: str) -> Optional[datetime]:
        stat = os.stat(filepath)

        # Försök birthtime först (macOS)
        if hasattr(stat, 'st_birthtime'):
            birthtime = datetime.fromtimestamp(stat.st_birthtime)
            if birthtime.year >= self.MIN_YEAR:
                return birthtime
            else:
                LOGGER.debug(f"birthtime {birthtime.year} för gammal, använder mtime")

        # Fallback till mtime
        mtime = datetime.fromtimestamp(stat.st_mtime)
        if mtime.year >= self.MIN_YEAR:
            return mtime

        # Även mtime är korrupt
        LOGGER.warning(f"Både birthtime och mtime är korrupta för {filepath}")
        return None


# === PRIORITERAD LISTA AV EXTRACTORS ===

EXTRACTORS = [
    FrontmatterExtractor(),
    GenericFilenameExtractor(),
    PDFExtractor(),
    FilesystemExtractor(),  # Alltid sist som fallback
]


# === HUVUDFUNKTIONER ===

def get_timestamp(filepath: str) -> datetime:
    """
    Hämta timestamp för en fil.
    
    Försöker alla extractors i prioritetsordning tills en lyckas.
    
    Args:
        filepath: Sökväg till filen
        
    Returns:
        datetime-objekt
        
    Raises:
        RuntimeError: HARDFAIL_DATE om inget datum kan extraheras
    """
    for extractor in EXTRACTORS:
        if extractor.can_extract(filepath):
            result = extractor.extract(filepath)
            if result:
                LOGGER.debug(f"Datum från {extractor.name}: {result} för {os.path.basename(filepath)}")
                return result
    
    raise RuntimeError(f"HARDFAIL_DATE: Kunde inte extrahera datum från {filepath}")


def get_date(filepath: str) -> str:
    """
    Hämta datum som YYYY-MM-DD sträng.
    
    Args:
        filepath: Sökväg till filen
        
    Returns:
        Datum som "YYYY-MM-DD" sträng
        
    Raises:
        RuntimeError: HARDFAIL_DATE om inget datum kan extraheras
    """
    return get_timestamp(filepath).strftime('%Y-%m-%d')


# === TEST ===

if __name__ == "__main__":
    import sys
    import yaml
    
    logging.basicConfig(level=logging.DEBUG, format='%(levelname)s - %(message)s')
    
    # Ladda sökvägar från config
    def _load_test_paths():
        try:
            config = get_config()
            return [
                os.path.expanduser(config['paths']['lake_store']),
                os.path.expanduser(config['paths']['asset_documents']),
            ]
        except FileNotFoundError as e:
            print(f"Config saknas: {e}")
            return []
    
    if len(sys.argv) < 2:
        print("Användning: python date_service.py <filepath> [filepath2] ...")
        print("\nTestar med exempelfiler...")
        
        test_paths = _load_test_paths()
        
        for test_dir in test_paths:
            if os.path.exists(test_dir):
                print(f"\n=== {test_dir} ===")
                files = [f for f in os.listdir(test_dir) if not f.startswith('.')][:5]
                for f in files:
                    filepath = os.path.join(test_dir, f)
                    if os.path.isfile(filepath):
                        try:
                            date = get_date(filepath)
                            ts = get_timestamp(filepath)
                            print(f"  {f[:50]:50} → {date} ({ts})")
                        except RuntimeError as e:
                            LOGGER.error(f"HARDFAIL för {f}: {e}")
                            print(f"  {f[:50]:50} → HARDFAIL: {e}")
    else:
        for filepath in sys.argv[1:]:
            filepath = os.path.expanduser(filepath)
            try:
                date = get_date(filepath)
                ts = get_timestamp(filepath)
                print(f"{filepath}")
                print(f"  Date: {date}")
                print(f"  Timestamp: {ts}")
            except RuntimeError as e:
                LOGGER.error(f"ERROR för {filepath}: {e}")
                print(f"{filepath}")
                print(f"  ERROR: {e}")
