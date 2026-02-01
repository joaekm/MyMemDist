import os
import yaml
import logging
import threading
from typing import Dict, Any, Optional, List

LOGGER = logging.getLogger("LakeService")

class LakeService:
    """
    LakeService - Kirurgiska ingrepp i Lake-filer.

    Ansvar:
    - Läsa och skriva YAML-frontmatter säkert.
    - Aldrig röra brödtexten (content).
    - Hantera samtidighet (enkel låsning).
    - Stödja UTF-8 och svenska tecken.
    """
    
    _file_locks = {}
    _global_lock = threading.Lock()

    def __init__(self, lake_path: str = None):
        # Om ingen path ges, försök gissa via config (men helst ska den injiceras)
        self.lake_path = lake_path

    def _get_lock(self, filepath: str):
        with self._global_lock:
            if filepath not in self._file_locks:
                self._file_locks[filepath] = threading.Lock()
            return self._file_locks[filepath]

    def read_metadata(self, filepath: str) -> Dict[str, Any]:
        """Läser enbart frontmatter från en fil."""
        if not os.path.exists(filepath):
            LOGGER.error(f"Fil saknas: {filepath}")
            return {}

        with self._get_lock(filepath):
            try:
                with open(filepath, 'r', encoding='utf-8') as f:
                    content = f.read()
                
                if not content.startswith("---"):
                    return {}
                
                parts = content.split("---", 2)
                if len(parts) < 3:
                    return {}
                
                return yaml.safe_load(parts[1]) or {}
            except Exception as e:
                LOGGER.error(f"Kunde inte läsa metadata från {filepath}: {e}")
                return {}

    def update_metadata(self, filepath: str, updates: Dict[str, Any]) -> bool:
        """
        Uppdaterar specifika fält i frontmatter.
        Merge-strategi: Shallow merge (skriver över toppnivå-nycklar).
        """
        if not os.path.exists(filepath):
            LOGGER.error(f"Kan inte uppdatera, fil saknas: {filepath}")
            return False

        lock = self._get_lock(filepath)
        with lock:
            try:
                # 1. Läs in hela filen
                with open(filepath, 'r', encoding='utf-8') as f:
                    content = f.read()

                # 2. Separera Header och Body
                if not content.startswith("---"):
                    LOGGER.warning(f"Fil saknar YAML-block: {filepath}")
                    return False
                
                parts = content.split("---", 2)
                if len(parts) < 3:
                    LOGGER.warning(f"Filstruktur ogiltig (delar saknas): {filepath}")
                    return False

                header_raw = parts[1]
                body = parts[2] # Allt efter andra '---'

                # 3. Parsa och Uppdatera
                metadata = yaml.safe_load(header_raw) or {}
                
                # Applicera ändringar
                changes_made = False
                for k, v in updates.items():
                    if metadata.get(k) != v:
                        metadata[k] = v
                        changes_made = True
                
                if not changes_made:
                    return True # Inget att göra, men "lyckades"

                # 4. Dumpa tillbaka (MED UNICODE-STÖD)
                new_header = yaml.dump(metadata, sort_keys=False, allow_unicode=True)

                # 5. Skriv tillbaka atomärt (nästan)
                with open(filepath, 'w', encoding='utf-8') as f:
                    f.write("---\n")
                    f.write(new_header)
                    f.write("---") # yaml.dump lägger ofta till en nyrad, parts[2] brukar börja med \n
                    f.write(body)
                
                LOGGER.info(f"Uppdaterade metadata i {os.path.basename(filepath)}: {list(updates.keys())}")
                return True

            except Exception as e:
                LOGGER.error(f"Kritist fel vid uppdatering av {filepath}: {e}")
                return False

    def update_semantics(self, filepath: str, context_summary: Optional[str] = None,
                        relations_summary: Optional[str] = None,
                        document_keywords: Optional[List[str]] = None,
                        set_timestamp_updated: bool = True) -> bool:
        """
        Uppdaterar semantiska fält (context_summary, relations_summary, document_keywords).
        Kan uppdatera en, flera eller alla fält atomärt via update_metadata.

        Detta är det ENDA sättet att ändra dessa fält för att garantera
        att Dreamers LLM-kurering respekteras.

        Args:
            filepath: Sökväg till Lake-filen
            context_summary: Ny sammanfattning (eller None för att behålla)
            relations_summary: Ny relationsbeskrivning (eller None)
            document_keywords: Nya nyckelord (eller None)
            set_timestamp_updated: Om True, sätts timestamp_updated till nu (default: True)
        """
        import datetime

        updates = {}

        if context_summary is not None:
            updates['context_summary'] = context_summary

        if relations_summary is not None:
            updates['relations_summary'] = relations_summary

        if document_keywords is not None:
            if not isinstance(document_keywords, list):
                LOGGER.warning(f"document_keywords måste vara en lista, fick {type(document_keywords)}")
                return False
            updates['document_keywords'] = document_keywords

        # Sätt timestamp_updated om semantiska fält faktiskt ändras
        if updates and set_timestamp_updated:
            updates['timestamp_updated'] = datetime.datetime.now().isoformat()

        if not updates:
            LOGGER.warning(f"Inga uppdateringar angivna för update_semantics i {filepath}")
            return False

        return self.update_metadata(filepath, updates)