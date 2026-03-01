"""
Shared configuration, singletons, and state for the ingestion package.

All submodules import from here instead of duplicating config loading.
"""

import os
import sys
import logging
import re
import threading

from services.utils.config_loader import get_config, get_prompts, get_expanded_paths
from services.utils.llm_service import LLMService
from services.utils.schema_validator import SchemaValidator

# --- LOGGING SETUP (FileHandler only, no terminal output) ---
try:
    _early_cfg = get_config()
except FileNotFoundError:
    logging.debug("Config not found at startup — using default log path")
    _early_cfg = {}

_log_file = os.path.expanduser(
    _early_cfg.get('logging', {}).get('system_log', '~/Library/Logs/MyMemory/system.log')
)
os.makedirs(os.path.dirname(_log_file), exist_ok=True)

_root = logging.getLogger()
_root.setLevel(logging.INFO)
for _h in _root.handlers[:]:
    _root.removeHandler(_h)

_fh = logging.FileHandler(_log_file)
_fh.setFormatter(logging.Formatter('%(asctime)s - INGESTION - %(levelname)s - %(message)s'))
_root.addHandler(_fh)

# Silence third-party loggers
for _name in ['httpx', 'httpcore', 'mcp', 'google', 'google_genai', 'anyio', 'watchdog']:
    logging.getLogger(_name).setLevel(logging.WARNING)


# --- CONFIG ---
_raw_config = get_config()
CONFIG = dict(_raw_config)
CONFIG['paths'] = get_expanded_paths()
PROMPTS_RAW = get_prompts()


def get_prompt(agent: str, key: str) -> str:
    """Get prompt from config."""
    if 'prompts' in PROMPTS_RAW:
        return PROMPTS_RAW['prompts'].get(agent, {}).get(key)
    return PROMPTS_RAW.get(agent, {}).get(key)


# --- PATHS ---
LAKE_STORE = os.path.expanduser(CONFIG['paths']['lake_store'])
FAILED_FOLDER = os.path.expanduser(CONFIG['paths']['asset_failed'])
GRAPH_DB_PATH = os.path.expanduser(CONFIG['paths']['graph_db'])

# --- PROCESSING SETTINGS ---
PROCESSING_CONFIG = CONFIG.get('processing', {})
SUMMARY_MAX_CHARS = PROCESSING_CONFIG.get('summary_max_chars', 30000)
HEADER_SCAN_CHARS = PROCESSING_CONFIG.get('header_scan_chars', 3000)

# --- ENRICHMENT STATE (OBJEKT-76) ---
ENRICHMENT_STATE_FILE = os.path.expanduser(
    CONFIG.get('enrichment', {}).get('daemon', {}).get(
        'state_file', '~/Library/Application Support/MyMemory/enrichment_state.json'
    )
)
ENRICHMENT_STATE_LOCK = threading.Lock()

# --- LOGGER ---
LOGGER = logging.getLogger('IngestionEngine')

# --- SINGLETONS (lazy) ---
_LLM_SERVICE = None
_SCHEMA_VALIDATOR = None


def _get_llm_service():
    global _LLM_SERVICE
    if _LLM_SERVICE is None:
        _LLM_SERVICE = LLMService()
    return _LLM_SERVICE


def _get_schema_validator():
    global _SCHEMA_VALIDATOR
    if _SCHEMA_VALIDATOR is None:
        try:
            _SCHEMA_VALIDATOR = SchemaValidator()
        except Exception as e:
            LOGGER.error(f"Could not load SchemaValidator: {e}")
            raise
    return _SCHEMA_VALIDATOR


# --- REGEX PATTERNS ---
UUID_SUFFIX_PATTERN = re.compile(
    r'_([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})\.(txt|md|pdf|docx|csv|xlsx)$',
    re.IGNORECASE
)

# --- RUNTIME STATE ---
PROCESSED_FILES = set()
PROCESS_LOCK = threading.Lock()

# Last ingestion critic results (for quality reporting / test compat)
_last_critic_approved = []
_last_critic_rejected = []

# Last ingestion payload after post_process_edges (for write-through verification)
_last_ingestion_payload = []

# Last raw text and semantic metadata (for quality reporting)
_last_raw_text = ""
_last_semantic_metadata = {}
