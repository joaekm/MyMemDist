#!/usr/bin/env python3
"""
Ingestion Engine (v12.0)

Integrates normalized data into the knowledge system.
Phase 2 of the pipeline: Collect & Normalize -> INGESTION -> Dreaming

Responsibilities:
- Generate semantic metadata (summary, keywords)
- Extract entities via MCP
- Resolve entities against existing graph
- Write to Lake, Graph, Vector
"""

# Prevent tokenizers fork crash on macOS (must be before any imports)
import os
os.environ["TOKENIZERS_PARALLELISM"] = "false"
import sys
import signal
import time
import yaml
import logging
import datetime
import threading
import re
import copy
import uuid
import asyncio
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, Any, List

# --- SIGTERM handler för graceful shutdown ---
# OBS: os._exit() istället för sys.exit() - undviker ThreadPoolExecutor cleanup-problem
# Se poc/process/signal_logging_poc.py (Test 5)
_shutdown_requested = False

def _handle_sigterm(signum, frame):
    global _shutdown_requested
    _shutdown_requested = True
    os._exit(0)

signal.signal(signal.SIGTERM, _handle_sigterm)
signal.signal(signal.SIGINT, _handle_sigterm)

# --- LOGGING SETUP (FileHandler only, no terminal output) ---
_log_file = os.path.expanduser('~/MyMemory/Logs/my_mem_system.log')
os.makedirs(os.path.dirname(_log_file), exist_ok=True)

_root = logging.getLogger()
_root.setLevel(logging.INFO)
for _h in _root.handlers[:]:
    _root.removeHandler(_h)

_fh = logging.FileHandler(_log_file)
_fh.setFormatter(logging.Formatter('%(asctime)s - INGESTION - %(levelname)s - %(message)s'))
_root.addHandler(_fh)

# --- IMPORTS ---
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from services.utils.json_parser import parse_llm_json
from services.utils.llm_service import LLMService
from services.utils.graph_service import GraphService
from services.utils.schema_validator import SchemaValidator, normalize_value
from services.processors.text_extractor import extract_text
from services.utils.shared_lock import resource_lock
from services.utils.metadata_service import generate_semantic_metadata, get_owner_name
from services.utils.parts_parser_service import (
    has_transcript_parts,
    extract_transcript_parts,
    build_chunk_text,
    chunk_document,
    build_overview_chunk_text
)

from services.utils.date_service import get_timestamp as date_service_timestamp

# Tysta tredjepartsloggers EFTER import
for _name in ['httpx', 'httpcore', 'mcp', 'google', 'google_genai', 'anyio', 'watchdog']:
    logging.getLogger(_name).setLevel(logging.WARNING)


# --- CONFIG LOADER ---
from services.utils.config_loader import get_config, get_prompts, get_expanded_paths

_raw_config = get_config()
# Expandera paths
CONFIG = dict(_raw_config)
CONFIG['paths'] = get_expanded_paths()
PROMPTS_RAW = get_prompts()


def get_prompt(agent: str, key: str) -> str:
    """Get prompt from config."""
    if 'prompts' in PROMPTS_RAW:
        return PROMPTS_RAW['prompts'].get(agent, {}).get(key)
    return PROMPTS_RAW.get(agent, {}).get(key)


# Settings
LAKE_STORE = os.path.expanduser(CONFIG['paths']['lake_store'])
FAILED_FOLDER = os.path.expanduser(CONFIG['paths']['asset_failed'])
GRAPH_DB_PATH = os.path.expanduser(CONFIG['paths']['graph_db'])

# Dreamer daemon state file (OBJEKT-76)
DREAMER_STATE_FILE = os.path.expanduser(
    CONFIG.get('dreamer', {}).get('daemon', {}).get(
        'state_file', '~/MyMemory/Index/.dreamer_state.json'
    )
)

# LLMService singleton (lazy init)
_LLM_SERVICE = None

def _get_llm_service():
    global _LLM_SERVICE
    if _LLM_SERVICE is None:
        _LLM_SERVICE = LLMService()
    return _LLM_SERVICE

# Processing limits from config
PROCESSING_CONFIG = CONFIG.get('processing', {})
SUMMARY_MAX_CHARS = PROCESSING_CONFIG.get('summary_max_chars', 30000)
HEADER_SCAN_CHARS = PROCESSING_CONFIG.get('header_scan_chars', 3000)

# Logger (configured in _setup_logging above)
LOGGER = logging.getLogger('IngestionEngine')

# Terminal status (visual feedback)
from services.utils.terminal_status import status as terminal_status, entity_detail

UUID_SUFFIX_PATTERN = re.compile(
    r'_([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})\.(txt|md|pdf|docx|csv|xlsx)$',
    re.IGNORECASE
)
STANDARD_TIMESTAMP_PATTERN = re.compile(r'^DATUM_TID:\s+(.+)$', re.MULTILINE)
TRANSCRIBER_DATE_PATTERN = re.compile(r'^DATUM:\s+(\d{4}-\d{2}-\d{2})$', re.MULTILINE)
TRANSCRIBER_START_PATTERN = re.compile(r'^START:\s+(\d{2}:\d{2})$', re.MULTILINE)
RICH_TRANSCRIBER_PATTERN = re.compile(
    r'^\*\*Tid:\*\*\s+(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})', re.MULTILINE
)

PROCESSED_FILES = set()
PROCESS_LOCK = threading.Lock()

# Last ingestion critic results (for quality reporting)
_last_critic_approved = []
_last_critic_rejected = []

# Last ingestion payload after post_process_edges (for write-through verification)
_last_ingestion_payload = []

# Last raw text and semantic metadata (for quality reporting)
_last_raw_text = ""
_last_semantic_metadata = {}

# Dreamer state lock (OBJEKT-76)
DREAMER_STATE_LOCK = threading.Lock()


def _increment_dreamer_node_counter(nodes_added: int):
    """
    Increment the Dreamer daemon node counter (OBJEKT-76).

    This signals to the daemon that new graph nodes have been created,
    allowing threshold-based triggering of Dreamer resolution cycles.
    """
    if nodes_added <= 0:
        return

    import json

    with DREAMER_STATE_LOCK:
        try:
            # Load existing state
            state = {'nodes_since_last_run': 0, 'last_run_timestamp': None}
            if os.path.exists(DREAMER_STATE_FILE):
                with open(DREAMER_STATE_FILE, 'r') as f:
                    state = json.load(f)

            # Increment counter
            state['nodes_since_last_run'] = state.get('nodes_since_last_run', 0) + nodes_added

            # Save state
            os.makedirs(os.path.dirname(DREAMER_STATE_FILE), exist_ok=True)
            with open(DREAMER_STATE_FILE, 'w') as f:
                json.dump(state, f, indent=2, default=str)

            LOGGER.debug(f"Dreamer counter: +{nodes_added} -> {state['nodes_since_last_run']} total")

        except (OSError, json.JSONDecodeError) as e:
            LOGGER.error(f"HARDFAIL: Could not update Dreamer state: {e}")
            raise RuntimeError(f"Failed to update Dreamer counter: {e}") from e


def reset_dreamer_counter():
    """
    Reset the Dreamer daemon node counter to zero.

    Called by rebuild orchestrator to prevent daemon from triggering
    during rebuild (since orchestrator runs Dreamer manually after each day).
    """
    import json

    with DREAMER_STATE_LOCK:
        try:
            state = {'nodes_since_last_run': 0, 'last_run_timestamp': None}
            if os.path.exists(DREAMER_STATE_FILE):
                with open(DREAMER_STATE_FILE, 'r') as f:
                    state = json.load(f)

            state['nodes_since_last_run'] = 0

            os.makedirs(os.path.dirname(DREAMER_STATE_FILE), exist_ok=True)
            with open(DREAMER_STATE_FILE, 'w') as f:
                json.dump(state, f, indent=2, default=str)

            LOGGER.info("Dreamer counter reset to 0")

        except (OSError, json.JSONDecodeError) as e:
            LOGGER.warning(f"Could not reset Dreamer state: {e}")


# Global Schema Validator (Lazy load)
_SCHEMA_VALIDATOR = None


def _get_schema_validator():
    global _SCHEMA_VALIDATOR
    if _SCHEMA_VALIDATOR is None:
        try:
            _SCHEMA_VALIDATOR = SchemaValidator()
        except Exception as e:
            LOGGER.error(f"Could not load SchemaValidator: {e}")
            raise
    return _SCHEMA_VALIDATOR


# Default source type profile (used if source type not in schema)
_DEFAULT_SOURCE_PROFILE = {
    "allow_create": ["Person", "Organization", "Group", "Project", "Event", "Roles"],
    "allow_edges": ["BELONGS_TO", "ATTENDED", "HAS_BUSINESS_RELATION", "HAS_ROLE"],
    "skip_critic": False,
    "prompt_key": None
}


def get_source_profile(source_type: str) -> Dict[str, Any]:
    """
    Load extraction profile for a source type from graph schema.

    Returns profile dict with keys:
        allow_create: list of node types allowed to CREATE
        allow_edges: list of edge types allowed to create
        skip_critic: bool — skip critic LLM call entirely
        prompt_key: str — key in services_prompts.yaml for source context instruction
    """
    schema = _get_schema_validator().schema
    profiles = schema.get('source_type_profiles', {})
    profile = profiles.get(source_type)
    if not profile:
        LOGGER.warning(f"No source_type_profile for '{source_type}', using default (full access)")
        return dict(_DEFAULT_SOURCE_PROFILE)
    return {
        "allow_create": profile.get("allow_create", []),
        "allow_edges": profile.get("allow_edges", []),
        "skip_critic": profile.get("skip_critic", False),
        "prompt_key": profile.get("prompt_key")
    }


def apply_source_profile(ingestion_payload: List[Dict], profile: Dict[str, Any]) -> List[Dict]:
    """
    Filter ingestion_payload based on source type profile.

    - CREATE mentions: keep only if node type is in allow_create
    - CREATE_EDGE mentions: keep only if edge type is in allow_edges
      AND both endpoints survive filtering
    - LINK mentions: always kept (linking to existing nodes is always allowed)

    Returns filtered payload.
    """
    allow_create = set(profile.get("allow_create", []))
    allow_edges = set(profile.get("allow_edges", []))

    # Pass 1: filter nodes
    filtered = []
    removed_uuids = set()
    for m in ingestion_payload:
        action = m.get("action")
        if action == "CREATE":
            node_type = m.get("type")
            if node_type not in allow_create:
                removed_uuids.add(m.get("target_uuid"))
                LOGGER.debug(f"Profile blocked CREATE: {m.get('label')} ({node_type}) — not in allow_create")
                continue
        if action == "CREATE_EDGE":
            continue  # Handle edges in pass 2
        filtered.append(m)

    # Pass 2: filter edges
    for m in ingestion_payload:
        if m.get("action") != "CREATE_EDGE":
            continue
        edge_type = m.get("edge_type")
        if edge_type not in allow_edges:
            LOGGER.debug(f"Profile blocked edge: {edge_type} — not in allow_edges")
            continue
        if m.get("source_uuid") in removed_uuids or m.get("target_uuid") in removed_uuids:
            LOGGER.debug(f"Profile dropped edge: {edge_type} — endpoint removed by profile")
            continue
        filtered.append(m)

    blocked_nodes = len([m for m in ingestion_payload if m.get("action") == "CREATE"]) - \
                    len([m for m in filtered if m.get("action") == "CREATE"])
    blocked_edges = len([m for m in ingestion_payload if m.get("action") == "CREATE_EDGE"]) - \
                    len([m for m in filtered if m.get("action") == "CREATE_EDGE"])
    if blocked_nodes > 0 or blocked_edges > 0:
        LOGGER.info(f"Source profile filtered: {blocked_nodes} CREATE nodes, {blocked_edges} edges removed")

    return filtered


# MCP Server Configuration
VALIDATOR_PARAMS = StdioServerParameters(
    command=sys.executable,
    args=[os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "agents", "validator_mcp.py"))]
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


# generate_semantic_metadata importeras från services.utils.metadata_service
# Den nya versionen inkluderar graf-berikning för rikare context/relations


async def _call_mcp_validator(initial_prompt: str, reference_timestamp: str, anchors: dict = None):
    """Internal async helper to communicate with MCP server."""
    async with stdio_client(VALIDATOR_PARAMS) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()

            result = await session.call_tool(
                "extract_and_validate_doc",
                arguments={
                    "initial_prompt": initial_prompt,
                    "reference_timestamp": reference_timestamp,
                    "anchors": anchors or {}
                }
            )
            return result.content[0].text if result.content else "{}"


def extract_entities_mcp(text: str, source_hint: str = "") -> Dict[str, Any]:
    """
    Extract entities via MCP server.
    Builds the prompt (order), MCP executes and validates.
    """
    LOGGER.info(f"Preparing MCP prompt for {source_hint}...")

    raw_prompt = get_prompt('doc_converter', 'strict_entity_extraction')
    if not raw_prompt:
        return {"nodes": [], "edges": []}

    validator = _get_schema_validator()
    schema = validator.schema

    # --- SCHEMA CONTEXT ---
    all_node_types = set(schema.get('nodes', {}).keys())
    valid_graph_nodes = all_node_types - {'Document', 'Source', 'File'}

    filtered_nodes = {k: v for k, v in schema.get('nodes', {}).items() if k not in {'Document'}}
    node_lines = []
    for k, v in filtered_nodes.items():
        desc = v.get('description', '')
        props = v.get('properties', {})

        prop_info = []
        for prop_name, prop_def in props.items():
            if prop_name in ['id', 'created_at', 'last_synced_at', 'last_seen_at', 'confidence', 'status',
                             'source_system', 'distinguishing_context', 'uuid', 'version']:
                continue

            req_marker = "*" if prop_def.get('required') else ""

            if 'values' in prop_def:
                enums = ", ".join(prop_def['values'])
                prop_info.append(f"{prop_name}{req_marker} [{enums}]")
            else:
                p_type = prop_def.get('type', 'string')
                prop_info.append(f"{prop_name}{req_marker} ({p_type})")

        constraints = []
        if 'name' in props and props['name'].get('description'):
            constraints.append(f"Name rules: {props['name']['description']}")

        info = f"- {k}: {desc}"
        if prop_info:
            info += f" | Properties: {', '.join(prop_info)}"
        if constraints:
            info += f" ({'; '.join(constraints)})"

        node_lines.append(info)
    node_types_str = "\n".join(node_lines)

    filtered_edges = {k: v for k, v in schema.get('edges', {}).items() if k != 'MENTIONS'}
    edge_names = list(filtered_edges.keys())
    whitelist, blacklist = [], []

    for k, v in filtered_edges.items():
        desc = v.get('description', '')
        sources = set(v.get('source_type', []))
        targets = set(v.get('target_type', []))
        line = f"- {k}: [{', '.join(sources)}] -> [{', '.join(targets)}]  // {desc}"

        # Include edge properties in context (skip confidence — always present)
        edge_props = v.get('properties', {})
        prop_info = []
        for prop_name, prop_def in edge_props.items():
            if prop_name == 'confidence':
                continue
            req_marker = " (*)" if prop_def.get('required', False) else ""
            p_type = prop_def.get('type', 'string')
            vals = prop_def.get('values')
            if vals:
                prop_info.append(f"{prop_name}{req_marker} ({p_type}: {vals})")
            else:
                prop_info.append(f"{prop_name}{req_marker} ({p_type})")
        if prop_info:
            line += f" | Properties: {', '.join(prop_info)}"

        whitelist.append(line)

        forbidden_sources = valid_graph_nodes - sources
        forbidden_targets = valid_graph_nodes - targets
        if forbidden_sources:
            blacklist.append(f"- {k}: NEVER starts from [{', '.join(forbidden_sources)}]")
        if forbidden_targets:
            blacklist.append(f"- {k}: NEVER points to [{', '.join(forbidden_targets)}]")

    edge_types_str = (
        f"ALLOWED RELATION NAMES:\n[{', '.join(edge_names)}]\n\n"
        f"ALLOWED CONNECTIONS (WHITELIST):\n" + "\n".join(whitelist) + "\n\n"
        f"FORBIDDEN CONNECTIONS (BLACKLIST - AUTO-GENERATED):\n" + "\n".join(blacklist)
    )

    # Load source context instruction from profile → prompts
    source_context_instruction = ""
    profile = get_source_profile(source_hint) if source_hint else {}
    prompt_key = profile.get("prompt_key")
    if prompt_key:
        source_context_instruction = get_prompt('doc_converter', prompt_key) or ""
    if not source_context_instruction:
        # Fallback for unknown source types
        if "Slack" in source_hint:
            source_context_instruction = "CONTEXT: This is a Slack chat. Format is often 'Name: Message'. Treat senders as strong Person candidates."
        elif "Mail" in source_hint:
            source_context_instruction = "CONTEXT: This is an email. Sender (From) and recipients (To) are important Person nodes."

    final_prompt = raw_prompt.format(
        text_chunk=text[:25000],
        node_types_context=node_types_str,
        edge_types_context=edge_types_str,
        known_entities_context=source_context_instruction
    )

    try:
        reference_timestamp = datetime.datetime.now().isoformat()
        anchors = {}
        response_json = asyncio.run(_call_mcp_validator(final_prompt, reference_timestamp, anchors))
        return parse_llm_json(response_json)
    except Exception as e:
        LOGGER.error(f"HARDFAIL: MCP Extraction failed: {e}")
        raise RuntimeError(f"MCP Entity Extraction failed: {e}") from e


def _call_critic_llm(entities_for_review: List[Dict]) -> Dict:
    """
    Skicka entiteter till critic-LLM för kvalitetsfiltrering.

    Args:
        entities_for_review: [{"name": "...", "type": "...", "confidence": 0.x}]

    Returns:
        {"approved": [{"name", "type", "reason"}], "rejected": [{"name", "type", "reason"}]}
        Vid fel: {"approved": entities_for_review, "rejected": []}
    """
    import json

    if not entities_for_review:
        return {"approved": [], "rejected": []}

    prompt_template = get_prompt('doc_converter', 'entity_critic')
    if not prompt_template:
        LOGGER.warning("entity_critic prompt saknas - hoppar över Critic-steget")
        return {"approved": entities_for_review, "rejected": []}

    # Bygg typdefinitioner från schemat (dynamisk injektion)
    validator = _get_schema_validator()
    schema = validator.schema
    type_definitions = []
    for node_type, node_def in schema.get('nodes', {}).items():
        if node_type == 'Document':
            continue  # Intern typ, inte för extraktion
        desc = node_def.get('description', '')
        type_definitions.append(f"- {node_type}: {desc}")
    type_definitions_str = "\n".join(type_definitions)

    prompt = prompt_template.format(
        entities_json=json.dumps(entities_for_review, indent=2, ensure_ascii=False),
        type_definitions=type_definitions_str
    )

    llm = _get_llm_service()
    # Anthropic Haiku för snabb validering (OBJEKT-85)
    response = llm.generate(prompt, provider='anthropic', model=llm.models['lite'])

    if not response.success:
        LOGGER.error(f"Critic LLM failed: {response.error}")
        return {"approved": entities_for_review, "rejected": []}

    result = parse_llm_json(response.text)
    return result


def critic_filter_entities(nodes: List[Dict]) -> List[Dict]:
    """
    LLM-baserad filtrering av extraherade entiteter (pre-resolve).
    Returnerar endast godkända noder.

    Fallback: Returnerar alla noder om prompt saknas eller LLM misslyckas.
    """
    if not nodes:
        return [], [], []

    entities_for_review = [
        {"name": n.get("name"), "type": n.get("type"), "confidence": n.get("confidence", 0.5)}
        for n in nodes if n.get("name") and n.get("type")
    ]

    if not entities_for_review:
        return [], [], []

    result = _call_critic_llm(entities_for_review)
    approved_names = {e["name"] for e in result.get("approved", [])}

    # Logga statistik
    rejected = result.get("rejected", [])
    if rejected:
        LOGGER.info(f"Critic: {len(approved_names)} godkända, {len(rejected)} avvisade")
        for rej in rejected[:5]:  # Logga max 5 avvisade
            LOGGER.debug(f"  Avvisad: {rej.get('name')} ({rej.get('type')}) - {rej.get('reason', 'N/A')}")

    # Filtrera original-noder baserat på godkända namn
    approved_nodes = [n for n in nodes if n.get("name") in approved_names]
    return approved_nodes, result.get("approved", []), result.get("rejected", [])


def critic_filter_resolved(ingestion_payload: List[Dict]) -> List[Dict]:
    """
    LLM-baserad kvalitetsfiltrering av resolvade entiteter.

    LINK-entiteter auto-godkänns (grafen bekräftar existens).
    CREATE-entiteter skickas till critic-LLM för filtrering.
    Edges som refererar avvisade CREATE-noder tas bort.

    Populerar _last_critic_approved och _last_critic_rejected globals.

    Returns:
        Filtrerad ingestion_payload.
    """
    global _last_critic_approved, _last_critic_rejected

    if not ingestion_payload:
        _last_critic_approved = []
        _last_critic_rejected = []
        return []

    # Separera per action-typ
    link_mentions = [m for m in ingestion_payload if m.get("action") == "LINK"]
    create_mentions = [m for m in ingestion_payload if m.get("action") == "CREATE"]
    edge_mentions = [m for m in ingestion_payload if m.get("action") == "CREATE_EDGE"]

    # Auto-godkänn alla LINK-mentions
    link_approved = [
        {"name": m["label"], "type": m["type"], "reason": "LINK (auto-approved)"}
        for m in link_mentions
    ]

    if not create_mentions:
        # Inget att granska — alla är LINK
        _last_critic_approved = link_approved
        _last_critic_rejected = []
        LOGGER.info(f"Critic: {len(link_mentions)} LINK (auto), 0 CREATE")
        return link_mentions + edge_mentions

    # Skicka CREATE-mentions till critic-LLM
    entities_for_review = [
        {"name": m["label"], "type": m["type"], "confidence": m.get("confidence", 0.5)}
        for m in create_mentions
    ]

    result = _call_critic_llm(entities_for_review)
    approved_names = {e["name"] for e in result.get("approved", [])}

    # Filtrera CREATE-mentions
    approved_creates = [m for m in create_mentions if m["label"] in approved_names]
    rejected_creates = [m for m in create_mentions if m["label"] not in approved_names]

    # Samla UUID:n från avvisade noder för edge-cleanup
    rejected_uuids = {m["target_uuid"] for m in rejected_creates}

    # Filtrera bort edges som refererar avvisade noder
    if rejected_uuids:
        filtered_edges = [
            e for e in edge_mentions
            if e.get("source_uuid") not in rejected_uuids
            and e.get("target_uuid") not in rejected_uuids
        ]
        edges_removed = len(edge_mentions) - len(filtered_edges)
        if edges_removed > 0:
            LOGGER.info(f"Critic: Removed {edges_removed} edges referencing rejected nodes")
    else:
        filtered_edges = edge_mentions

    # Populera globals för testkompatibilitet (test_ingestion_cycle.py)
    _last_critic_approved = link_approved + result.get("approved", [])
    _last_critic_rejected = result.get("rejected", [])

    # Logga statistik
    LOGGER.info(
        f"Critic: {len(link_mentions)} LINK (auto), "
        f"{len(approved_creates)} CREATE approved, "
        f"{len(rejected_creates)} CREATE rejected"
    )
    for rej in result.get("rejected", [])[:5]:
        LOGGER.debug(f"  Avvisad: {rej.get('name')} ({rej.get('type')}) - {rej.get('reason', 'N/A')}")

    return link_mentions + approved_creates + filtered_edges


# --- Post-processing: deterministic edge properties ---

# Calendar event block: ## HH:MM-HH:MM: Title
_CALENDAR_EVENT_RE = re.compile(r'^## (\d{2}:\d{2})-(\d{2}:\d{2}): (.+)$', re.MULTILINE)

# Calendar all-day event: ## Heldag: Title (no ATTENDED)
_CALENDAR_ALLDAY_RE = re.compile(r'^## Heldag: ', re.MULTILINE)


def _parse_calendar_events(raw_text: str) -> List[Dict]:
    """
    Parse calendar text into structured event blocks.

    Returns list of:
    {
        "title": str,
        "start": "HH:MM",
        "end": "HH:MM",
        "duration_minutes": int,
        "accepted_attendees": [str, ...]  # lowercase email-prefix names
    }
    """
    events = []

    # Split text into sections by ## headers
    sections = re.split(r'^(?=## )', raw_text, flags=re.MULTILINE)

    for section in sections:
        match = _CALENDAR_EVENT_RE.match(section)
        if not match:
            continue

        start_str, end_str, title = match.group(1), match.group(2), match.group(3).strip()

        # Skip all-day sentinel (00:00-00:00)
        if start_str == "00:00" and end_str == "00:00":
            continue

        # Calculate duration
        try:
            start_dt = datetime.datetime.strptime(start_str, "%H:%M")
            end_dt = datetime.datetime.strptime(end_str, "%H:%M")
            delta = (end_dt - start_dt).total_seconds() / 60
            if delta < 0:
                delta += 1440  # Midnight crossing
            duration_minutes = int(delta)
        except ValueError as e:
            LOGGER.warning(f"Post-process: Could not parse calendar time '{start_str}-{end_str}': {e}")
            duration_minutes = 0

        # Parse accepted attendees from **Deltagare:** line
        accepted = []
        att_match = re.search(r'\*\*Deltagare:\*\*\s*(.+?)(?:\n\n|\n\*\*|\n##|\Z)', section, re.DOTALL)
        if att_match:
            att_text = att_match.group(1)
            # Each attendee: "name (status)" separated by commas
            for part in att_text.split(','):
                part = part.strip()
                if '(accepterat)' in part:
                    name = part.replace('(accepterat)', '').strip()
                    if name:
                        accepted.append(name.lower())

        events.append({
            "title": title,
            "start": start_str,
            "end": end_str,
            "duration_minutes": duration_minutes,
            "accepted_attendees": accepted,
        })

    return events


def _normalize_name_for_match(name: str) -> str:
    """Normalize a name for fuzzy calendar matching.

    Converts "Joakim Ekman" -> "joakim.ekman" style and
    "joakim.ekman" stays as-is, for matching against calendar
    attendee names (email-prefix format).
    """
    name = name.lower().strip()
    # If already dotted email-prefix format
    if '.' in name and ' ' not in name:
        return name
    # Convert "Förnamn Efternamn" -> "förnamn.efternamn"
    return name.replace(' ', '.')


def _build_calendar_attended(ingestion_payload: List[Dict], raw_text: str) -> None:
    """
    Build ATTENDED edges deterministically for Calendar Events.

    1. Parse calendar event blocks from raw_text
    2. Match accepted attendees to Person nodes in payload
    3. Match event titles to Event nodes in payload
    4. Remove any LLM-generated ATTENDED edges
    5. Create deterministic ATTENDED edges with duration_minutes
    """
    cal_events = _parse_calendar_events(raw_text)
    if not cal_events:
        return

    # Build lookups from ingestion_payload
    # Person: normalized_name -> {uuid, label, type}
    person_lookup = {}
    for m in ingestion_payload:
        if m.get("action") in ("CREATE", "LINK") and m.get("type") == "Person":
            norm = _normalize_name_for_match(m["label"])
            person_lookup[norm] = m

    # Event: title (lowered) -> {uuid, label, type}
    event_lookup = {}
    for m in ingestion_payload:
        if m.get("action") in ("CREATE", "LINK") and m.get("type") == "Event":
            event_lookup[m["label"].lower().strip()] = m

    # Remove all LLM-generated ATTENDED edges (we replace them)
    original_len = len(ingestion_payload)
    ingestion_payload[:] = [
        m for m in ingestion_payload
        if not (m.get("action") == "CREATE_EDGE" and m.get("edge_type") == "ATTENDED")
    ]
    removed = original_len - len(ingestion_payload)
    if removed > 0:
        LOGGER.info(f"Post-process: Removed {removed} LLM-generated ATTENDED edges (replaced by deterministic)")

    # Create deterministic ATTENDED edges
    created = 0
    for cal_event in cal_events:
        # Find matching Event node
        event_title_lower = cal_event["title"].lower().strip()
        event_mention = event_lookup.get(event_title_lower)
        if not event_mention:
            # Try partial match (event node name might be shortened)
            for key, em in event_lookup.items():
                if key in event_title_lower or event_title_lower in key:
                    event_mention = em
                    break

        if not event_mention:
            LOGGER.debug(f"Post-process: No Event node for '{cal_event['title']}', skipping ATTENDED")
            continue

        event_uuid = event_mention["target_uuid"]

        for attendee_name in cal_event["accepted_attendees"]:
            # Find matching Person node
            person_mention = person_lookup.get(attendee_name)
            if not person_mention:
                # Try matching parts (e.g. "cenk.bisgen" vs "cenk bisgen" in payload)
                for pkey, pm in person_lookup.items():
                    if attendee_name.replace('.', ' ') == pkey.replace('.', ' '):
                        person_mention = pm
                        break

            if not person_mention:
                LOGGER.debug(f"Post-process: No Person node for attendee '{attendee_name}', skipping ATTENDED")
                continue

            person_uuid = person_mention["target_uuid"]
            person_label = person_mention.get("canonical_name", person_mention["label"])
            event_label = event_mention.get("canonical_name", event_mention["label"])

            edge_props = {}
            if cal_event["duration_minutes"] > 0:
                edge_props["duration_minutes"] = cal_event["duration_minutes"]

            attended_edge = {
                "action": "CREATE_EDGE",
                "source_uuid": person_uuid,
                "target_uuid": event_uuid,
                "edge_type": "ATTENDED",
                "confidence": 1.0,
                "source_text": f"{person_label} -> {event_label}",
                "source_name": person_label,
                "source_type": "Person",
                "target_name": event_label,
                "target_type": "Event",
            }
            if edge_props:
                attended_edge["edge_properties"] = edge_props

            ingestion_payload.append(attended_edge)
            created += 1

    if created > 0:
        LOGGER.info(f"Post-process: Created {created} deterministic ATTENDED edges with duration_minutes")


def _validate_required_edge_properties(ingestion_payload: List[Dict]) -> None:
    """
    Validate REQUIRED edge properties. HARDFAIL (log ERROR) if missing.
    Does NOT set defaults — the edge is written as-is and the gap is visible in tests.
    """
    for m in ingestion_payload:
        if m.get("action") != "CREATE_EDGE":
            continue

        edge_type = m.get("edge_type")
        edge_props = m.get("edge_properties", {})

        if edge_type == "HAS_BUSINESS_RELATION" and not edge_props.get("relation_type"):
            source_name = m.get("source_name", "?")
            target_name = m.get("target_name", "?")
            LOGGER.error(
                f"Post-process: HAS_BUSINESS_RELATION missing REQUIRED relation_type "
                f"({source_name} -> {target_name}). LLM failed to extract — no fallback applied."
            )


def post_process_edges(ingestion_payload: List[Dict], source_type: str, raw_text: str) -> List[Dict]:
    """
    Post-process edges with deterministic property computation.

    For Calendar Events: builds ATTENDED edges deterministically from
    parsed attendee data (only accepted attendees get ATTENDED).

    For all source types: validates REQUIRED edge properties and logs
    errors if missing (no fallback — HARDFAIL principle).
    """
    if source_type == "Calendar Event":
        _build_calendar_attended(ingestion_payload, raw_text)

    _validate_required_edge_properties(ingestion_payload)

    return ingestion_payload


def resolve_entities(nodes: List[Dict], edges: List[Dict], source_type: str, filename: str) -> List[Dict]:
    """
    Resolve extracted entities against existing graph.
    Returns list of mentions (actions to perform).

    Includes canonical_name for each entity:
    - LINK: canonical_name from graph (the authoritative name)
    - CREATE: canonical_name = input name
    """
    # Load schema to identify type-specific properties
    validator = _get_schema_validator()
    schema = validator.schema
    base_prop_names = set(schema.get('base_properties', {}).get('properties', {}).keys())

    mentions = []
    name_to_uuid = {}
    name_to_canonical = {}  # Maps input name -> canonical name
    name_to_type = {}  # Maps input name -> node type

    # Check if database exists before opening in read-only mode
    # After hard reset, graph is empty - all entities will be CREATE
    if not os.path.exists(GRAPH_DB_PATH):
        LOGGER.info(f"Graph DB not found at {GRAPH_DB_PATH}, all entities will be CREATE")
        graph = None
    else:
        graph = GraphService(GRAPH_DB_PATH, read_only=True)
    seen_candidates = set()

    for node in nodes:
        name = node.get('name')
        type_str = node.get('type')
        confidence = node.get('confidence', 0.5)
        # Extract text from node_context (validator_mcp normalizes to [{text, origin}])
        nc = node.get('node_context', '')
        if isinstance(nc, list) and nc and isinstance(nc[0], dict):
            node_context_text = nc[0].get('text', '')
        else:
            node_context_text = normalize_value(nc, 'string') or ''

        if not name or not type_str:
            continue

        # Noise filter based on confidence
        if confidence < 0.3:
            continue

        key = f"{name}|{type_str}"
        if key in seen_candidates:
            continue
        seen_candidates.add(key)

        # Boost confidence for trusted sources
        if source_type in ["Slack Log", "Email Thread"]:
            confidence = max(confidence, 0.8)

        # Entity resolution: LINK if exists, CREATE if new
        existing_uuid = None
        if graph is not None:
            existing_uuid = graph.find_node_by_name(type_str, name, fuzzy=True)

        if existing_uuid:
            action = "LINK"
            target_uuid = existing_uuid
            # Hämta kanoniskt namn från grafen
            existing_node = graph.get_node(existing_uuid)
            canonical_name = existing_node.get("name", name) if existing_node else name
        else:
            action = "CREATE"
            target_uuid = str(uuid.uuid4())
            canonical_name = name  # Vid CREATE = input-namn

        name_to_uuid[name] = target_uuid
        name_to_canonical[name] = canonical_name
        name_to_type[name] = type_str

        # Extract type-specific properties from LLM output
        type_specific_props = {}
        node_type_def = schema.get('nodes', {}).get(type_str, {})
        allowed_type_props = set(node_type_def.get('properties', {}).keys()) - base_prop_names
        for prop_name in allowed_type_props:
            if prop_name in node and prop_name != 'name':
                type_specific_props[prop_name] = node[prop_name]
        mention = {
            "action": action,
            "target_uuid": target_uuid,
            "type": type_str,
            "label": name,
            "canonical_name": canonical_name,
            "node_context_text": node_context_text,
            "confidence": confidence
        }
        if type_specific_props:
            mention["properties"] = type_specific_props

        mentions.append(mention)

    if graph is not None:
        graph.close()

    # Handle relations - use canonical names for source_text
    for edge in edges:
        source_name = edge.get('source')
        target_name = edge.get('target')
        rel_type = edge.get('type')
        rel_conf = edge.get('confidence', 0.5)

        if source_name in name_to_uuid and target_name in name_to_uuid:
            source_uuid = name_to_uuid[source_name]
            target_uuid = name_to_uuid[target_name]
            # Använd canonical names i source_text
            source_canonical = name_to_canonical.get(source_name, source_name)
            target_canonical = name_to_canonical.get(target_name, target_name)
            # Hämta typer för terminal-output
            source_type = name_to_type.get(source_name, "?")
            target_type = name_to_type.get(target_name, "?")

            # Extract type-specific edge properties from LLM output
            edge_props = {}
            edge_type_def = schema.get('edges', {}).get(rel_type, {})
            allowed_edge_props = set(edge_type_def.get('properties', {}).keys()) - {'confidence'}
            for prop_name in allowed_edge_props:
                if prop_name in edge:
                    edge_props[prop_name] = edge[prop_name]

            edge_mention = {
                "action": "CREATE_EDGE",
                "source_uuid": source_uuid,
                "target_uuid": target_uuid,
                "edge_type": rel_type,
                "confidence": rel_conf,
                "source_text": f"{source_canonical} -> {target_canonical}",
                "source_name": source_canonical,
                "source_type": source_type,
                "target_name": target_canonical,
                "target_type": target_type
            }
            if edge_props:
                edge_mention["edge_properties"] = edge_props

            mentions.append(edge_mention)
        else:
            missing = [x for x in [source_name, target_name] if x not in name_to_uuid]
            LOGGER.debug(f"Dropped edge ({rel_type}): endpoint(s) {missing} not resolved")

    return mentions


def write_lake(unit_id: str, filename: str, raw_text: str, source_type: str,
               semantic_metadata: Dict, ingestion_payload: List) -> str:
    """Write document to Lake with frontmatter."""
    base_name = os.path.splitext(filename)[0]
    lake_file = os.path.join(LAKE_STORE, f"{base_name}.md")

    # HARDFAIL if semantic metadata is incomplete — Lake is the commit receipt
    ai_model = semantic_metadata.get("ai_model")
    if not ai_model:
        raise RuntimeError(f"HARDFAIL: write_lake({filename}) — semantic_metadata missing 'ai_model'. metadata_service likely failed.")

    timestamp_content = extract_content_date(raw_text, filename)
    default_access_level = CONFIG.get('security', {}).get('default_access_level', 5)

    frontmatter = {
        "unit_id": unit_id,
        "source_ref": lake_file,
        "original_filename": filename,
        "timestamp_ingestion": datetime.datetime.now().isoformat(),
        "timestamp_content": timestamp_content,
        "timestamp_updated": None,
        "source_type": source_type,
        "access_level": default_access_level,
        "owner": get_owner_name(),
        "context_summary": semantic_metadata.get("context_summary", ""),
        "relations_summary": semantic_metadata.get("relations_summary", ""),
        "document_keywords": semantic_metadata.get("document_keywords", []),
        "ai_model": ai_model,
    }

    fm_str = yaml.dump(frontmatter, sort_keys=False, allow_unicode=True)
    with open(lake_file, 'w', encoding='utf-8') as f:
        f.write(f"---\n{fm_str}---\n\n# {filename}\n\n{raw_text}")

    LOGGER.info(f"Lake: {filename} ({source_type}) -> {len(ingestion_payload)} mentions")
    return lake_file


def write_graph(unit_id: str, filename: str, ingestion_payload: List, vector_service=None) -> tuple:
    """Write entities and edges to graph, and index nodes to vector."""
    graph = GraphService(GRAPH_DB_PATH)
    if vector_service is None:
        from services.utils.vector_service import get_vector_service
        vector_service = get_vector_service("knowledge_base")

    # Skapa Document-nod för källdokumentet (krävs för MENTIONS-kanter)
    graph.upsert_node(
        id=unit_id,
        type="Document",
        properties={
            "name": filename,
            "status": "ACTIVE",
            "source_system": "IngestionEngine"
        }
    )

    nodes_written = 0
    edges_written = 0

    for entity in ingestion_payload:
        action = entity.get("action")

        if action in ["CREATE", "LINK"]:
            target_uuid = entity.get("target_uuid")
            node_type = entity.get("type")
            label = entity.get("label")
            confidence = entity.get("confidence", 0.5)
            node_context_text = entity.get("node_context_text", "")

            if not target_uuid or not node_type:
                continue
            if not label:
                LOGGER.error(f"HARDFAIL: Entity missing label: {entity}")
                continue

            node_context_entry = {
                "text": node_context_text or f"Mentioned in {filename}",
                "origin": unit_id
            }

            props = {
                "name": label,
                "status": "PROVISIONAL",
                "confidence": confidence,
                "node_context": [node_context_entry],
                "source_system": "IngestionEngine"
            }

            # Add type-specific properties from LLM extraction
            type_props = entity.get("properties", {})
            if type_props:
                props.update(type_props)

            graph.upsert_node(
                id=target_uuid,
                type=node_type,
                properties=props
            )

            # Index node to vector immediately (not wait for Dreamer)
            vector_service.upsert_node({
                'id': target_uuid,
                'type': node_type,
                'properties': props,
                'aliases': []
            })
            nodes_written += 1

            graph.upsert_edge(
                source=unit_id,
                target=target_uuid,
                edge_type="MENTIONS",
                properties={"confidence": confidence}
            )
            edges_written += 1

        elif action == "CREATE_EDGE":
            source_uuid = entity.get("source_uuid")
            target_uuid = entity.get("target_uuid")
            edge_type = entity.get("edge_type")
            edge_conf = entity.get("confidence", 0.5)

            if source_uuid and target_uuid and edge_type:
                edge_props = {"confidence": edge_conf}
                extra_edge_props = entity.get("edge_properties", {})
                if extra_edge_props:
                    edge_props.update(extra_edge_props)

                # Quality gate: drop edges with no semantic properties (only confidence)
                # ATTENDED undantagen — deltagarkopplingen är värdefull utan duration_minutes
                semantic_keys = set(edge_props.keys()) - {"confidence"}
                if not semantic_keys and edge_type != "ATTENDED":
                    source_name = entity.get("source_name", "?")
                    target_name = entity.get("target_name", "?")
                    LOGGER.warning(
                        f"Edge dropped (no semantic properties): "
                        f"{source_name} -[{edge_type}]-> {target_name}"
                    )
                    continue

                graph.upsert_edge(
                    source=source_uuid,
                    target=target_uuid,
                    edge_type=edge_type,
                    properties=edge_props
                )
                edges_written += 1

                # Terminal output: visa sparad relation
                entity_detail(
                    source_name=entity.get("source_name", "?"),
                    source_type=entity.get("source_type", "?"),
                    edge_type=edge_type,
                    target_name=entity.get("target_name", "?"),
                    target_type=entity.get("target_type", "?")
                )

    graph.close()
    LOGGER.info(f"Graph: {filename} -> {nodes_written} nodes, {edges_written} edges")
    return nodes_written, edges_written


def write_vector(unit_id: str, filename: str, raw_text: str, source_type: str,
                 semantic_metadata: Dict, timestamp_ingestion: str, vector_service=None):
    """
    Write document to vector index.

    For transcripts with Rich Transcriber Del-struktur:
    - Indexes each part as a separate chunk for precise semantic search
    - Chunk IDs: {unit_id}__part_{N}

    For other documents:
    - Indexes as single document (existing behavior)
    """
    if vector_service is None:
        from services.utils.vector_service import get_vector_service
        vector_service = get_vector_service("knowledge_base")

    # Check if transcript with parts structure
    if source_type == "Transcript" and has_transcript_parts(raw_text):
        parts = extract_transcript_parts(raw_text)
        if parts:
            LOGGER.info(f"Vector: {filename} -> {len(parts)} chunks (transcript parts)")

            for part in parts:
                chunk_id = f"{unit_id}__part_{part.part_number}"
                chunk_text = build_chunk_text(part)

                vector_service.upsert(
                    id=chunk_id,
                    text=chunk_text,
                    metadata={
                        "timestamp": timestamp_ingestion,
                        "filename": filename,
                        "source_type": source_type,
                        "parent_id": unit_id,
                        "part_number": part.part_number,
                        "title": part.title,
                        "time_start": part.time_start or "",
                        "time_end": part.time_end or ""
                    }
                )
            return

    # Try document chunking for long non-transcript docs (OBJEKT-100)
    chunking_cfg = CONFIG.get('processing', {}).get('chunking', {})
    chunk_size = chunking_cfg.get('chunk_size', 2000)
    chunk_overlap = chunking_cfg.get('chunk_overlap', 200)
    chunk_threshold = chunking_cfg.get('chunk_threshold', 4000)
    overview_content_chars = chunking_cfg.get('overview_content_chars', 500)

    ctx_summary = semantic_metadata.get("context_summary", "")
    rel_summary = semantic_metadata.get("relations_summary", "")

    parts = chunk_document(raw_text, source_type, chunk_size, chunk_overlap, chunk_threshold)

    if parts:
        # Part 0: Overview chunk
        overview_text = build_overview_chunk_text(
            filename, ctx_summary, rel_summary, raw_text, overview_content_chars
        )
        vector_service.upsert(
            id=f"{unit_id}__part_0",
            text=overview_text,
            metadata={
                "timestamp": timestamp_ingestion,
                "filename": filename,
                "source_type": source_type,
                "parent_id": unit_id,
                "part_number": 0,
                "title": "Overview",
            }
        )

        # Parts 1..N: Content chunks
        for part in parts:
            chunk_id = f"{unit_id}__part_{part.part_number}"
            chunk_text = build_chunk_text(part)
            vector_service.upsert(
                id=chunk_id,
                text=chunk_text,
                metadata={
                    "timestamp": timestamp_ingestion,
                    "filename": filename,
                    "source_type": source_type,
                    "parent_id": unit_id,
                    "part_number": part.part_number,
                    "title": part.title or "",
                    "time_start": part.time_start or "",
                    "time_end": part.time_end or "",
                }
            )

        total = len(parts) + 1
        LOGGER.info(f"Vector: {filename} -> {total} chunks ({source_type} chunking)")
        return

    # Standard document indexing (short docs or fallback)
    vector_text = f"FILENAME: {filename}\nSUMMARY: {ctx_summary}\nRELATIONS: {rel_summary}\n\nCONTENT:\n{raw_text[:8000]}"

    vector_service.upsert(
        id=unit_id,
        text=vector_text,
        metadata={
            "timestamp": timestamp_ingestion,
            "filename": filename,
            "source_type": source_type
        }
    )
    LOGGER.info(f"Vector: {filename} -> ChromaDB")


def _clean_before_reingest(unit_id: str, filename: str, lake_file: str, vector_service=None):
    """
    Clean stale data from Graph + Vector before re-ingestion.

    Removes old node_context entries (origin == unit_id), deletes orphan entities,
    removes Document node + MENTIONS edges, and clears vector entries.

    Does NOT touch Assets (they're the source of truth for re-ingest).
    Lake file is deleted so write_lake() can recreate it as the final "commit".
    """
    graph = GraphService(GRAPH_DB_PATH)
    if vector_service is None:
        from services.utils.vector_service import get_vector_service
        vs = get_vector_service("knowledge_base")
    else:
        vs = vector_service

    try:
        # 1. Clean entity node_context entries from this document
        edges = graph.get_edges_from(unit_id)
        mentions = [e for e in edges if e.get("type") == "MENTIONS"]

        entities_cleaned = 0
        entities_deleted = 0

        for edge in mentions:
            entity_id = edge["target"]
            entity = graph.get_node(entity_id)
            if not entity:
                continue

            props = entity.get("properties", {})
            node_context = props.get("node_context", [])

            # Remove entries originating from this document
            new_context = [
                nc for nc in node_context
                if not (isinstance(nc, dict) and nc.get("origin") == unit_id)
            ]

            if not new_context:
                # No context left — check if entity is orphan
                other_incoming = [e for e in graph.get_edges_to(entity_id) if e["source"] != unit_id]
                other_outgoing = graph.get_edges_from(entity_id)

                if not other_incoming and not other_outgoing:
                    graph.delete_node(entity_id)
                    vs.delete(entity_id)
                    entities_deleted += 1
                    continue

            props["node_context"] = new_context
            graph.update_node_properties(entity_id, props)

            vs.upsert_node({
                'id': entity_id,
                'type': entity.get('type'),
                'properties': props,
                'aliases': entity.get('aliases', [])
            })
            entities_cleaned += 1

        # 2. Delete Document node + MENTIONS edges
        graph.delete_node(unit_id)

        # 3. Delete vector entries (document + transcript chunks)
        vs.delete(unit_id)
        vs.delete_by_parent(unit_id)

    finally:
        graph.close()

    # 4. Delete Lake file (will be recreated as final "commit" step)
    try:
        os.remove(lake_file)
    except OSError as e:
        LOGGER.warning(f"Could not remove Lake file during re-ingest: {e}")

    LOGGER.info(
        f"Re-ingest cleanup: {filename} -> "
        f"{entities_cleaned} entities cleaned, {entities_deleted} orphans deleted"
    )


def _needs_reingest(filepath: str, lake_file: str) -> bool:
    """
    Check if Asset file is newer than Lake file (content was updated).

    Returns True if re-ingestion is needed.
    """
    try:
        asset_mtime = os.path.getmtime(filepath)
        lake_mtime = os.path.getmtime(lake_file)
        return asset_mtime > lake_mtime
    except OSError as e:
        LOGGER.warning(f"Could not compare timestamps for {filepath}: {e}")
        return False


def process_document(filepath: str, filename: str, _lock_held: bool = False):
    """
    Main document processing function.
    Orchestrates the full ingestion pipeline.

    Args:
        filepath: Full path to source file
        filename: Filename (used for UUID extraction)
        _lock_held: If True, caller already holds resource locks (e.g., rebuild).
                    If False, this function acquires locks per document.
    """
    with PROCESS_LOCK:
        if filename in PROCESSED_FILES:
            return
        PROCESSED_FILES.add(filename)

    match = UUID_SUFFIX_PATTERN.search(filename)
    if not match:
        return
    unit_id = match.group(1)
    base_name = os.path.splitext(filename)[0]
    lake_file = os.path.join(LAKE_STORE, f"{base_name}.md")

    is_reingest = False
    if os.path.exists(lake_file):
        if _needs_reingest(filepath, lake_file):
            is_reingest = True
            LOGGER.info(f"Re-ingest triggered: {filename} (Asset newer than Lake)")
        else:
            LOGGER.debug(f"Skippar {filename} - redan i Lake")
            return  # Idempotent

    LOGGER.info(f"{'Re-processing' if is_reingest else 'Processing'}: {filename}")
    terminal_status("ingestion", filename, "processing")

    def _do_process(vector_service=None):
        """Inner processing logic."""
        # Reset token counter for per-document tracking
        if _LLM_SERVICE:
            _LLM_SERVICE.reset_token_usage()

        # 0. Clean stale data before re-ingestion
        if is_reingest:
            terminal_status("ingestion", filename, "re-ingest cleanup")
            _clean_before_reingest(unit_id, filename, lake_file, vector_service=vector_service)

        # 1. Extract text (via text_extractor)
        raw_text = extract_text(filepath)

        if not raw_text or len(raw_text) < 10:
            LOGGER.debug(f"File {filename} appears incomplete ({len(raw_text) if raw_text else 0} chars). Waiting for on_modified.")
            with PROCESS_LOCK:
                PROCESSED_FILES.discard(filename)
            return

        # 2. Determine source type
        source_type = "Document"
        if "slack" in filepath.lower():
            source_type = "Slack Log"
        elif "mail" in filepath.lower():
            source_type = "Email Thread"
        elif "calendar" in filepath.lower():
            source_type = "Calendar Event"
        elif "transcripts" in filepath.lower():
            source_type = "Transcript"

        # 2b. Load source type extraction profile from schema
        profile = get_source_profile(source_type)
        LOGGER.info(f"Source profile: {source_type} — create={profile['allow_create']}, edges={profile['allow_edges']}, skip_critic={profile['skip_critic']}")

        # 3. Extract entities via MCP
        entity_data = extract_entities_mcp(raw_text, source_hint=source_type)
        nodes = entity_data.get('nodes', [])
        edges = entity_data.get('edges', [])

        # 4. Resolve ALLA noder mot graf FÖRST (edges behöver alla för UUID-lookup)
        ingestion_payload = resolve_entities(nodes, edges, source_type, filename)

        # 4b. Apply source type profile (filter CREATE/edges based on profile)
        ingestion_payload = apply_source_profile(ingestion_payload, profile)

        # 5. Critic EFTER resolve (bara CREATE, LINK auto-godkänd)
        if profile.get("skip_critic"):
            # Profile says skip critic — all surviving entries are accepted
            global _last_critic_approved, _last_critic_rejected
            link_count = len([m for m in ingestion_payload if m.get("action") == "LINK"])
            create_count = len([m for m in ingestion_payload if m.get("action") == "CREATE"])
            _last_critic_approved = [
                {"name": m["label"], "type": m["type"], "reason": f"{m.get('action')} (critic skipped by profile)"}
                for m in ingestion_payload if m.get("action") in ("LINK", "CREATE")
            ]
            _last_critic_rejected = []
            LOGGER.info(f"Critic: skipped by profile ({link_count} LINK, {create_count} CREATE)")
        else:
            ingestion_payload = critic_filter_resolved(ingestion_payload)

        # 5b. Post-processing: deterministiska edge properties
        ingestion_payload = post_process_edges(ingestion_payload, source_type, raw_text)

        # Expose payload for test write-through verification
        global _last_ingestion_payload, _last_raw_text
        _last_ingestion_payload = copy.deepcopy(ingestion_payload)
        _last_raw_text = raw_text

        # 6. Generate semantic metadata MED graf-berikning (via metadata_service)
        semantic_metadata = generate_semantic_metadata(
            text=raw_text,
            resolved_entities=ingestion_payload,
            current_meta=None,  # Nygenering
            filename=filename
        )

        global _last_semantic_metadata
        _last_semantic_metadata = copy.deepcopy(semantic_metadata) if semantic_metadata else {}

        # 7. Write to Graph
        nodes_written, edges_written = write_graph(unit_id, filename, ingestion_payload, vector_service=vector_service)

        # 7b. Update Dreamer daemon counter (OBJEKT-76)
        _increment_dreamer_node_counter(nodes_written)

        # 8. Write to Vector
        timestamp_ingestion = datetime.datetime.now().isoformat()
        write_vector(unit_id, filename, raw_text, source_type, semantic_metadata, timestamp_ingestion, vector_service=vector_service)

        # 9. Write to Lake (SIST - fungerar som "commit" att allt lyckades)
        write_lake(unit_id, filename, raw_text, source_type, semantic_metadata, ingestion_payload)

        # Done - log token usage and terminal
        action = "Re-ingested" if is_reingest else "Completed"
        token_usage = _get_llm_service().get_token_usage() if _LLM_SERVICE else {}
        if token_usage:
            usage_parts = []
            for m, u in token_usage.items():
                usage_parts.append(f"{m}: {u['input_tokens']}in/{u['output_tokens']}out ({u['calls']} calls)")
            LOGGER.info(f"Token usage for {filename}: {', '.join(usage_parts)}")
        LOGGER.info(f"{action}: {filename}")
        detail = f"{nodes_written} noder, {edges_written} relationer"
        if is_reingest:
            detail = f"re-ingest: {detail}"
        terminal_status("ingestion", filename, "done", detail=detail)

    try:
        if _lock_held:
            # Caller holds locks (rebuild scenario)
            _do_process()
        else:
            # Acquire locks for this document (realtime scenario)
            from services.utils.vector_service import vector_scope
            with resource_lock("graph", exclusive=True):
                with vector_scope(exclusive=True) as vs:
                    _do_process(vector_service=vs)

    except Exception as e:
        LOGGER.error(f"HARDFAIL {filename}: {e}")
        terminal_status("ingestion", filename, "failed", str(e))
        with PROCESS_LOCK:
            PROCESSED_FILES.discard(filename)
        raise RuntimeError(f"HARDFAIL: Document processing failed for {filename}: {e}") from e


class DocumentHandler:
    """Watchdog event handler for new and modified documents."""

    def on_created(self, event):
        if event.is_directory:
            return
        process_document(event.src_path, os.path.basename(event.src_path))

    def on_modified(self, event):
        if event.is_directory:
            return
        fname = os.path.basename(event.src_path)
        if not UUID_SUFFIX_PATTERN.search(fname):
            return
        with PROCESS_LOCK:
            PROCESSED_FILES.discard(fname)
        process_document(event.src_path, fname)


# --- INIT & WATCHDOG ---
if __name__ == "__main__":
    from services.utils.terminal_status import service_status
    os.makedirs(LAKE_STORE, exist_ok=True)
    service_status("Ingestion Engine", "started")

    from watchdog.observers import Observer
    from watchdog.events import FileSystemEventHandler

    class WatchdogHandler(FileSystemEventHandler):
        def on_created(self, event):
            if event.is_directory:
                return
            process_document(event.src_path, os.path.basename(event.src_path))

        def on_modified(self, event):
            if event.is_directory:
                return
            fname = os.path.basename(event.src_path)
            if not UUID_SUFFIX_PATTERN.search(fname):
                return
            # Allow re-processing: clear from PROCESSED_FILES
            # so process_document can re-evaluate via _needs_reingest()
            with PROCESS_LOCK:
                PROCESSED_FILES.discard(fname)
            process_document(event.src_path, fname)

    folders = [
        CONFIG['paths']['asset_documents'],
        CONFIG['paths']['asset_slack'],
        CONFIG.get('paths', {}).get('asset_mail'),
        CONFIG.get('paths', {}).get('asset_calendar'),
        CONFIG['paths']['asset_transcripts'],
        CONFIG.get('paths', {}).get('asset_ai_generated'),
    ]

    with ThreadPoolExecutor(max_workers=5) as executor:
        for folder in folders:
            if folder and os.path.exists(folder):
                for f in os.listdir(folder):
                    if UUID_SUFFIX_PATTERN.search(f):
                        executor.submit(process_document, os.path.join(folder, f), f)

    observer = Observer()
    for folder in folders:
        if folder and os.path.exists(folder):
            observer.schedule(WatchdogHandler(), folder, recursive=False)
    observer.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()
