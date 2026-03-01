"""
Edge post-processing for ingestion.

Deterministic edge property computation:
- Calendar ATTENDED edges from parsed attendee data
- Required edge property validation (HARDFAIL principle)
"""

import datetime
import logging
import re
from typing import Dict, List

from services.engines.ingestion._shared import _get_schema_validator, LOGGER


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

    validator = _get_schema_validator()
    attended_edge_type = validator.find_edge_type_by_property('duration_minutes')
    if not attended_edge_type:
        return

    # Schema-driven: read source/target types from edge definition
    edge_def = validator.schema['edges'][attended_edge_type]
    source_types = set(edge_def.get('source_type', []))
    target_types = set(edge_def.get('target_type', []))

    # Build lookups from ingestion_payload using schema-defined types
    source_lookup = {}
    for m in ingestion_payload:
        if m.get("action") in ("CREATE", "LINK") and m.get("type") in source_types:
            norm = _normalize_name_for_match(m["label"])
            source_lookup[norm] = m

    target_lookup = {}
    for m in ingestion_payload:
        if m.get("action") in ("CREATE", "LINK") and m.get("type") in target_types:
            target_lookup[m["label"].lower().strip()] = m

    # Remove all LLM-generated ATTENDED edges (we replace them)
    original_len = len(ingestion_payload)
    ingestion_payload[:] = [
        m for m in ingestion_payload
        if not (m.get("action") == "CREATE_EDGE" and m.get("edge_type") == attended_edge_type)
    ]
    removed = original_len - len(ingestion_payload)
    if removed > 0:
        LOGGER.info(f"Post-process: Removed {removed} LLM-generated ATTENDED edges (replaced by deterministic)")

    # Create deterministic ATTENDED edges
    created = 0
    for cal_event in cal_events:
        # Find matching target node (Event)
        event_title_lower = cal_event["title"].lower().strip()
        target_mention = target_lookup.get(event_title_lower)
        if not target_mention:
            # Try partial match (event node name might be shortened)
            for key, tm in target_lookup.items():
                if key in event_title_lower or event_title_lower in key:
                    target_mention = tm
                    break

        if not target_mention:
            LOGGER.debug(f"Post-process: No target node for '{cal_event['title']}', skipping {attended_edge_type}")
            continue

        target_uuid = target_mention["target_uuid"]

        for attendee_name in cal_event["accepted_attendees"]:
            # Find matching source node (Person)
            source_mention = source_lookup.get(attendee_name)
            if not source_mention:
                # Try matching parts (e.g. "cenk.bisgen" vs "cenk bisgen" in payload)
                for skey, sm in source_lookup.items():
                    if attendee_name.replace('.', ' ') == skey.replace('.', ' '):
                        source_mention = sm
                        break

            if not source_mention:
                LOGGER.debug(f"Post-process: No source node for attendee '{attendee_name}', skipping {attended_edge_type}")
                continue

            source_uuid = source_mention["target_uuid"]
            source_label = source_mention.get("canonical_name", source_mention["label"])
            target_label = target_mention.get("canonical_name", target_mention["label"])

            edge_props = {}
            if cal_event["duration_minutes"] > 0:
                edge_props["duration_minutes"] = cal_event["duration_minutes"]

            attended_edge = {
                "action": "CREATE_EDGE",
                "source_uuid": source_uuid,
                "target_uuid": target_uuid,
                "edge_type": attended_edge_type,
                "confidence": 1.0,
                "source_text": f"{source_label} -> {target_label}",
                "source_name": source_label,
                "source_type": source_mention.get("type", ""),
                "target_name": target_label,
                "target_type": target_mention.get("type", ""),
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

    Schema-driven: iterates all edge types and checks all required properties
    defined in graph_schema_template.json.
    """
    edges_schema = _get_schema_validator().schema.get('edges', {})

    for m in ingestion_payload:
        if m.get("action") != "CREATE_EDGE":
            continue

        edge_type = m.get("edge_type")
        edge_props = m.get("edge_properties", {})
        edge_def = edges_schema.get(edge_type, {})

        for prop_name, prop_def in edge_def.get('properties', {}).items():
            if not isinstance(prop_def, dict):
                continue
            if not prop_def.get('required') or prop_name == 'confidence':
                continue
            if not edge_props.get(prop_name):
                source_name = m.get("source_name", "?")
                target_name = m.get("target_name", "?")
                LOGGER.error(
                    f"Post-process: {edge_type} missing REQUIRED {prop_name} "
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
    calendar_profile = _get_schema_validator().get_source_type_mappings().get('calendar', '')
    if source_type == calendar_profile:
        _build_calendar_attended(ingestion_payload, raw_text)

    _validate_required_edge_properties(ingestion_payload)

    return ingestion_payload
