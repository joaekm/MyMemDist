"""
Entity extraction via MCP validator server.

Builds extraction prompts with schema context and delegates to the
MCP validator_mcp.py process for entity extraction + validation.
"""

import asyncio
import datetime
import logging
import os
import sys

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

from services.utils.json_parser import parse_llm_json
from services.engines.ingestion._shared import (
    CONFIG, _get_schema_validator, get_prompt, LOGGER,
)
from services.engines.ingestion.source_profile import get_source_profile

# MCP Server Configuration
VALIDATOR_PARAMS = StdioServerParameters(
    command=sys.executable,
    args=[os.path.abspath(os.path.join(
        os.path.dirname(__file__), "..", "..", "agents", "validator_mcp.py"
    ))]
)


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


def extract_entities_mcp(text: str, source_hint: str = "") -> dict:
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
    doc_type = validator.get_document_node_type()
    all_node_types = set(schema.get('nodes', {}).keys())
    valid_graph_nodes = all_node_types - {doc_type}

    filtered_nodes = {k: v for k, v in schema.get('nodes', {}).items() if k != doc_type}
    node_lines = []
    for k, v in filtered_nodes.items():
        desc = v.get('description', '')
        props = v.get('properties', {})

        prop_info = []
        for prop_name, prop_def in props.items():
            system_props = set(CONFIG.get('enrichment', {}).get('system_properties', []))
            if prop_name in system_props:
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

    source_edges = _get_schema_validator().get_source_edge_types()
    filtered_edges = {k: v for k, v in schema.get('edges', {}).items() if k not in source_edges}
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
            # extraction_type overrides type for LLM prompt (e.g. list→string)
            p_type = prop_def.get('extraction_type', prop_def.get('type', 'string'))
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
