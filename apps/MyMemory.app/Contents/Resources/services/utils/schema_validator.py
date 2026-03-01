import json
import os
import logging
import yaml  # Kräver PyYAML. Fallback finns nedan om den saknas.
from typing import Dict, Any, Tuple, Optional

LOGGER = logging.getLogger(__name__)


def normalize_value(value: Any, expected_type: str, item_schema: Dict = None) -> Any:
    """
    Normalisera värde till förväntad typ.

    Args:
        value: Värdet att normalisera
        expected_type: Förväntad typ från schemat (string, list, integer, etc.)
        item_schema: För listor - schema för varje element

    Returns:
        Normaliserat värde, eller None om omöjligt
    """
    if value is None:
        return None

    if expected_type == "string":
        if isinstance(value, str):
            return value
        if isinstance(value, list):
            return ' '.join(str(v) for v in value)
        return str(value)

    if expected_type == "integer":
        if isinstance(value, int):
            return value
        try:
            return int(value)
        except (ValueError, TypeError):
            return None

    if expected_type == "float":
        if isinstance(value, (int, float)):
            return float(value)
        try:
            return float(value)
        except (ValueError, TypeError):
            return None

    if expected_type == "boolean":
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            return value.lower() in ('true', '1', 'yes')
        return bool(value)

    if expected_type == "list" and item_schema:
        if not isinstance(value, list):
            return None
        normalized = []
        for item in value:
            if isinstance(item, dict):
                norm_item = {}
                for field, field_def in item_schema.items():
                    field_type = field_def.get("type", "string")
                    field_value = item.get(field)
                    if field_value is not None:
                        norm_item[field] = normalize_value(field_value, field_type)
                    elif field_def.get("required"):
                        return None  # Saknar required field
                    else:
                        norm_item[field] = None
                normalized.append(norm_item)
            else:
                return None  # Element är inte dict
        return normalized

    return value

class SchemaValidator:
    def __init__(self, schema_path: str = None):
        # 1. Om ingen sökväg ges, slå upp den i config-filen
        if not schema_path:
            self.schema_path = self._resolve_schema_path_from_config()
        else:
            self.schema_path = schema_path
            
        self.schema = self._load_and_merge_schema()
        LOGGER.info(f"SchemaValidator loaded from {self.schema_path}")

    def _resolve_schema_path_from_config(self) -> str:
        """Läser config för att hitta rätt schema-fil."""
        from services.utils.config_loader import get_config, get_config_path
        from pathlib import Path

        # Schema ligger i samma katalog som config
        config_dir = Path(get_config_path()).parent
        default_template = config_dir / "graph_schema_template.json"

        try:
            config = get_config()
            relative_path = config.get("graph_schema")
            if relative_path:
                return str(config_dir / relative_path)
        except (FileNotFoundError, KeyError) as e:
            LOGGER.warning(f"Failed to read config: {e}. Using default: {default_template}")

        return str(default_template)

    def _load_and_merge_schema(self) -> Dict[str, Any]:
        """Laddar JSON-filen och slår ihop 'base_properties' med noder."""
        if not os.path.exists(self.schema_path):
            raise FileNotFoundError(f"Schema file not found at {self.schema_path}")

        try:
            with open(self.schema_path, 'r', encoding='utf-8') as f:
                raw_schema = json.load(f)
        except json.JSONDecodeError as e:
            LOGGER.error(f"Invalid JSON in schema file: {e}")
            raise

        # Hämta base properties
        base_props = raw_schema.get("base_properties", {}).get("properties", {})
        
        # Merge logic
        nodes = raw_schema.get("nodes", {})
        for node_name, node_def in nodes.items():
            merged_props = base_props.copy()
            merged_props.update(node_def.get("properties", {}))
            node_def["properties"] = merged_props
            
        raw_schema["nodes"] = nodes
        return raw_schema

    def find_node_type_by_property(self, unique_property: str) -> Optional[str]:
        """Find node type that defines a specific non-base property.

        Use properties unique to one node type as structural identifiers:
        - 'email' → Person
        - 'org_type' → Organization
        - 'project_status' → Project
        """
        base_props = set(self.schema.get('base_properties', {}).get('properties', {}).keys())
        for name, defn in self.schema.get('nodes', {}).items():
            own_props = set(defn.get('properties', {}).keys()) - base_props
            if unique_property in own_props:
                return name
        return None

    def find_edge_type_by_property(self, unique_property: str) -> Optional[str]:
        """Find edge type that defines a specific property.

        Use properties unique to one edge type as structural identifiers:
        - 'job_title' → BELONGS_TO
        - 'relation_type' → HAS_BUSINESS_RELATION
        - 'duration_minutes' → ATTENDED
        """
        for name, defn in self.schema.get('edges', {}).items():
            if unique_property in defn.get('properties', {}):
                return name
        return None

    def find_edge_type_by_target(self, target_type: str) -> Optional[str]:
        """Find the single edge type targeting a specific node type.

        Works when only one edge type targets the given type:
        - 'Roles' target → HAS_ROLE (only edge targeting Roles)
        """
        matches = [
            name for name, defn in self.schema.get('edges', {}).items()
            if target_type in defn.get('target_type', [])
            and 'Source' not in defn.get('source_type', [])
        ]
        return matches[0] if len(matches) == 1 else None

    def validate_node(self, node_data: Dict[str, Any]) -> Tuple[bool, str]:
        node_type = node_data.get("type")
        if not node_type: return False, "Missing field: 'type'"
        
        node_def = self.schema["nodes"].get(node_type)
        if not node_def:
            return False, f"Unknown node type: '{node_type}'"

        # Validera required base_properties från schemat
        base_props = self.schema.get("base_properties", {}).get("properties", {})
        for field, field_def in base_props.items():
            if field_def.get("required", False) and field not in node_data:
                return False, f"Missing system field: '{field}'"

        # Check name quality
        node_name = node_data.get('name', '')
        if not node_name: return False, "Missing 'name' field"
        
        # Rule: Name cannot be identical to Type (lazy LLM)
        if node_name.lower().replace(" ", "").replace("_", "") == node_type.lower().replace(" ", "").replace("_", ""):
             return False, f"Node name '{node_name}' is too generic (same as type '{node_type}'). Please provide a specific name."

        # Validate Properties against Schema
        properties_def = node_def.get("properties", {})
        for prop_name, prop_def in properties_def.items():
            # 1. Required Check
            if prop_def.get("required", False) and prop_name not in node_data:
                return False, f"Missing required property: '{prop_name}'"

            # 2. Type and Value Check
            if prop_name in node_data:
                value = node_data[prop_name]

                # Enum Check
                allowed_values = prop_def.get("values")
                if allowed_values and value not in allowed_values:
                    return False, f"Invalid value for '{prop_name}': '{value}'. Allowed: {allowed_values}"

                # Type Check
                expected_type = prop_def.get("type")
                item_schema = prop_def.get("item_schema")
                if expected_type:
                    ok, msg = self._validate_type(value, expected_type, prop_name, item_schema)
                    if not ok:
                        return False, msg

        return True, "OK"

    def _validate_type(self, value: Any, expected_type: str, prop_name: str, item_schema: Dict = None) -> Tuple[bool, str]:
        """
        Validerar att värdet matchar förväntad typ från schemat.

        Args:
            value: Värdet att validera
            expected_type: Förväntad typ (string, integer, float, boolean, list, etc.)
            prop_name: Fältnamn för felmeddelanden
            item_schema: För listor - schema för varje element

        Returns:
            (True, "OK") om giltig, (False, "felmeddelande") annars
        """
        type_map = {
            "string": str,
            "integer": int,
            "float": (int, float),
            "boolean": bool,
            "list": list,
            "timestamp": str,
            "date": str,
            "uuid": str,
            "enum": str,
        }

        if expected_type not in type_map:
            return True, "OK"  # Okänd typ, hoppa över

        python_type = type_map[expected_type]
        if not isinstance(value, python_type):
            return False, f"Type mismatch for '{prop_name}': expected {expected_type}, got {type(value).__name__}"

        # Validera item_schema för listor
        if expected_type == "list" and item_schema and isinstance(value, list):
            for i, item in enumerate(value):
                if not isinstance(item, dict):
                    return False, f"{prop_name}[{i}]: expected dict, got {type(item).__name__}"
                for field, field_def in item_schema.items():
                    field_type = field_def.get("type", "string")
                    field_required = field_def.get("required", False)
                    field_value = item.get(field)

                    if field_required and field_value is None:
                        return False, f"{prop_name}[{i}].{field}: required field missing"
                    if field_value is not None:
                        ok, msg = self._validate_type(field_value, field_type, f"{prop_name}[{i}].{field}")
                        if not ok:
                            return False, msg

        return True, "OK"

    def validate_edge_structure(self, edge: Dict[str, Any], nodes_map: Dict[str, str]) -> Tuple[bool, str]:
        """
        Validerar en kants strukturella korrekthet: kanttyp finns i schemat,
        source/target-nodtyper är tillåtna. Kontrollerar INTE edge properties.
        Används av Dreamer för cleanup av befintliga kanter i grafen.
        """
        rel_type = edge.get('type')
        source = edge.get('source')
        target = edge.get('target')

        if not rel_type or not source or not target:
            return False, "Malformed edge: missing type, source, or target"

        edge_def = self.schema.get('edges', {}).get(rel_type)
        if not edge_def:
            return False, f"Unknown relation type: '{rel_type}'"

        s_type = nodes_map.get(source)
        t_type = nodes_map.get(target)

        if not s_type: return False, f"Source node '{source}' not found"
        if not t_type: return False, f"Target node '{target}' not found"

        allowed_sources = edge_def.get('source_type', [])
        allowed_targets = edge_def.get('target_type', [])

        if s_type not in allowed_sources:
            return False, f"Invalid source type '{s_type}' for relation '{rel_type}'. Allowed: {allowed_sources}"

        if t_type not in allowed_targets:
            return False, f"Invalid target type '{t_type}' for relation '{rel_type}'. Allowed: {allowed_targets}"

        return True, "OK"

    def validate_edge(self, edge: Dict[str, Any], nodes_map: Dict[str, str]) -> Tuple[bool, str]:
        """
        Validerar en kant mot schemat (struktur + properties).
        nodes_map: {node_name: node_type} - Mappning av nodnamn till typ för uppslagning.
        """
        # Structural validation first
        ok, msg = self.validate_edge_structure(edge, nodes_map)
        if not ok:
            return False, msg

        rel_type = edge.get('type')
        edge_def = self.schema.get('edges', {}).get(rel_type)

        # Validera edge properties mot schemat
        properties_def = edge_def.get("properties", {})
        for prop_name, prop_def in properties_def.items():
            # Required check
            if prop_def.get("required", False) and prop_name not in edge:
                return False, f"Edge '{rel_type}': missing required property '{prop_name}'"

            # Type and value check
            if prop_name in edge:
                value = edge[prop_name]

                # Enum check
                allowed_values = prop_def.get("values")
                if allowed_values and value not in allowed_values:
                    return False, f"Edge '{rel_type}': invalid value for '{prop_name}': '{value}'. Allowed: {allowed_values}"

                # Type check — extraction_type overrides type for LLM validation
                expected_type = prop_def.get("extraction_type", prop_def.get("type"))
                if expected_type:
                    ok, msg = self._validate_type(value, expected_type, f"edge.{prop_name}")
                    if not ok:
                        return False, msg

        return True, "OK"

    def get_node_types(self) -> set:
        """All defined node type names from schema."""
        return set(self.schema.get('nodes', {}).keys())

    def get_edge_types(self) -> set:
        """All defined edge type names from schema."""
        return set(self.schema.get('edges', {}).keys())

    def get_profile_names(self) -> set:
        """All source type profile names from schema."""
        profiles = self.schema.get('source_type_profiles', {})
        return {k for k in profiles if k != 'description'}

    def get_document_node_type(self) -> str:
        """Node type representing source documents (primary_key_strategy=UUID)."""
        for name, defn in self.schema.get('nodes', {}).items():
            if defn.get('primary_key_strategy') == 'UUID':
                return name
        raise ValueError("No document node type found in schema")

    def get_source_edge_target_types(self) -> set:
        """Node types that can be targets of document→entity edges."""
        doc_type = self.get_document_node_type()
        result = set()
        for edge_def in self.schema.get('edges', {}).values():
            if doc_type in edge_def.get('source_type', []):
                result.update(edge_def.get('target_type', []))
        return result

    def get_source_type_mappings(self) -> dict:
        """Path keyword → profile name mappings from processing_policy."""
        return dict(self.schema.get('processing_policy', {}).get('source_mappings', {}))

    def get_default_source_type(self) -> str:
        """Default source type profile name from processing_policy."""
        return self.schema.get('processing_policy', {}).get('default_source_type', '')

    def get_source_edge_types(self) -> set:
        """Edge types from Source nodes to entities (e.g. MENTIONS).

        Schema-driven: returns edge types where 'Source' is in source_type.
        Used to filter out document→entity edges when only entity→entity
        edges are relevant.
        """
        return {
            edge_name for edge_name, edge_def in self.schema.get('edges', {}).items()
            if 'Source' in edge_def.get('source_type', [])
        }

    def get_base_property_defaults(self, resolve_now: bool = True) -> Dict[str, Any]:
        """
        Generate default values for required base_properties that have a 'default'.

        Args:
            resolve_now: If True, "$NOW" is resolved to datetime.now().isoformat().
                         If False, "$NOW" is kept as-is (for callers needing reference_timestamp).

        Returns:
            Dict mapping property name to resolved default value.
        """
        from datetime import datetime

        base_props = self.schema.get("base_properties", {}).get("properties", {})
        defaults = {}

        for prop_name, prop_def in base_props.items():
            if not prop_def.get("required", False):
                continue
            if "default" not in prop_def:
                continue

            raw_default = prop_def["default"]
            if raw_default == "$NOW" and resolve_now:
                defaults[prop_name] = datetime.now().isoformat()
            else:
                defaults[prop_name] = raw_default

        return defaults