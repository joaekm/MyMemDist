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
        """Läser config/my_mem_config.yaml för att hitta rätt schema-fil."""
        base_dir = os.path.dirname(os.path.dirname(os.path.dirname(__file__))) # Root: MyMemory/
        config_path = os.path.join(base_dir, "config", "my_mem_config.yaml")
        
        default_template = os.path.join(base_dir, "config", "graph_schema_template.json")

        if not os.path.exists(config_path):
            LOGGER.warning(f"Config file not found at {config_path}. Using default: {default_template}")
            return default_template

        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)
                relative_path = config.get("graph_schema")

                if relative_path:
                    return os.path.join(base_dir, relative_path)

        except (OSError, yaml.YAMLError) as e:
            LOGGER.warning(f"Failed to read config file: {e}. Using default: {default_template}")

        return default_template

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
            
            # Healing policy fallback
            if "healing_policy" not in node_def:
                node_def["healing_policy"] = {
                    "max_days_as_provisional": 90,
                    "min_connections_to_survive": 2
                }

        raw_schema["nodes"] = nodes
        return raw_schema

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

        # Validera status mot schema-definierade värden
        status = node_data.get("status")
        status_def = base_props.get("status", {})
        allowed_statuses = status_def.get("values", ["VERIFIED", "PROVISIONAL"])
        if status not in allowed_statuses:
            return False, f"Invalid status: '{status}'. Allowed: {allowed_statuses}"

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

    def get_healing_policy(self, node_type: str) -> Dict[str, Any]:
        return self.schema["nodes"].get(node_type, {}).get("healing_policy", {})

    def validate_edge(self, edge: Dict[str, Any], nodes_map: Dict[str, str]) -> Tuple[bool, str]:
        """
        Validerar en kant mot schemat.
        nodes_map: {node_name: node_type} - Mappning av nodnamn till typ för uppslagning.
        """
        rel_type = edge.get('type')
        source = edge.get('source')
        target = edge.get('target')

        if not rel_type or not source or not target:
            return False, "Malformed edge: missing type, source, or target"

        # Kolla att relationstypen finns
        edge_def = self.schema.get('edges', {}).get(rel_type)
        if not edge_def:
            return False, f"Unknown relation type: '{rel_type}'"

        # Kolla att noder finns i extraktionen
        s_type = nodes_map.get(source)
        t_type = nodes_map.get(target)

        if not s_type: return False, f"Source node '{source}' not found in extraction"
        if not t_type: return False, f"Target node '{target}' not found in extraction"

        # Validera riktning
        allowed_sources = edge_def.get('source_type', [])
        allowed_targets = edge_def.get('target_type', [])

        if s_type not in allowed_sources:
            return False, f"Invalid source type '{s_type}' for relation '{rel_type}'. Allowed: {allowed_sources}"
        
        if t_type not in allowed_targets:
            return False, f"Invalid target type '{t_type}' for relation '{rel_type}'. Allowed: {allowed_targets}"

        return True, "OK"