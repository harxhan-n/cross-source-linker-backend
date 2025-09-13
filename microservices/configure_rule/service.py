import os
import json
from agents.rule_code_block_agent import RuleCodeBlockAgent

RULE_DATA_PATH = os.path.abspath(os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'db_jsons', 'rule_data.json'))
STATIC_DATA_PATH = os.path.abspath(os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'db_jsons', 'static_data.json'))
FIELD_DATA_PATH = os.path.abspath(os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'db_jsons', 'field_data.json'))

def get_static_options():
    with open(STATIC_DATA_PATH, 'r', encoding='utf-8') as f:
        static_data = json.load(f)
    return static_data.get('match_classification', []), static_data.get('match_types', [])

def add_rule_data(rule_dict):
    print(f"[INFO] add_rule_data called with: {rule_dict}")
    match_classification_options, match_type_options = get_static_options()
    # Validate required fields
    required_fields = ["rule_name", "description", "source_field", "target_field", "match_classification", "match_type", "tie-breaker", "weight", "rationale_statement"]
    missing = [k for k in required_fields if k not in rule_dict]
    if missing:
        raise ValueError(f"Missing required key(s): {', '.join(missing)}")
    # Validate match_classification and match_type
    if rule_dict["match_classification"] not in match_classification_options:
        raise ValueError(f"Invalid match_classification: {rule_dict['match_classification']}")
    if rule_dict["match_type"] not in match_type_options:
        raise ValueError(f"Invalid match_type: {rule_dict['match_type']}")
    # Validate source_field and target_field exist in field_data.json (active fields only)
    try:
        with open(FIELD_DATA_PATH, 'r', encoding='utf-8') as f:
            field_data = json.load(f)
    except Exception:
        field_data = []
    active_field_names = {f["field_name"] for f in field_data if f.get("is_active") is True}
    if rule_dict["source_field"] not in active_field_names:
        raise ValueError(f"source_field '{rule_dict['source_field']}' does not exist in active fields.")
    if rule_dict["target_field"] not in active_field_names:
        raise ValueError(f"target_field '{rule_dict['target_field']}' does not exist in active fields.")

    # Remove code_block if present
    rule = {k: v for k, v in rule_dict.items() if k != "code_block"}
    rule["is_active"] = True
    # Ensure description is present
    if "description" not in rule or not rule["description"]:
        raise ValueError("Missing required key: description")
    # Generate code_block using ADK agent, validate, and auto-fix if needed
    from agents.code_compilation_agent import CodeCompilationAgent
    try:
        agent = RuleCodeBlockAgent()
        code_block = agent.generate_code_block(
            rule["description"],
            rule["rationale_statement"],
            rule["source_field"],
            rule["target_field"]
        )
        if not code_block or not code_block.strip():
            raise ValueError("Failed to generate code_block from agent.")
        # Validate code_block with mock data
        compilation_agent = CodeCompilationAgent()
        mock_inputs = ("mock_source", "mock_target")
        result = compilation_agent.validate_code_block(code_block, "rule_code_block", mock_inputs)
        if not result["success"]:
            # Auto-fix with ADK agent
            code_block = agent.generate_code_block(
                rule["description"],
                rule["rationale_statement"],
                rule["source_field"],
                rule["target_field"]
            )
            result = compilation_agent.validate_code_block(code_block, "rule_code_block", mock_inputs)
            if not result["success"]:
                raise ValueError(f"Generated code_block is invalid: {result['error']}\n{result['traceback']}")
        rule["code_block"] = code_block
    except Exception as e:
        raise ValueError(f"Error generating code_block: {e}")
    try:
        with open(RULE_DATA_PATH, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except Exception:
        data = []
    # Auto-generate rule_id
    if data:
        max_id = max((r.get("rule_id", 0) for r in data), default=0)
    else:
        max_id = 0
    rule["rule_id"] = max_id + 1
    data.append(rule)
    with open(RULE_DATA_PATH, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=4)
    print(f"[INFO] rule_data.json updated. Current rules: {data}")
    return [r for r in data if r.get("is_active") is True]
