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

def edit_rule_data(rule_id, updates):
    rule_id = int(rule_id)
    with open(RULE_DATA_PATH, 'r', encoding='utf-8') as f:
        data = json.load(f)
    found = False
    match_classification_options, match_type_options = get_static_options()
    with open(FIELD_DATA_PATH, 'r', encoding='utf-8') as f:
        field_data = json.load(f)
    active_field_names = {f["field_name"] for f in field_data if f.get("is_active") is True}
    for rule in data:
        if rule.get("rule_id") == rule_id and rule.get("is_active", True):
            needs_regen = False
            for k, v in updates.items():
                if k == "match_classification" and v not in match_classification_options:
                    raise ValueError(f"Invalid match_classification: {v}")
                if k == "match_type" and v not in match_type_options:
                    raise ValueError(f"Invalid match_type: {v}")
                if k in ("source_field", "target_field") and v not in active_field_names:
                    raise ValueError(f"{k} '{v}' does not exist in active fields.")
                if k in rule or k == "description":
                    rule[k] = v
                if k in ("description", "rationale_statement", "source_field", "target_field"):
                    needs_regen = True
            # Regenerate code_block if relevant fields changed, validate, and auto-fix if needed
            if needs_regen:
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
            found = True
            break
    if not found:
        raise KeyError(f"Rule '{rule_id}' not found.")
    with open(RULE_DATA_PATH, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=4)
    return [r for r in data if r.get("is_active") is True]
