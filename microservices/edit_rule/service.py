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
    
    # First, find the rule to be updated
    rule_to_update = None
    for rule in data:
        if rule.get("rule_id") == rule_id and rule.get("is_active", True):
            rule_to_update = rule
            break
    
    if not rule_to_update:
        raise KeyError(f"Rule '{rule_id}' not found.")
    
    # Create a temporary copy of the rule with updates applied
    updated_rule = dict(rule_to_update)
    needs_regen = False
    for k, v in updates.items():
        if k == "match_classification" and v not in match_classification_options:
            raise ValueError(f"Invalid match_classification: {v}")
        if k == "match_type" and v not in match_type_options:
            raise ValueError(f"Invalid match_type: {v}")
        if k in ("source_field", "target_field") and v not in active_field_names:
            raise ValueError(f"{k} '{v}' does not exist in active fields.")
        if k in updated_rule or k == "description":
            updated_rule[k] = v
        if k in ("description", "rationale_statement", "source_field", "target_field"):
            needs_regen = True
    
    # Check for duplicate rule name
    if "rule_name" in updates:
        new_rule_name = updates["rule_name"]
        name_duplicates = [
            r for r in data 
            if r.get("rule_name") == new_rule_name 
            and r.get("rule_id") != rule_id 
            and r.get("is_active", True)
        ]
        if name_duplicates:
            raise ValueError(f"Rule with name '{new_rule_name}' already exists. Please use a different rule name.")
    
    # Check for duplicate rule definition (if relevant fields are being updated)
    if any(k in updates for k in ["source_field", "target_field", "match_classification", "match_type"]):
        source_field = updated_rule.get("source_field")
        target_field = updated_rule.get("target_field")
        match_classification = updated_rule.get("match_classification")
        match_type = updated_rule.get("match_type")
        
        definition_duplicates = [
            r for r in data if (
                r.get("source_field") == source_field and
                r.get("target_field") == target_field and
                r.get("match_classification") == match_classification and
                r.get("match_type") == match_type and
                r.get("rule_id") != rule_id and
                r.get("is_active", True)
            )
        ]
        
        if definition_duplicates:
            duplicate = definition_duplicates[0]
            raise ValueError(
                f"Cannot update rule: this would create a duplicate of rule ID {duplicate.get('rule_id')}, "
                f"'{duplicate.get('rule_name')}'. Both rules would have the same source field, target field, "
                f"match classification, and match type."
            )
    
    # Apply the updates to the actual rule
    for k, v in updates.items():
        if k in rule_to_update or k == "description":
            rule_to_update[k] = v
    
    # Regenerate code_block if relevant fields changed
    if needs_regen:
        from agents.code_compilation_agent import CodeCompilationAgent
        try:
            agent = RuleCodeBlockAgent()
            code_block = agent.generate_code_block(
                rule_to_update["description"],
                rule_to_update["rationale_statement"],
                rule_to_update["source_field"],
                rule_to_update["target_field"]
            )
            if not code_block or not code_block.strip():
                raise ValueError("Failed to generate code_block from agent.")
            
            compilation_agent = CodeCompilationAgent()
            mock_inputs = ("mock_source", "mock_target")
            result = compilation_agent.validate_code_block(code_block, "rule_code_block", mock_inputs)
            if not result["success"]:
                # Auto-fix with ADK agent
                code_block = agent.generate_code_block(
                    rule_to_update["description"],
                    rule_to_update["rationale_statement"],
                    rule_to_update["source_field"],
                    rule_to_update["target_field"]
                )
                result = compilation_agent.validate_code_block(code_block, "rule_code_block", mock_inputs)
                if not result["success"]:
                    raise ValueError(f"Generated code_block is invalid: {result['error']}\n{result['traceback']}")
            
            rule_to_update["code_block"] = code_block
        except Exception as e:
            raise ValueError(f"Error generating code_block: {e}")
    
    found = True
    if not found:
        raise KeyError(f"Rule '{rule_id}' not found.")
    with open(RULE_DATA_PATH, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=4)
    return [r for r in data if r.get("is_active") is True]
