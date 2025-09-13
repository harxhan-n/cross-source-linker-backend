import os
import json

RULE_DATA_PATH = os.path.abspath(os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'db_jsons', 'rule_data.json'))

def soft_delete_rule(rule_id):
    rule_id = int(rule_id)
    with open(RULE_DATA_PATH, 'r', encoding='utf-8') as f:
        data = json.load(f)
    found = False
    for rule in data:
        if rule.get("rule_id") == rule_id and rule.get("is_active", True):
            rule["is_active"] = False
            found = True
            break
    if not found:
        raise KeyError(f"Rule '{rule_id}' not found.")
    with open(RULE_DATA_PATH, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=4)
    return [r for r in data if r.get("is_active") is True]
