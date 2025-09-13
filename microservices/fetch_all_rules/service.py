import os
import json

RULE_DATA_PATH = os.path.abspath(os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'db_jsons', 'rule_data.json'))

def get_all_rules():
    with open(RULE_DATA_PATH, 'r', encoding='utf-8') as f:
        data = json.load(f)
    return [rule for rule in data if rule.get("is_active") is True]
