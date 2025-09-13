import os
import json

FIELD_DATA_PATH = os.path.abspath(os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'db_jsons', 'field_data.json'))

def get_all_fields():
    with open(FIELD_DATA_PATH, 'r', encoding='utf-8') as f:
        data = json.load(f)
    return [field for field in data if field.get("is_active") is True]
