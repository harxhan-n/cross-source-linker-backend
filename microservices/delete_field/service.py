import os
import json

FIELD_DATA_PATH = os.path.abspath(os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'db_jsons', 'field_data.json'))

def soft_delete_field(field_name):
    print(f"[INFO] soft_delete_field called for field: {field_name}")
    with open(FIELD_DATA_PATH, 'r', encoding='utf-8') as f:
        data = json.load(f)
    found = False
    for field in data:
        if field.get("field_name") == field_name and field.get("is_active", True):
            print(f"[DEBUG] Soft deleting field: {field}")
            field["is_active"] = False
            found = True
            break
    if not found:
        print(f"[WARN] Field '{field_name}' not found for delete.")
        raise KeyError(f"Field '{field_name}' not found.")
    with open(FIELD_DATA_PATH, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=4)
    print(f"[INFO] field_data.json updated after delete. Current fields: {data}")
    return [f for f in data if f.get("is_active") is True]
