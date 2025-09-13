import os
import json

FIELD_DATA_PATH = os.path.abspath(os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'db_jsons', 'field_data.json'))

def edit_field_data(field_name, updates):
    print(f"[INFO] edit_field_data called for field: {field_name} with updates: {updates}")
    with open(FIELD_DATA_PATH, 'r', encoding='utf-8') as f:
        data = json.load(f)
    found = False
    for field in data:
        if field.get("field_name") == field_name and field.get("is_active", True):
            print(f"[DEBUG] Editing field: {field}")
            for k, v in updates.items():
                if k in field:
                    print(f"[DEBUG] Updating {k} from {field[k]} to {v}")
                    field[k] = v
            found = True
            break
    if not found:
        print(f"[WARN] Field '{field_name}' not found for edit.")
        raise KeyError(f"Field '{field_name}' not found.")
    with open(FIELD_DATA_PATH, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=4)
    print(f"[INFO] field_data.json updated after edit. Current fields: {data}")
    return [f for f in data if f.get("is_active") is True]
