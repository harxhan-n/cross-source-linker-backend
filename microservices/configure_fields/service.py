import os
import json

FIELD_DATA_PATH = os.path.abspath(os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'db_jsons', 'field_data.json'))

def add_field_data(field_dict):
    print(f"[INFO] add_field_data called with: {field_dict}")
    new_field = {
        "field_name": field_dict["field_name"],
        "type": field_dict["field_type"],
        "required": field_dict["requried"],
        "has_influence": field_dict["has_influence"],
        "is_active": True
    }
    print(f"[DEBUG] Using field_data.json at: {FIELD_DATA_PATH}")
    try:
        with open(FIELD_DATA_PATH, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except Exception:
        data = []
    for field in data:
        if field.get("field_name") == new_field["field_name"]:
            print(f"[DEBUG] Duplicate field_name detected: {new_field['field_name']}")
            raise ValueError("Field with this name already exists.")
    data.append(new_field)
    print(f"[INFO] New field appended: {new_field}")
    try:
        print(f"[DEBUG] Type of data to write: {type(data)}")
        with open(FIELD_DATA_PATH, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=4)
        print(f"[INFO] field_data.json updated. Current fields: {data}")
    except Exception as e:
        print(f"[ERROR] Failed to write to field_data.json: {e}")
        raise
    # Only return fields where is_active is True
    active_fields = [field for field in data if field.get("is_active") is True]
    return active_fields
