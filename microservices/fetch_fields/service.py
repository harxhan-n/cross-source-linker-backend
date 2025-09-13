import os
import json

def get_static_data():
	static_data_path = os.path.abspath(os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'db_jsons', 'static_data.json'))
	print(f"[INFO] fetch_fields_service: Reading static_data.json from {static_data_path}")
	with open(static_data_path, 'r', encoding='utf-8') as f:
		data = json.load(f)
	print(f"[DEBUG] static_data.json contents: {data}")
	return data
