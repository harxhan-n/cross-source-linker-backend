import os
import json
from flask import jsonify

def fetch_all_batches():
    BATCH_DATA_PATH = os.path.abspath(os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'db_jsons', 'batch_data.json'))
    if not os.path.exists(BATCH_DATA_PATH):
        return jsonify({
            "status_code": 200,
            "status_message": "SUCCESS",
            "data": []
        }), 200
    with open(BATCH_DATA_PATH, 'r', encoding='utf-8') as f:
        batch_data = json.load(f)
    result = [
        {"batch_id": b.get("batch_id"), "batch_name": b.get("batch_name")}
        for b in batch_data if b.get("batch_id") and b.get("batch_name")
    ]
    return jsonify({
        "status_code": 200,
        "status_message": "SUCCESS",
        "data": result
    }), 200
