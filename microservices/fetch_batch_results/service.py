import os
import json
from flask import request, jsonify

def fetch_batch_results(request):
    data = request.get_json()
    batch_id = data.get('batch_id') if data else None
    if not batch_id:
        return jsonify({
            "status_code": 400,
            "status_message": "BAD REQUEST",
            "message": "batch_id is required in JSON body."
        }), 400
    # Find batch info in batch_data.json
    BATCH_DATA_PATH = os.path.abspath(os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'db_jsons', 'batch_data.json'))
    if not os.path.exists(BATCH_DATA_PATH):
        return jsonify({
            "status_code": 404,
            "status_message": "NOT FOUND",
            "message": "No batch data found."
        }), 404
    with open(BATCH_DATA_PATH, 'r', encoding='utf-8') as f:
        batch_data = json.load(f)
    batch_info = next((b for b in batch_data if b.get('batch_id') == batch_id), None)
    if not batch_info:
        return jsonify({
            "status_code": 404,
            "status_message": "NOT FOUND",
            "message": f"Batch with id '{batch_id}' not found."
        }), 404
    # Read result files
    def read_json_file(path):
        if not os.path.exists(path):
            return None
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)
    matched = read_json_file(batch_info['matched_data']['file_path'])
    suspected = read_json_file(batch_info['suspected_data']['file_path'])
    unmatched_source = read_json_file(batch_info['unmatched_source_data']['file_path'])
    unmatched_target = read_json_file(batch_info['unmatched_target_data']['file_path'])
    return jsonify({
        "status_code": 200,
        "status_message": "SUCCESS",
        "message": f"Results for batch {batch_id} fetched successfully.",
        "data": {
            "matched": matched,
            "suspected": suspected,
            "unmatched_source": unmatched_source,
            "unmatched_target": unmatched_target
        }
    }), 200
