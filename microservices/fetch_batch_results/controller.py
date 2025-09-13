from flask import request, jsonify
from .service import fetch_batch_results

def fetch_batch_results_controller():
    try:
        return fetch_batch_results(request)
    except Exception as e:
        print(f"[ERROR] {e}")
        return jsonify({
            "status_code": 500,
            "status_message": "INTERNAL SERVER ERROR",
            "message": "Unexpected error occurred. Please try again!"
        }), 500
