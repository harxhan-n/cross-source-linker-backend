from flask import jsonify
from .service import fetch_all_batches

def fetch_all_batches_controller():
    try:
        return fetch_all_batches()
    except Exception as e:
        print(f"[ERROR] {e}")
        return jsonify({
            "status_code": 500,
            "status_message": "INTERNAL SERVER ERROR",
            "message": "Unexpected error occurred. Please try again!"
        }), 500
