from flask import request, jsonify
from .service import re_run_batch

def re_run_batch_controller():
    try:
        return re_run_batch(request)
    except Exception as e:
        print(f"[ERROR] {e}")
        return jsonify({
            "status_code": 500,
            "status_message": "INTERNAL SERVER ERROR",
            "message": "Unexpected error occurred. Please try again!"
        }), 500
