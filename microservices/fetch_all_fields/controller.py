from flask import jsonify
from .service import get_all_fields

def fetch_all_fields_controller():
    try:
        data = get_all_fields()
        return jsonify({
            "status_code": 200,
            "status_message": "SUCCESS",
            "message": "Fetched all fields.",
            "data": data
        }), 200
    except Exception:
        return jsonify({
            "status_code": 500,
            "status_message": "INTERNAL SERVER ERROR",
            "message": "Unexpected error occurred. Please try again!"
        }), 500
