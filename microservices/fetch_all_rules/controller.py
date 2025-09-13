from flask import jsonify
from .service import get_all_rules

def fetch_all_rules_controller():
    try:
        data = get_all_rules()
        return jsonify({
            "status_code": 200,
            "status_message": "SUCCESS",
            "message": "Fetched all rules.",
            "data": data
        }), 200
    except Exception:
        return jsonify({
            "status_code": 500,
            "status_message": "INTERNAL SERVER ERROR",
            "message": "Unexpected error occurred. Please try again!"
        }), 500
