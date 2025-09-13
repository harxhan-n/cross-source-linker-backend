from flask import jsonify, request
from .service import add_rule_data

def configure_rule_controller():
    try:
        if request.method != 'POST':
            return jsonify({
                "status_code": 400,
                "status_message": "BAD REQUEST",
                "message": "Request malformed, verify and try again!"
            }), 400
        req_data = request.get_json(force=True, silent=True)
        if not req_data:
            return jsonify({
                "status_code": 400,
                "status_message": "BAD REQUEST",
                "message": "Request malformed, verify and try again!"
            }), 400
        try:
            updated_rules = add_rule_data(req_data)
        except ValueError as ve:
            return jsonify({
                "status_code": 400,
                "status_message": "BAD REQUEST",
                "message": str(ve)
            }), 400
        except Exception:
            return jsonify({
                "status_code": 500,
                "status_message": "INTERNAL SERVER ERROR",
                "message": "Unexpected error occurred. Please try again!"
            }), 500
        return jsonify({
            "status_code": 201,
            "status_message": "SUCCESS",
            "message": "Successfully added the rule!",
            "data": updated_rules
        }), 201
    except Exception:
        return jsonify({
            "status_code": 500,
            "status_message": "INTERNAL SERVER ERROR",
            "message": "Unexpected error occurred. Please try again!"
        }), 500
