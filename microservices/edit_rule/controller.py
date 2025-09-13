from flask import jsonify, request
from .service import edit_rule_data

def edit_rule_controller(rule_id):
    try:
        if request.method != 'PATCH':
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
            updated_rules = edit_rule_data(rule_id, req_data)
        except KeyError:
            return jsonify({
                "status_code": 404,
                "status_message": "NOT FOUND",
                "message": f"Rule '{rule_id}' not found."
            }), 404
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
            "status_code": 200,
            "status_message": "SUCCESS",
            "message": "Rule updated successfully!",
            "data": updated_rules
        }), 200
    except Exception:
        return jsonify({
            "status_code": 500,
            "status_message": "INTERNAL SERVER ERROR",
            "message": "Unexpected error occurred. Please try again!"
        }), 500
