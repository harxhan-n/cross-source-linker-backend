from flask import jsonify, request
from .service import soft_delete_rule

def delete_rule_controller(rule_id):
    try:
        if request.method != 'DELETE':
            return jsonify({
                "status_code": 400,
                "status_message": "BAD REQUEST",
                "message": "Request malformed, verify and try again!"
            }), 400
        try:
            updated_rules = soft_delete_rule(rule_id)
        except KeyError:
            return jsonify({
                "status_code": 404,
                "status_message": "NOT FOUND",
                "message": f"Rule '{rule_id}' not found."
            }), 404
        except Exception:
            return jsonify({
                "status_code": 500,
                "status_message": "INTERNAL SERVER ERROR",
                "message": "Unexpected error occurred. Please try again!"
            }), 500
        return jsonify({
            "status_code": 200,
            "status_message": "SUCCESS",
            "message": "Rule deleted (soft) successfully!",
            "data": updated_rules
        }), 200
    except Exception:
        return jsonify({
            "status_code": 500,
            "status_message": "INTERNAL SERVER ERROR",
            "message": "Unexpected error occurred. Please try again!"
        }), 500
