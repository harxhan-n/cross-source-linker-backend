from flask import jsonify, request
from .service import soft_delete_field

def delete_field_controller(field_name):
    try:
        if request.method != 'DELETE':
            return jsonify({
                "status_code": 400,
                "status_message": "BAD REQUEST",
                "message": "Request malformed, verify and try again!"
            }), 400
        try:
            updated_fields = soft_delete_field(field_name)
        except KeyError:
            return jsonify({
                "status_code": 404,
                "status_message": "NOT FOUND",
                "message": f"Field '{field_name}' not found."
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
            "message": "Field deleted (soft) successfully!",
            "data": updated_fields
        }), 200
    except Exception:
        return jsonify({
            "status_code": 500,
            "status_message": "INTERNAL SERVER ERROR",
            "message": "Unexpected error occurred. Please try again!"
        }), 500
