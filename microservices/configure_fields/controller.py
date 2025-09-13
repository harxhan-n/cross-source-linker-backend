from flask import jsonify, request
from .service import add_field_data

def configure_fields_controller():
    try:
        if request.method != 'POST':
            response = {
                "status_code": 400,
                "status_message": "BAD REQUEST",
                "message": "Request malformed, verify and try again!"
            }
            return jsonify(response), 400

        req_data = request.get_json(force=True, silent=True)
        if not req_data:
            response = {
                "status_code": 400,
                "status_message": "BAD REQUEST",
                "message": "Request malformed, verify and try again!"
            }
            return jsonify(response), 400


        required_fields = ["field_name", "field_type", "has_influence", "requried"]
        missing_keys = [k for k in required_fields if k not in req_data]
        if missing_keys:
            response = {
                "status_code": 400,
                "status_message": "BAD REQUEST",
                "message": f"Missing required key(s): {', '.join(missing_keys)}"
            }
            return jsonify(response), 400

        try:
            updated_data = add_field_data(req_data)
            print(f"[DEBUG] Controller received updated_data: {updated_data}")
        except ValueError as ve:
            msg = str(ve)
            if "already exists" in msg:
                response = {
                    "status_code": 400,
                    "status_message": "BAD REQUEST",
                    "message": "Field with this name already exists."
                }
            else:
                response = {
                    "status_code": 400,
                    "status_message": "BAD REQUEST",
                    "message": msg
                }
            return jsonify(response), 400
        except Exception:
            response = {
                "status_code": 500,
                "status_message": "INTERNAL SERVER ERROR",
                "message": "Unexpected error occurred. Please try again!"
            }
            return jsonify(response), 500

        response = {
            "status_code": 201,
            "status_message": "SUCCESS",
            "message": "Successfully added the field to parse!",
            "data": updated_data
        }
        return jsonify(response), 201
    except Exception:
        response = {
            "status_code": 500,
            "status_message": "INTERNAL SERVER ERROR",
            "message": "Unexpected error occurred. Please try again!"
        }
        return jsonify(response), 500
