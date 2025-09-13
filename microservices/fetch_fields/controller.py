from flask import jsonify
from .service import get_static_data

def fetch_fields_controller():
	try:
		data = get_static_data()
		response = {
			"status_code": 200,
			"status_message": "SUCCESS",
			"message": "Fetched dropdown field's datat",
			"data": data
		}
		return jsonify(response), 200
	except Exception:
		response = {
			"status_code": 500,
			"status_message": "INTERNAL SERVER ERROR",
			"message": "Unexpected error occurred. Please try again!"
		}
		return jsonify(response), 500
