from flask import request, send_file, jsonify
from .service import export_batch_results

def export_batch_results_controller():
    try:
        return export_batch_results(request)
    except Exception as e:
        print(f"[ERROR] {e}")
        return jsonify({
            "status_code": 500,
            "status_message": "INTERNAL SERVER ERROR",
            "message": "Unexpected error occurred. Please try again!"
        }), 500
