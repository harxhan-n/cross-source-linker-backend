from flask import request
from .service import suggest_bulk_rules_service

def suggest_bulk_rules_controller():
    data = request.get_json()
    comments = data.get('comments')
    suspected_records = data.get('suspected_records')
    if not comments or not suspected_records:
        return {
            "status_code": 400,
            "status_message": "BAD REQUEST",
            "message": "Both 'comments' and 'suspected_records' are required."
        }, 400
    return suggest_bulk_rules_service(comments, suspected_records)
