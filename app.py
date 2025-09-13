# Suggest bulk rules (POST)
from microservices.suggest_bulk_rules.controller import suggest_bulk_rules_controller

from flask_cors import CORS
from flask import Flask
from microservices.fetch_fields.controller import fetch_fields_controller
from microservices.configure_fields.controller import configure_fields_controller
from microservices.edit_field.controller import edit_field_controller
from microservices.delete_field.controller import delete_field_controller
from microservices.configure_rule.controller import configure_rule_controller
from microservices.fetch_all_fields.controller import fetch_all_fields_controller
from microservices.fetch_all_rules.controller import fetch_all_rules_controller
from microservices.edit_rule.controller import edit_rule_controller
from microservices.delete_rule.controller import delete_rule_controller
from microservices.process_batch.controller import process_batch_controller
from microservices.re_run_batch.controller import re_run_batch_controller
from microservices.fetch_batch_results.controller import fetch_batch_results_controller
from microservices.export_batch_results.controller import export_batch_results_controller
from microservices.fetch_all_batches.controller import fetch_all_batches_controller

app = Flask(__name__)
CORS(app)  # Enable Cross-Origin Resource Sharing for frontend requests

# Root endpoint
@app.route("/", methods=["GET"])
def healthcheck():
    return {
        "status": 200,
        "status_message": "SUCCESS",
        "message": "Cross source link backend services are active!"
    }

# Fetch the droppdown fields (GET)
@app.route("/fetch_fields", methods=["GET"])
def fetch_fields():
    return fetch_fields_controller()

# Get all fields
@app.route("/all_fields", methods=["GET"])
def all_fields():
    return fetch_all_fields_controller()

# Add field (POST)
@app.route("/configure_fields", methods=["POST"])
def configure_fields():
    return configure_fields_controller()

# Edit field (PATCH)
@app.route("/edit_fields/<field_name>", methods=["PATCH"])
def edit_field(field_name):
    return edit_field_controller(field_name)

# Soft delete field (DELETE)
@app.route("/delete_fields/<field_name>", methods=["DELETE"])
def delete_field(field_name):
    return delete_field_controller(field_name)

# Get all rules
@app.route("/all_rules", methods=["GET"])
def all_rules():
    return fetch_all_rules_controller()

# Add rule (POST)
# Add rule (POST)
@app.route("/configure_rules", methods=["POST"])
def configure_rules():
    return configure_rule_controller()

# Suggest bulk rules (POST)
@app.route("/suggest_bulk_rules", methods=["POST"])
def suggest_bulk_rules():
    return suggest_bulk_rules_controller()

# Edit rule (PATCH)
@app.route("/edit_rules/<int:rule_id>", methods=["PATCH"])
def edit_rule(rule_id):
    return edit_rule_controller(rule_id)

# Soft delete rule (DELETE)
@app.route("/delete_rules/<int:rule_id>", methods=["DELETE"])
def delete_rule(rule_id):
    return delete_rule_controller(rule_id)

# Fetch all batch names and ids (GET)
@app.route("/all_batches", methods=["GET"])
def all_batches():
    return fetch_all_batches_controller()

# Process batch (POST)
@app.route("/process_batch", methods=["POST"])
def process_batch():
    return process_batch_controller()

# Re-run batch (POST)
@app.route("/re_run_batch", methods=["POST"])
def re_run_batch():
    return re_run_batch_controller()

# Fetch batch results (POST)
@app.route("/fetch_batch_results", methods=["POST"])
def fetch_batch_results():
    return fetch_batch_results_controller()

# Export batch results (POST)
@app.route("/export_batch_results", methods=["POST"])
def export_batch_results():
    return export_batch_results_controller()

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=8080)