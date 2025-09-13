# Cross Source Linker Backend

This is the backend for the Cross Source Linker project. It provides APIs and agent-driven logic for field and rule management, batch processing, and automated rule suggestion/validation.

## Features
- Modular microservices for field and rule configuration
- Batch processing and matching logic
- Agent-driven rule code generation and validation
- Bulk rule suggestion API using LLM agents
- JSON-based persistent storage
- Flask-based REST API

## Project Structure
```
app.py
requirements.txt
.env (not tracked)
credentials.json (not tracked)
token.pickle (not tracked)
db_jsons/
  batch_data.json
  field_data.json
  rule_data.json
  static_data.json
agents/
  bulk_rule_suggestion_agent.py
  code_compilation_agent.py
  rule_code_block_agent.py
microservices/
  configure_fields/
  configure_rule/
  ... (other microservices)
    controller.py
    service.py
venv/ (not tracked)
```

## Setup
1. Create and activate a Python virtual environment:
   ```
   python -m venv venv
   .\venv\Scripts\activate
   ```
2. Install dependencies:
   ```
   pip install -r requirements.txt
   ```
3. Set up your `.env` file with required API keys and config.

## Running the App
```
python app.py
```
The Flask server will start (default: port 8080).


## API Endpoints

| Endpoint                        | Method | Description                                              |
|----------------------------------|--------|----------------------------------------------------------|
| `/`                              | GET    | Healthcheck/status                                       |
| `/fetch_fields`                  | GET    | Fetch dropdown fields                                    |
| `/all_fields`                    | GET    | Get all fields                                           |
| `/configure_fields`              | POST   | Add a new field                                          |
| `/edit_fields/<field_name>`      | PATCH  | Edit a field by name                                     |
| `/delete_fields/<field_name>`    | DELETE | Soft delete a field by name                              |
| `/all_rules`                     | GET    | Get all rules                                            |
| `/configure_rules`               | POST   | Add a new rule                                           |
| `/suggest_bulk_rules`            | POST   | Suggest rules from comments and suspected records         |
| `/edit_rules/<int:rule_id>`      | PATCH  | Edit a rule by ID                                        |
| `/delete_rules/<int:rule_id>`    | DELETE | Soft delete a rule by ID                                 |
| `/all_batches`                   | GET    | Get all batch names and IDs                              |
| `/process_batch`                 | POST   | Process a batch for matching                             |
| `/re_run_batch`                  | POST   | Re-run a batch                                           |
| `/fetch_batch_results`           | POST   | Fetch results for a batch                                |
| `/export_batch_results`          | POST   | Export batch results                                     |


## Version Control
- Sensitive files and virtual environments are excluded via `.gitignore`.
- To push code to git:
  ```
  git add .
  git commit -m "Your commit message"
  git push
  ```

## License
MIT
