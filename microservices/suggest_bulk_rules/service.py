import json
from agents.bulk_rule_suggestion_agent import BulkRuleSuggestionAgent
from agents.code_compilation_agent import CodeCompilationAgent
from microservices.configure_rule.service import add_rule_data


def suggest_bulk_rules_service(comments, suspected_records):
    # If only indices are provided, resolve to full records
    resolved_records = []
    for rec in suspected_records:
        if 'source_record' in rec and 'target_record' in rec:
            resolved_records.append(rec)
        elif 'source_index' in rec and 'target_index' in rec and 'batch_dir' in rec:
            # Try to load records from batch files
            import os, pandas as pd
            batch_dir = rec['batch_dir']
            # Try to find source/target file names
            source_path = None
            target_path = None
            for fname in os.listdir(batch_dir):
                if 'source' in fname.lower():
                    source_path = os.path.join(batch_dir, fname)
                if 'target' in fname.lower():
                    target_path = os.path.join(batch_dir, fname)
            if source_path and target_path:
                df_source = pd.read_excel(source_path) if source_path.endswith('.xlsx') else pd.read_csv(source_path)
                df_target = pd.read_excel(target_path) if target_path.endswith('.xlsx') else pd.read_csv(target_path)
                src_idx = rec['source_index']
                tgt_idx = rec['target_index']
                rec['source_record'] = df_source.iloc[src_idx].to_dict()
                rec['target_record'] = df_target.iloc[tgt_idx].to_dict()
            resolved_records.append(rec)
        else:
            # Not enough info to resolve
            continue
    agent = BulkRuleSuggestionAgent()
    rule_suggestions = agent.suggest_rules(comments, resolved_records)
    compilation_agent = CodeCompilationAgent()
    added_rules = []
    errors = []
    for rule in rule_suggestions:
        code_block = rule.get('code_block')
        if not code_block:
            errors.append({"rule": rule, "error": "No code_block generated."})
            continue
        mock_inputs = ("mock_source", "mock_target")
        result = compilation_agent.validate_code_block(code_block, "rule_code_block", mock_inputs)
        if result["success"]:
            try:
                add_rule_data(rule)
                added_rules.append(rule)
            except Exception as e:
                errors.append({"rule": rule, "error": str(e)})
        else:
            errors.append({"rule": rule, "error": result["error"], "traceback": result["traceback"]})
    return {
        "status_code": 200,
        "status_message": "SUCCESS",
        "message": f"{len(added_rules)} rules added, {len(errors)} failed.",
        "added_rules": added_rules,
        "errors": errors
    }, 200
