import os
import pandas as pd
import numpy as np
import json
from datetime import datetime

def re_run_batch(request):
    # Accept JSON body with batch_id only
    data = request.get_json()
    batch_id = data.get('batch_id') if data else None
    if not batch_id:
        return {
            "status_code": 400,
            "status_message": "BAD REQUEST",
            "message": "batch_id is required in JSON body."
        }, 400
    # Lookup batch_name and batch_dir from batch_data.json
    BATCH_DATA_PATH = os.path.abspath(os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'db_jsons', 'batch_data.json'))
    if not os.path.exists(BATCH_DATA_PATH):
        return {
            "status_code": 404,
            "status_message": "NOT FOUND",
            "message": "No batch data found."
        }, 404
    with open(BATCH_DATA_PATH, 'r', encoding='utf-8') as f:
        batch_data = json.load(f)
    batch_info = next((b for b in batch_data if b.get('batch_id') == batch_id), None)
    if not batch_info:
        return {
            "status_code": 404,
            "status_message": "NOT FOUND",
            "message": f"Batch with id '{batch_id}' not found."
        }, 404
    batch_name = batch_info['batch_name']
    orig_batch_dir = batch_info['batch_dir']
    base_dir = 'batch_information'
    if not os.path.exists(orig_batch_dir):
        return {
            "status_code": 400,
            "status_message": "BAD REQUEST",
            "message": f"Batch directory '{orig_batch_dir}' does not exist. Only existing batches can be re-run."
        }, 400
    # Find next available re-run number
    n = 1
    while os.path.exists(os.path.join(base_dir, f"{batch_name}_ReRun{n}")):
        n += 1
    batch_dir = os.path.join(base_dir, f"{batch_name}_ReRun{n}")
    os.makedirs(batch_dir, exist_ok=True)
    # Copy source and target files from original batch
    orig_files = os.listdir(orig_batch_dir)
    # Find first CSV/Excel files for source and target
    source_file_name = next((f for f in orig_files if f.lower().endswith(('.csv', '.xls', '.xlsx'))), None)
    target_file_name = next((f for f in orig_files if f != source_file_name and f.lower().endswith(('.csv', '.xls', '.xlsx'))), None)
    if not source_file_name or not target_file_name:
        return {
            "status_code": 400,
            "status_message": "BAD REQUEST",
            "message": f"Original batch '{batch_name}' does not contain both source and target files."
        }, 400
    import shutil
    source_path = os.path.join(batch_dir, source_file_name)
    target_path = os.path.join(batch_dir, target_file_name)
    shutil.copy2(os.path.join(orig_batch_dir, source_file_name), source_path)
    shutil.copy2(os.path.join(orig_batch_dir, target_file_name), target_path)
    def load_df(path):
        if path.endswith('.csv'):
            return pd.read_csv(path)
        elif path.endswith('.xls') or path.endswith('.xlsx'):
            return pd.read_excel(path)
        else:
            raise ValueError('Unsupported file type. Only CSV and Excel are supported.')
    try:
        df_source = load_df(source_path)
        df_target = load_df(target_path)
    except Exception as e:
        return {
            "status_code": 400,
            "status_message": "BAD REQUEST",
            "message": f"Error loading files: {e}"
        }, 400
    RULE_DATA_PATH = os.path.abspath(os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'db_jsons', 'rule_data.json'))
    with open(RULE_DATA_PATH, 'r', encoding='utf-8') as f:
        rules = [r for r in json.load(f) if r.get('is_active') is True]
    valid_rules = []
    for rule in rules:
        src_field = rule.get('source_field')
        tgt_field = rule.get('target_field')
        if src_field in df_source.columns and tgt_field in df_target.columns:
            valid_rules.append(rule)
    def convert_obj(obj):
        if isinstance(obj, (pd.Timestamp, np.datetime64)):
            return str(obj)
        if isinstance(obj, dict):
            return {k: convert_obj(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [convert_obj(i) for i in obj]
        return obj
    def save_json(data, name):
        path = os.path.join(batch_dir, name)
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(convert_obj(data), f, indent=4)
        return path
    matched = []
    suspected = []
    unmatched_source = set(df_source.index)
    unmatched_target = set(df_target.index)
    for rule in valid_rules:
        src_field = rule['source_field']
        tgt_field = rule['target_field']
        code_block = rule.get('code_block')
        for src_idx, src_row in df_source.iterrows():
            for tgt_idx, tgt_row in df_target.iterrows():
                src_dict = src_row.to_dict()
                tgt_dict = tgt_row.to_dict()
                local_env = {'source': src_dict, 'target': tgt_dict}
                try:
                    if code_block:
                        match = eval(compile(code_block, '<string>', 'eval'), {}, local_env)
                    else:
                        match = src_row[src_field] == tgt_row[tgt_field]
                except Exception as e:
                    match = False
                if match:
                    rationale_template = rule.get('rationale_statement') or ""
                    try:
                        rationale = rationale_template.format(
                            src_field=src_field,
                            tgt_field=tgt_field,
                            src_value=src_dict.get(src_field),
                            tgt_value=tgt_dict.get(tgt_field),
                            source=src_dict,
                            target=tgt_dict
                        )
                    except Exception:
                        rationale = rationale_template
                    matched.append({
                        'source_index': src_idx,
                        'target_index': tgt_idx,
                        'source_record': src_dict,
                        'target_record': tgt_dict,
                        'rule_id': rule['rule_id'],
                        'rule': rule,
                        'weight': rule.get('weight'),
                        'tie-breaker': rule.get('tie-breaker'),
                        'src_field': src_field,
                        'tgt_field': tgt_field,
                        'rationale_statement': rationale
                    })
                    if src_idx in unmatched_source:
                        unmatched_source.remove(src_idx)
                    if tgt_idx in unmatched_target:
                        unmatched_target.remove(tgt_idx)
    # Count occurrences
    src_counts = {}
    tgt_counts = {}
    for m in matched:
        src_counts[m['source_index']] = src_counts.get(m['source_index'], 0) + 1
        tgt_counts[m['target_index']] = tgt_counts.get(m['target_index'], 0) + 1

    # Find all source/target indices with duplicates
    duplicate_src = {idx for idx, count in src_counts.items() if count > 1}
    duplicate_tgt = {idx for idx, count in tgt_counts.items() if count > 1}

    # All matches with a duplicate source or target go to suspected
    suspected_flat = [m for m in matched if m['source_index'] in duplicate_src or m['target_index'] in duplicate_tgt]
    matched_final = [m for m in matched if m not in suspected_flat]

    # Group suspected by source_index (can also do by target_index if needed)
    from collections import defaultdict
    suspected_grouped = defaultdict(list)
    for m in suspected_flat:
        suspected_grouped[m['source_index']].append(m)
    # Format as array of objects: {source_index, source_record, targets: [all suspected matches]}
    suspected = []
    for src_idx, matches in suspected_grouped.items():
        suspected.append({
            'source_index': src_idx,
            'source_record': matches[0]['source_record'],
            'targets': [
                {
                    k: v for k, v in m.items() if k not in ['source_index', 'source_record']
                } for m in matches
            ]
        })

    # For suspected, add/refresh full source_record and rationale for each target
    for s in suspected:
        s['source_record'] = df_source.loc[s['source_index']].to_dict()
        for t in s['targets']:
            tgt_idx = t['target_index']
            t['target_record'] = df_target.loc[tgt_idx].to_dict()
            rule = t.get('rule')
            rationale_template = rule.get('rationale_statement') if rule else ""
            src_field_s = t.get('src_field')
            tgt_field_s = t.get('tgt_field')
            src_val = s['source_record'].get(src_field_s) if src_field_s else None
            tgt_val = t['target_record'].get(tgt_field_s) if tgt_field_s else None
            try:
                t['rationale_statement'] = rationale_template.format(
                    src_field=src_field_s,
                    tgt_field=tgt_field_s,
                    src_value=src_val,
                    tgt_value=tgt_val,
                    source=s['source_record'],
                    target=t['target_record']
                )
            except Exception:
                t['rationale_statement'] = rationale_template

    # Also reformat rationale_statement for matched_final
    for m in matched_final:
        rule = next((r for r in valid_rules if r['rule_id'] == m['rule_id']), None)
        m['rule'] = rule
        rationale_template = rule.get('rationale_statement') if rule else ""
        src_field_m = m.get('src_field')
        tgt_field_m = m.get('tgt_field')
        src_val = m['source_record'].get(src_field_m) if src_field_m else None
        tgt_val = m['target_record'].get(tgt_field_m) if tgt_field_m else None
        try:
            m['rationale_statement'] = rationale_template.format(
                src_field=src_field_m,
                tgt_field=tgt_field_m,
                src_value=src_val,
                tgt_value=tgt_val,
                source=m['source_record'],
                target=m['target_record']
            )
        except Exception:
            m['rationale_statement'] = rationale_template

    matched_final = [m for m in matched if m not in suspected]

    unmatched_source_records = df_source.loc[list(unmatched_source)].to_dict(orient='records')
    unmatched_target_records = df_target.loc[list(unmatched_target)].to_dict(orient='records')
    matched_path = save_json(matched_final, 'matched.json')
    suspected_path = save_json(suspected, 'suspected.json')
    unmatched_source_path = save_json(unmatched_source_records, 'unmatched_source.json')
    unmatched_target_path = save_json(unmatched_target_records, 'unmatched_target.json')

    # Compose result and batch summary to match process_batch
    batch_id = 'batch' + str(len(os.listdir('batch_information')))
    # Store only the relative batch_dir path (like process_batch)
    # batch_dir is already relative (e.g., 'batch_information/Sample Data_ReRun1')
    batch_name_full = f'{batch_name}_ReRun{n}'
    result = {
        'batch_id': batch_id,
        'batch_dir': batch_dir,
        'batch_name': batch_name_full,
        'matched_data': {
            'file_path': matched_path,
            'count': len(matched_final)
        },
        'suspected_data': {
            'file_path': suspected_path,
            'count': len(suspected)
        },
        'unmatched_source_data': {
            'file_path': unmatched_source_path,
            'count': len(unmatched_source_records)
        },
        'unmatched_target_data': {
            'file_path': unmatched_target_path,
            'count': len(unmatched_target_records)
        }
    }

    # Log batch summary
    BATCH_DATA_PATH = os.path.abspath(os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'db_jsons', 'batch_data.json'))
    try:
        if os.path.exists(BATCH_DATA_PATH):
            with open(BATCH_DATA_PATH, 'r', encoding='utf-8') as f:
                batch_data = json.load(f)
        else:
            batch_data = []
        batch_data.append(result)
        with open(BATCH_DATA_PATH, 'w', encoding='utf-8') as f:
            json.dump(batch_data, f, indent=4)
    except Exception as e:
        print(f"[ERROR] Failed to log batch summary: {e}")

    return {
        "status_code": 200,
        "status_message": "SUCCESS",
        "message": f"Re-run batch processed and results saved.",
        "data": result
    }, 200
