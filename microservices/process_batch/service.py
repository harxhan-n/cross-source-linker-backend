import os
import pandas as pd
import numpy as np
import json
from collections import defaultdict

def process_batch(request):
    # Step 1: Parse form data and files
    batch_name = request.form.get('batch_name')
    source_file = request.files.get('source_file')
    target_file = request.files.get('target_file')
    if not batch_name or not source_file or not target_file:
        return {
            "status_code": 400,
            "status_message": "BAD REQUEST",
            "message": "batch_name, source_file, and target_file are required."
        }, 400
        
    # Step 2: Check if batch already exists
    batch_dir = os.path.join('batch_information', batch_name)
    if os.path.exists(batch_dir):
        return {
            "status_code": 400,
            "status_message": "BAD REQUEST",
            "message": f"Batch '{batch_name}' already exists. Choose a different name."
        }, 400
    os.makedirs(batch_dir, exist_ok=True)
    source_path = os.path.join(batch_dir, source_file.filename)
    target_path = os.path.join(batch_dir, target_file.filename)
    source_file.save(source_path)
    target_file.save(target_path)

    # Step 3: Load files into DataFrames
    def load_df(path):
        if path.endswith('.csv'):
            df = pd.read_csv(path)
        elif path.endswith('.xls') or path.endswith('.xlsx'):
            df = pd.read_excel(path)
        else:
            raise ValueError('Unsupported file type. Only CSV and Excel are supported.')
            
        # Ensure consistent column order by sorting columns alphabetically
        # This helps maintain consistent matching behavior
        df = df.sort_index(axis=1)
        return df
        
    try:
        df_source = load_df(source_path)
        df_target = load_df(target_path)
        
        # Print DataFrame info for debugging
        print(f"[DEBUG] process_batch: Loaded source dataframe from {source_path}")
        print(f"[DEBUG] process_batch: Source DataFrame shape: {df_source.shape}")
        print(f"[DEBUG] process_batch: Source DataFrame columns: {sorted(df_source.columns.tolist())}")
        
        print(f"[DEBUG] process_batch: Loaded target dataframe from {target_path}")
        print(f"[DEBUG] process_batch: Target DataFrame shape: {df_target.shape}")
        print(f"[DEBUG] process_batch: Target DataFrame columns: {sorted(df_target.columns.tolist())}")
    except Exception as e:
        return {
            "status_code": 400,
            "status_message": "BAD REQUEST",
            "message": f"Error loading files: {e}"
        }, 400

    # Step 4: Load active rules
    RULE_DATA_PATH = os.path.abspath(os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'db_jsons', 'rule_data.json'))
    with open(RULE_DATA_PATH, 'r', encoding='utf-8') as f:
        all_rules = json.load(f)
        # Sort rules by rule_id to ensure consistent processing order
        all_rules.sort(key=lambda r: r.get('rule_id', 0))
        rules = [r for r in all_rules if r.get('is_active') is True]

    # Only keep rules with valid source/target fields in the files
    valid_rules = []
    for rule in rules:
        src_field = rule.get('source_field')
        tgt_field = rule.get('target_field')
        if src_field in df_source.columns and tgt_field in df_target.columns:
            valid_rules.append(rule)
            
    # Print active rules being used
    print(f"[DEBUG] Process batch using {len(valid_rules)} rules:")
    for rule in valid_rules:
        print(f"  - Rule {rule.get('rule_id')}: {rule.get('rule_name')} ({rule.get('source_field')} -> {rule.get('target_field')})")

    # Step 5: Record comparison and classification
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
                        # First try to run as a function definition (rule_code_block)
                        func_env = {}
                        try:
                            exec(code_block, func_env)
                            rule_func = func_env.get('rule_code_block')
                            if callable(rule_func):
                                match = rule_func(src_dict.get(src_field), tgt_dict.get(tgt_field))
                            else:
                                match = eval(compile(code_block, '<string>', 'eval'), {}, local_env)
                        except Exception as ex:
                            print(f"[DEBUG] Error executing code_block as function, trying as expression: {ex}")
                            match = eval(compile(code_block, '<string>', 'eval'), {}, local_env)
                    else:
                        match = src_row[src_field] == tgt_row[tgt_field]
                except Exception as e:
                    print(f"[DEBUG] Error in rule matching: {e}")
                    match = False
                # Add debug for email matches
                if rule.get('rule_name') == 'Email Match':
                    print(f"[DEBUG] Email rule check: source={src_dict.get(src_field)}, target={tgt_dict.get(tgt_field)}, match={match}")
                
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
                        'src_field': src_field,
                        'tgt_field': tgt_field,
                        'rationale_statement': rationale
                    })
                    if src_idx in unmatched_source:
                        unmatched_source.remove(src_idx)
                    if tgt_idx in unmatched_target:
                        unmatched_target.remove(tgt_idx)

    # Organize matches by source-target pairs to identify multiple rule matches for the same pair
    src_tgt_pairs = defaultdict(list)
    for m in matched:
        pair_key = (m['source_index'], m['target_index'])
        src_tgt_pairs[pair_key].append(m)
    
    # Sort matches by rule_id to ensure consistent rule priority
    for pair_key in src_tgt_pairs:
        src_tgt_pairs[pair_key].sort(key=lambda x: x['rule_id'])
    
    # If a source-target pair has multiple matches (via different rules), keep only the first one
    # This handles one-to-one matches via multiple rules
    unique_matches = []
    for pair, matches in sorted(src_tgt_pairs.items()):  # Sort by pair keys for consistent ordering
        unique_matches.append(matches[0])  # Keep only the first rule match for each pair
    
    # Count how many unique target records each source record matches with
    src_to_unique_targets = defaultdict(set)
    for m in unique_matches:
        src_to_unique_targets[m['source_index']].add(m['target_index'])
    
    # Identify source records that match with multiple target records
    duplicate_src = {src_idx for src_idx, target_set in src_to_unique_targets.items() if len(target_set) > 1}
    
    # Only matches where source has multiple target matches go to suspected
    suspected_flat = [m for m in unique_matches if m['source_index'] in duplicate_src]
    matched_final = [m for m in unique_matches if m not in suspected_flat]

    # Group suspected by source_index (can also do by target_index if needed)
    suspected_grouped = defaultdict(list)
    for m in suspected_flat:
        suspected_grouped[m['source_index']].append(m)
    
    # Format as array of objects: {source_index, source_record, targets: [all suspected matches]}
    suspected = []
    for src_idx, matches in suspected_grouped.items():
        # All matches have same source_record
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

    # Print summary of matching results for debugging
    print(f"[DEBUG] Process batch matching summary:")
    print(f"  - Total matches found: {len(matched)}")
    print(f"  - Unique matches (after pair deduplication): {len(unique_matches)}")
    print(f"  - Suspected matches: {len(suspected)}")
    print(f"  - Final matched count: {len(matched_final)}")
    print(f"  - Unmatched source: {len(unmatched_source)}")
    print(f"  - Unmatched target: {len(unmatched_target)}")

    # Prepare unmatched records
    unmatched_source_records = df_source.loc[list(unmatched_source)].to_dict(orient='records')
    unmatched_target_records = df_target.loc[list(unmatched_target)].to_dict(orient='records')

    matched_path = save_json(matched_final, 'matched.json')
    suspected_path = save_json(suspected, 'suspected.json')
    unmatched_source_path = save_json(unmatched_source_records, 'unmatched_source.json')
    unmatched_target_path = save_json(unmatched_target_records, 'unmatched_target.json')

    result = {
        'batch_id': 'batch'+ str(len(os.listdir('batch_information'))), 
        'batch_dir': batch_dir,
        'batch_name': batch_name,
        'matched_data':{
            'file_path': matched_path,
            'count': len(matched_final)
        },
        'suspected_data':{
            'file_path': suspected_path,
            'count': len(suspected)
        },
        'unmatched_source_data':{
            'file_path': unmatched_source_path,
            'count': len(unmatched_source_records)
        },
        'unmatched_target_data':{
            'file_path': unmatched_target_path, 
            'count': len(unmatched_target_records)
        }
    }

    # Debug info to help diagnose issues
    print(f"[DEBUG] Process batch stats - Matched: {len(matched_final)}, Suspected: {len(suspected)}, "
          f"Unmatched Source: {len(unmatched_source_records)}, Unmatched Target: {len(unmatched_target_records)}")

    try:
        BATCH_DATA_PATH = os.path.abspath(os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'db_jsons', 'batch_data.json'))
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
        "message": f"Cross source link process is completed for the batch!",
        "data": result
    }, 200