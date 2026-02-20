#!/usr/bin/env python3
"""
check_provenance.py

Usage:
  python check_provenance.py [--repo ROOT] [--report OUT.json]

Runs checks described in the issue:
 - For each request/*.csv file, ensures there is one provenance JSON.
 - Ensures all CSV 'raw' variables are described in the corresponding JSON.

Outputs a summary and per-file results to stdout and optionally writes a JSON report.
"""
import argparse
import csv
import json
import os
import sys
from glob import glob

def find_csvs(repo_root):
    request_dir = os.path.join(repo_root, "request")
    if not os.path.isdir(request_dir):
        # fallback to requests
        request_dir = os.path.join(repo_root, "requests")
    patterns = [os.path.join(request_dir, "*.csv"), os.path.join(request_dir, "**", "*.csv")]
    csvs = []
    for p in patterns:
        csvs.extend(glob(p, recursive=True))
    return sorted(set(csvs))

def find_json_candidates(repo_root):
    patterns = [os.path.join(repo_root, "**", "*.json")]
    js = []
    for p in patterns:
        js.extend(glob(p, recursive=True))
    return sorted(set(js))

def read_csv_header(path):
    try:
        with open(path, newline='', encoding='utf-8') as f:
            reader = csv.reader(f)
            for row in reader:
                # first non-empty row assumed header
                if row:
                    return [c.strip() for c in row]
    except Exception as e:
        return None
    return None

def extract_raw_vars_from_json(obj):
    """
    Try multiple heuristics to locate raw variable descriptions inside the provenance json:
      - top-level key containing 'raw' (case-insensitive)
      - top-level key containing 'variables' and possibly nested under 'input' or 'raw'
    The returned structure is a set of variable names and a map name->description when available.
    """
    candidates = {}
    def collect_from_value(val):
        names = {}
        if isinstance(val, list):
            for item in val:
                if isinstance(item, str):
                    names[item] = ""
                elif isinstance(item, dict):
                    # try common keys
                    name = None
                    desc = ""
                    for k in ("name", "variable", "id", "var"):
                        if k in item and isinstance(item[k], str):
                            name = item[k]
                            break
                    if not name:
                        # maybe the dict is { varname: description }
                        if len(item)==1:
                            k0 = next(iter(item))
                            if isinstance(item[k0], str):
                                name = k0
                                desc = item[k0]
                    # description fields
                    for k in ("description", "desc", "label", "long_name", "comment"):
                        if k in item and isinstance(item[k], str):
                            desc = item[k]
                            break
                    if name:
                        names[name] = desc
        elif isinstance(val, dict):
            # treat keys as variable names if the values are strings or dicts with description
            for k, v in val.items():
                if isinstance(v, str):
                    names[k] = v
                elif isinstance(v, dict):
                    desc = ""
                    for dk in ("description", "desc", "label", "long_name", "comment"):
                        if dk in v and isinstance(v[dk], str):
                            desc = v[dk]
                            break
                    names[k] = desc
        return names

    # 1. top-level keys containing 'raw'
    for k, v in obj.items() if isinstance(obj, dict) else []:
        if "raw" in k.lower():
            candidates.update(collect_from_value(v))

    # 2. top-level keys containing 'variable' or 'variables'
    for k, v in obj.items() if isinstance(obj, dict) else []:
        if "variable" in k.lower():
            candidates.update(collect_from_value(v))

    # 3. keys like 'input', 'inputs' might contain raw variables
    for k, v in obj.items() if isinstance(obj, dict) else []:
        if k.lower() in ("input", "inputs", "data", "raw_data"):
            candidates.update(collect_from_value(v))

    # 4. fallback: scan entire json for lists/dicts that look like variable lists
    def scan(value):
        found = {}
        if isinstance(value, list):
            # if list of small dicts with 'name' or 'variable' keys, collect
            for item in value:
                if isinstance(item, dict) and any(x in item for x in ("name","variable","id")):
                    found.update(collect_from_value([item]))
        elif isinstance(value, dict):
            for v in value.values():
                found.update(scan(v))
        return found

    if not candidates:
        candidates.update(scan(obj))

    return candidates  # dict name->description (may be empty string)

def match_provenance_for_dataset(dataset_name, json_files):
    # Prefer exact filename match dataset_name.json
    exact = []
    contains = []
    for j in json_files:
        b = os.path.splitext(os.path.basename(j))[0]
        if b == dataset_name:
            exact.append(j)
        elif dataset_name.lower() in b.lower():
            contains.append(j)
    if exact:
        return exact  # return list (often length 1)
    return contains

def main():
    parser = argparse.ArgumentParser(description="Check provenance JSONs correspond to request CSVs")
    parser.add_argument("--repo", "-r", default=".", help="Path to repository root (default: .)")
    parser.add_argument("--report", help="Optional path to write JSON report")
    args = parser.parse_args()

    repo_root = os.path.abspath(args.repo)
    csvs = find_csvs(repo_root)
    json_files = find_json_candidates(repo_root)

    if not csvs:
        print("No CSV files found in request/ (or requests/). Exiting.", file=sys.stderr)
        sys.exit(2)

    print(f"Found {len(csvs)} CSV(s) in request/ and {len(json_files)} JSON file(s) in repo.\n")

    results = []
    total_missing_json = 0
    total_missing_vars = 0

    for csvpath in csvs:
        dataset_name = os.path.splitext(os.path.basename(csvpath))[0]
        candidates = match_provenance_for_dataset(dataset_name, json_files)
        entry = {"dataset": dataset_name, "csv": os.path.relpath(csvpath, repo_root), "json_candidates": [os.path.relpath(c, repo_root) for c in candidates], "status": "ok", "missing_json": False, "csv_vars_not_described": [], "json_vars_not_in_csv": []}
        if not candidates:
            entry["status"] = "missing_provenance"
            entry["missing_json"] = True
            total_missing_json += 1
            results.append(entry)
            print(f"[MISSING PROV] No provenance JSON found for dataset '{dataset_name}' (csv: {entry['csv']}).")
            continue

        # pick best candidate (prefer exact match)
        candidate = candidates[0]

        # read csv header
        header = read_csv_header(csvpath)
        if header is None:
            entry["status"] = "csv_header_error"
            results.append(entry)
            print(f"[ERROR] Could not read header for CSV {entry['csv']}")
            continue

        # read json
        try:
            with open(candidate, "r", encoding="utf-8") as f:
                jobj = json.load(f)
        except Exception as e:
            entry["status"] = "json_load_error"
            entry["json_error"] = str(e)
            results.append(entry)
            print(f"[ERROR] Could not parse JSON {candidate}: {e}")
            continue

        raw_desc = extract_raw_vars_from_json(jobj)
        json_var_names = set(raw_desc.keys())
        csv_var_names = set(header)

        # variables in csv that are not described in json
        missing_in_json = sorted([v for v in csv_var_names if v not in json_var_names])
        # variables described in json but not present in csv
        missing_in_csv = sorted([v for v in json_var_names if v not in csv_var_names])

        entry["json"] = os.path.relpath(candidate, repo_root)
        entry["csv_vars"] = sorted(list(csv_var_names))
        entry["json_vars"] = sorted(list(json_var_names))
        entry["csv_vars_not_described"] = missing_in_json
        entry["json_vars_not_in_csv"] = missing_in_csv

        if missing_in_json:
            entry["status"] = "vars_missing_in_json"
            total_missing_vars += len(missing_in_json)
            print(f"[MISMATCH] Dataset '{dataset_name}': {len(missing_in_json)} CSV variable(s) missing in JSON description: {missing_in_json}")
        if missing_in_csv:
            entry["status"] = entry["status"] if entry["status"]!="ok" else "extra_vars_in_json"
            print(f"[INFO] Dataset '{dataset_name}': {len(missing_in_csv)} JSON variable(s) not found in CSV: {missing_in_csv}")

        if entry["status"] == "ok":
            print(f"[OK] Dataset '{dataset_name}': provenance JSON found and variables match (csv: {entry['csv']}, json: {entry['json']})")

        results.append(entry)

    summary = {
        "repo_root": repo_root,
        "datasets_checked": len(csvs),
        "missing_provenance_files": total_missing_json,
        "csv_vars_undocumented_total": total_missing_vars
    }

    print("\nSummary:")
    print(json.dumps(summary, indent=2))

    if args.report:
        out = {"summary": summary, "results": results}
        with open(args.report, "w", encoding="utf-8") as f:
            json.dump(out, f, indent=2)
        print(f"Wrote report to {args.report}")

if __name__ == "__main__":
    main()