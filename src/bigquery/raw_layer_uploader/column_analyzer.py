import os
import json
from collections import defaultdict, Counter
from pathlib import Path
import time
import csv
import logging
from src.bigquery.raw_layer_uploader.yaml_config import load_config

config = load_config()

#  Config
max_lines = None  # None means check all

#  Metrics
field_types = defaultdict(Counter)
nested_fields = defaultdict(set)

#  Recursive function to analyze nested
def analyze_field(prefix, value):
    # Analyze values and update field_types, nested_fields
    type_name = type(value).__name__
    field_types[prefix][type_name] += 1

    if isinstance(value, dict):
        for sub_key, sub_val in value.items():
            nested_fields[prefix].add(sub_key)
            new_prefix = f"{prefix}.{sub_key}"
            analyze_field(new_prefix, sub_val)
    elif isinstance(value, list):
        nested_fields[prefix].add("[list]")
        for i, item in enumerate(value):
            new_prefix = f"{prefix}[]"
            analyze_field(new_prefix, item)

#  Progress tracking
def find_fields(input_file, output_dir, output_file):
    start_time = time.time()
    print_interval = 100000
    line_count = 0

    logging.info(f"[Start] Reading: {input_file}")

    with open(input_file, 'r', encoding='utf-8') as f:
        for line in f:
            line_count += 1
            try:
                record = json.loads(line.strip())
            except json.JSONDecodeError as e:
                logging.info(f"[Warning] Failed to parse line {line_count}: {e}")
                continue

            for key, value in record.items():
                analyze_field(key, value)

            if line_count % print_interval == 0:
                elapsed = time.time() - start_time
                logging.info(f"[Progress] {line_count} lines processed in {elapsed:.2f}s")

            if max_lines and line_count >= max_lines:
                break

    #  Save results to JSON
    os.makedirs(output_dir, exist_ok=True)

    #  Save to CSV
    output_path = f"{output_dir}/{output_file}"
    with open(output_path, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["Field", "Type", "Count"])
        for field, type_counts in field_types.items():
            for type_name, count in type_counts.items():
                writer.writerow([field, type_name, count])

