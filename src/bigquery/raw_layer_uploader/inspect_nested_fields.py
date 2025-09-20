import json
from collections import defaultdict, Counter
from pathlib import Path
import time

# Config
input_file = Path("data/product_info.jsonl")
max_lines = None  # Set number of lines that need to check, None to check all

# Metrics
field_types = defaultdict(Counter)
nested_fields = defaultdict(set)

# Progress tracking
start_time = time.time()
print_interval = 100000
line_count = 0

print(f"[Start] Reading: {input_file}")

with input_file.open('r', encoding='utf-8') as f:
    for line in len(f):
        line_count += 1
        try:
            record = json.loads(line.strip())
        except json.JSONDecodeError as e:
            print(f"[Warning] Failed to parse line {line_count}: {e}")
            continue

        for key, value in record.items():
            type_name = type(value).__name__
            field_types[key][type_name] += 1

            # Check nested
            if isinstance(value, dict):
                nested_fields[key].update(value.keys())
            elif isinstance(value, list):
                nested_fields[key].add("[list]")

        if line_count % print_interval == 0:
            elapsed = time.time() - start_time
            print(f"[Progress] Processed {line_count} lines in {elapsed:.2f}s")

        if max_lines and line_count >= max_lines:
            break

#  Reporting
print("\n SUMMARY ")
print(f"Total lines processed: {line_count}")
print("Detected field types:")
for field, types in field_types.items():
    print(f"  - {field}: {dict(types)}")

print("\nNested fields detected:")
for field, subfields in nested_fields.items():
    print(f"  - {field}: nested into {len(subfields)} fields → {list(subfields)}")

print("\n[Done] Runtime: {:.2f}s".format(time.time() - start_time))
