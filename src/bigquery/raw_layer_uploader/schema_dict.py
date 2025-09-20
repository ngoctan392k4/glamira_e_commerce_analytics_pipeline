import pandas as pd
import json

# Map datatype in CSV to BigQuery
type_mapping = {
    "str": "STRING",
    "int": "INT64",
    "float": "FLOAT64",
    "bool": "BOOL",
    "list": "ARRAY",
    "dict": "RECORD",
    "datetime": "TIMESTAMP"
}

def create_schema(csv_path, output_json_path):
    df = pd.read_csv(csv_path)

    # Use nested dict to build schema
    schema_tree = {}

    for _, row in df.iterrows():
        parts = row["Field"].strip().split(".")
        bq_type = type_mapping.get(row["Type"].strip(), "STRING")
        node = schema_tree

        for i, part in enumerate(parts):
            if part not in node:
                node[part] = {"__type__": None, "__children__": {}}
            if i == len(parts) - 1:  # field cuối
                node[part]["__type__"] = bq_type
            node = node[part]["__children__"]

    # Recursive function to transform to format BigQuery
    def build_schema(tree):
        schema_list = []
        for name, meta in tree.items():
            children = meta["__children__"]
            if children:
                schema_list.append({
                    "name": name,
                    "type": "RECORD",
                    "mode": "NULLABLE",
                    "fields": build_schema(children)
                })
            else:
                schema_list.append({
                    "name": name,
                    "type": meta["__type__"],
                    "mode": "NULLABLE"
                })
        return schema_list

    schema_json = build_schema(schema_tree)

    with open(output_json_path, "w", encoding="utf-8") as f:
        json.dump(schema_json, f, ensure_ascii=False, indent=2)

