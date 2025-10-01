import json
import os
import logging
from google.cloud import bigquery
from google.cloud import storage
from tqdm import tqdm
from src.bigquery.raw_layer_uploader.yaml_config import load_config

config = load_config()

#  Config

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = config["CREDENTIAL"]

#  Step 1: Normalize repeated field
def normalize_repeated_field(row):
    obj = row.get("cart_products", [])

    last_key = "option"
    for item in obj:
        if last_key in item:
            val = item[last_key]
            if not isinstance(val, list):
                val = [val]

            new_list = []
            for v in val:
                if isinstance(v, dict):
                    new_list.append({
                        "option_label": v.get("option_label"),
                        "option_id": v.get("option_id"),
                        "value_label": v.get("value_label"),
                        "value_id": v.get("value_id")
                    })
                else:
                    new_list.append({
                        "option_label": None,
                        "option_id": None,
                        "value_label": str(v) if v is not None else None,
                        "value_id": None
                    })
            item[last_key] = new_list


def normalize(local_input_file, local_fixed_file):
    with open(local_input_file, "r", encoding="utf-8") as fin, \
        open(local_fixed_file, "w", encoding="utf-8") as fout:
        for line in tqdm(fin):
            row = json.loads(line)
            normalize_repeated_field(row)
            fout.write(json.dumps(row, ensure_ascii=False) + "\n")

#  Step 2: Upload fixed file to GCS
def upload_gcs(project_id, gcs_bucket, gcs_object, local_fixed_file):
    logging.info("Uploading to GCS...")
    storage_client = storage.Client(project=project_id)
    bucket = storage_client.bucket(gcs_bucket)
    blob = bucket.blob(gcs_object)
    blob.upload_from_filename(local_fixed_file)
    GCS_URI = f"gs://{gcs_bucket}/{gcs_object}"
    logging.info(f"Uploaded to {GCS_URI}")

#  Step 3: Load schema
# def build_schema(schema_file):
#     with open(schema_file, "r", encoding="utf-8") as f:
#         fields = json.load(f)

#     schema = []
#     for field in fields:
#         schema.append(
#             bigquery.SchemaField(
#                 name=field["name"],
#                 field_type=field["type"],
#                 mode=field.get("mode", "NULLABLE"),
#                 description=field.get("description"),
#                 fields=build_schema(field.get("fields", []))
#             )
#         )
#     return schema

def build_schema(schema_file):
    # Nếu là string (đường dẫn file) thì load file JSON
    if isinstance(schema_file, str):
        with open(schema_file, "r", encoding="utf-8") as f:
            fields = json.load(f)
    # Nếu là list (nested fields) thì xử lý trực tiếp
    elif isinstance(schema_file, list):
        fields = schema_file
    else:
        raise TypeError("schema_file must be a filepath (str) or list of fields")

    schema = []
    for field in fields:
        schema.append(
            bigquery.SchemaField(
                name=field["name"],
                field_type=field["type"],
                mode=field.get("mode", "NULLABLE"),
                description=field.get("description"),
                fields=build_schema(field["fields"]) if "fields" in field else ()
            )
        )
    return schema

#  Step 4: Create dataset & table if not exist
def create_dataset_table(schema, project_id, dataset_id, table_id):
    bq_client = bigquery.Client(project=project_id)

    dataset_ref = bigquery.Dataset(f"{project_id}.{dataset_id}")
    try:
        bq_client.get_dataset(dataset_ref)
        logging.info(f"Dataset `{dataset_id}` already exists")
    except Exception:
        bq_client.create_dataset(dataset_ref)
        logging.info(f"Created dataset `{dataset_id}`")

    table_ref = f"{project_id}.{dataset_id}.{table_id}"
    try:
        bq_client.get_table(table_ref)
        logging.info(f"Table `{table_id}` already exists")
    except Exception:
        table = bigquery.Table(table_ref, schema=schema)
        bq_client.create_table(table)
        logging.info(f"Created table `{table_id}`")

#  Step 5: Load to BigQuery
def load_bigquery(schema, project_id, dataset_id, table_id, gcs_bucket, gcs_object):
    bq_client = bigquery.Client(project=project_id)
    table_ref = f"{project_id}.{dataset_id}.{table_id}"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND
    )

    logging.info("Loading data from GCS to BigQuery...")
    load_job = bq_client.load_table_from_uri(f"gs://{gcs_bucket}/{gcs_object}", table_ref, job_config=job_config)
    load_job.result()
    logging.info(f"Loaded data into `{table_ref}`")
