import time
import logging
from src.bigquery.raw_layer_uploader.yaml_config import load_config
from src.bigquery.raw_layer_uploader.schema_dict import create_schema
from src.bigquery.raw_layer_uploader.column_analyzer import find_fields
from src.bigquery.raw_layer_uploader.gcs_to_bigquery import normalize, upload_gcs, build_schema, create_dataset_table, load_bigquery

if __name__ == "__main__":
    start_time = time.time()

    #---------------------
    # Set up Logging
    #---------------------
    config = load_config()
    log_cf = config.get("LOGGING", {})
    logging.basicConfig(
        level=getattr(logging, log_cf.get("level","INFO").upper(), logging.INFO),
        format='%(asctime)s [%(levelname)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        handlers=[
            logging.FileHandler(log_cf["log_file"], encoding='utf-8'),
            logging.StreamHandler() if log_cf.get("to_console", False) else logging.NullHandler()
        ]

    )

    #---------------------
    # Analyze column
    #---------------------
    logging.info(f"Analyzing column for raw data")
    find_fields(input_file="data/raw_data.jsonl", output_dir="src/bigquery/raw_layer_uploader/fields", output_file="field_types_raw.csv")

    logging.info(f"Analyzing column for product data")
    find_fields(input_file="data/product_info.jsonl", output_dir="src/bigquery/raw_layer_uploader/fields", output_file="field_types_product.csv")

    logging.info(f"Analyzing column for ip2location data")
    find_fields(input_file="data/ip_location_data.jsonl", output_dir="src/bigquery/raw_layer_uploader/fields", output_file="field_types_location.csv")

    #---------------------
    # Upload from GCS to Bigquery
    #---------------------
    #  Create schema file
    create_schema("src/bigquery/raw_layer_uploader/fields/field_types_raw.csv", "src/bigquery/raw_layer_uploader/schema/bigquery_schema_raw.json")
    create_schema("src/bigquery/raw_layer_uploader/fields/field_types_product.csv", "src/bigquery/raw_layer_uploader/schema/bigquery_schema_product.json")
    create_schema("src/bigquery/raw_layer_uploader/fields/field_types_location.csv", "src/bigquery/raw_layer_uploader/schema/bigquery_schema_location.json")


    logging.info(f"Uploading raw data")
    logging.info(f"Pre-processing raw data")
    normalize(local_input_file="data/raw_data.jsonl", local_fixed_file="data/raw_data_fixed.jsonl")
    logging.info(f"Upload fixed file to GCS")
    upload_gcs(project_id="glamira-project-464503", gcs_bucket="glamira-project", gcs_object="raw_data_fixed.jsonl", local_fixed_file="data/raw_data_fixed.jsonl")
    logging.info(f"Building schema")
    schema = build_schema(schema_file="src/bigquery/raw_layer_uploader/schema/bigquery_schema_raw.json")
    logging.info(f"Creating dataset and table")
    create_dataset_table(schema=schema, project_id="glamira-project-464503", dataset_id="raw_glamira", table_id="raw_glamira_behaviour")
    logging.info(f"Loading to bigquery")
    load_bigquery(schema=schema, project_id="glamira-project-464503", dataset_id="raw_glamira", table_id="raw_glamira_behaviour", gcs_bucket="glamira-project", gcs_object="raw_data_fixed.jsonl")

    logging.info(f"Uploading product data")
    logging.info(f"Building schema")
    schema = build_schema(schema_file="src/bigquery/raw_layer_uploader/schema/bigquery_schema_product.json")
    logging.info(f"Creating dataset and table")
    create_dataset_table(schema=schema, project_id="glamira-project-464503", dataset_id="raw_glamira", table_id="raw_product_data")
    logging.info(f"Loading to bigquery")
    load_bigquery(schema=schema, project_id="glamira-project-464503", dataset_id="raw_glamira", table_id="raw_product_data", gcs_bucket="glamira-project", gcs_object="product_info.jsonl")

    logging.info(f"Uploading location data")
    logging.info(f"Building schema")
    schema = build_schema(schema_file="src/bigquery/raw_layer_uploader/schema/bigquery_schema_location.json")
    logging.info(f"Creating dataset and table")
    create_dataset_table(schema=schema, project_id="glamira-project-464503", dataset_id="raw_glamira", table_id="raw_ip_location")
    logging.info(f"Loading to bigquery")
    load_bigquery(schema=schema, project_id="glamira-project-464503", dataset_id="raw_glamira", table_id="raw_ip_location", gcs_bucket="glamira-project", gcs_object="ip_location_data.jsonl")

    end_time = time.time()
