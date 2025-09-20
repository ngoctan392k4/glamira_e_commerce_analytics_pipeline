from google.cloud import storage
import os
from src.upload_gcs.yaml_config import load_config

config = load_config()

def upload_objects(bucket_name, file_name_path, destination_blob_name):
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = config["CREDENTIAL"]
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(file_name_path)
    print("Completed")

