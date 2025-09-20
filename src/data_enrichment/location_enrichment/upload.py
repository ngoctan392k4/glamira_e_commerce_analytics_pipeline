from src.data_enrichment.location_enrichment.yaml_config import load_config
from pymongo import MongoClient
from tqdm import tqdm
import os
import json
import logging


def upload_ip_location():
    config = load_config()
    url = config["MONGODB_URL"]
    client = MongoClient(url, serverSelectionTimeoutMS=30000)
    database = client['glamira']
    collection = database['ip_location']

    folder_path = "data/location"
    file_list = [f for f in os.listdir(folder_path) if f.endswith(".json")]

    for filename in tqdm(file_list, desc="Uploading:"):
        if filename.endswith(".json"):
            file_path = os.path.join(folder_path, filename)
            with open(file_path, "r", encoding="utf-8") as rf:
                try:
                    data = json.load(rf)
                    if isinstance(data, list):
                        result = collection.insert_many(data)
                    else:
                        logging.exception(f"File {filename} does not contain JSON")
                except Exception as e:
                    logging.exception(f"Error when reading file {filename}: {e}")

    logging.info(f"Have uploaded ip location into the new collection")
