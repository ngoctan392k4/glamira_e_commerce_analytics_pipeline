from  src.data_enrichment.product_enrichment.yaml_config import load_config
from pymongo import MongoClient
import os
import json
from tqdm import tqdm
import logging

def upload_product_data():
    config = load_config()
    url = config["MONGODB_URL"]

    client = MongoClient(url, serverSelectionTimeoutMS=30000)
    database = client['glamira']
    collection = database['product_info']

    folder_path = "data/product_detail_v2"
    file_list = [f for f in os.listdir(folder_path) if f.endswith(".json")]

    batch = []

    for idx, filename in enumerate(tqdm(file_list, desc="Uploading: ")):
        file_path = os.path.join(folder_path, filename)
        try:
            with open(file_path, "r", encoding="utf-8") as rf:
                data = json.load(rf)
                if isinstance(data, dict):
                    batch.append(data)
                else:
                    logging.warning(f"File {filename} is not a valid JSON object.")
        except Exception as e:
            logging.exception(f"Error when reading file {filename}: {e}")

        if len(batch) >= config["BATCH_SIZE"]:
            try:
                collection.insert_many(batch, ordered=False)
                batch.clear()
            except Exception as e:
                logging.exception(f"Error when insert_many: {e}")
                batch.clear()

    if batch:
        try:
            collection.insert_many(batch, ordered=False)
        except Exception as e:
            logging.exception(f"Error with final insert_many: {e}")

    logging.info("All product data uploaded.")
