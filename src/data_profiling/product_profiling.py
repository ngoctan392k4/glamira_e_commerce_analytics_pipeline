import os
from pymongo import MongoClient
import csv
from tqdm import tqdm
from src.data_profiling.yaml_config import load_config
config = load_config()

def product_analyze():
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = config["CREDENTIAL"]
    client = MongoClient(config["MONGODB_URL"], serverSelectionTimeoutMS=5)
    database = client["glamira"]
    collection = database["product_data"]

    # Find number of domain - region has glamira store
    domain = set()
    for doc in tqdm(collection.find(), total=collection.count_documents({})):
        keys = list(doc.keys())[2:]
        domain.update(keys)
    with open("distinct_domain.csv", "w", encoding='utf-8', newline="") as wf:
        writer = csv.writer(wf)
        writer.writerow(["domain_name"])
        for dom in domain:
            writer.writerow([dom])

    fields = set()
    for doc in tqdm(collection.find(), total=collection.count_documents({})):
        key = list(doc.keys())[2]
        sub_keys = list(doc.get(key, {}).keys())
        fields.update(sub_keys)

    with open ("product_field.csv", "w", encoding="utf-8", newline="") as wf:
        writer = csv.writer(wf)
        writer.writerow(["product_field"])
        for field in fields:
            writer.writerow([field])
