from src.data_enrichment.location_enrichment.save_files import save_ip_address
from src.data_enrichment.location_enrichment.yaml_config import load_config
from pymongo import MongoClient
import os

def get_ip_address():
    config = load_config()
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = config["CREDENTIAL"]
    url = config["MONGODB_URL"]

    client = MongoClient(url, serverSelectionTimeoutMS=5)
    database = client["glamira"]
    summary = database["summary"]

    pipeline = [
        {"$group": {"_id": "$ip"}},
        {"$project": {"ip": "$_id", "_id": 0}}
    ]
    cursor = summary.aggregate(pipeline, allowDiskUse=True)

    ip_data = [doc["ip"] for doc in cursor if doc.get("ip")]

    save_ip_address(ip_data)