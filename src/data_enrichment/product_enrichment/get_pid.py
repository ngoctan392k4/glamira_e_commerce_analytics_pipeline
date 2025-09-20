from src.data_enrichment.product_enrichment.save_files import save_pid_url_by_collection
from src.data_enrichment.product_enrichment.yaml_config import load_config
from pymongo import MongoClient
import os
import logging


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

def get_product_id():
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = config["CREDENTIAL"]
    url = config["MONGODB_URL"]

    client = MongoClient(url, serverSelectionTimeoutMS=5)
    database = client["glamira"]
    summary = database["summary"]

    collections = [
        "view_product_detail",
        "select_product_option",
        "select_product_option_quality",
        "add_to_cart_action",
        "product_detail_recommendation_visible",
        "product_detail_recommendation_noticed",
        "product_view_all_recommend_clicked"
    ]

    for col in collections:
        logging.info(f"Processing collection: {col}")
        product_map = {}

        if col == "product_view_all_recommend_clicked":
            pipeline = [
                {"$match": {
                    "collection": col,
                    "viewing_product_id": {"$ne": None},
                    "referrer_url": {"$ne": None}
                }},
                {"$project": {
                    "product_id": "$viewing_product_id",
                    "url": "$referrer_url",
                    "_id": 0
                }}
            ]
            results = summary.aggregate(pipeline, allowDiskUse=True)
        else:
            pipeline_1 = [
                {"$match": {
                    "collection": col,
                    "product_id": {"$ne": None},
                    "current_url": {"$ne": None}
                }},
                {"$project": {
                    "_id": 0,
                    "product_id": "$product_id",
                    "url": "$current_url"
                }}
            ]

            pipeline_2 = [
                {"$match": {
                    "collection": col,
                    "viewing_product_id": {"$ne": None},
                    "current_url": {"$ne": None}
                }},
                {"$project": {
                    "_id": 0,
                    "product_id": "$viewing_product_id",
                    "url": "$current_url"
                }}
            ]

            results = list(summary.aggregate(pipeline_1, allowDiskUse=True)) + list(summary.aggregate(pipeline_2, allowDiskUse=True))

        for doc in results:
            pid = doc.get("product_id")
            url = doc.get("url")
            if not pid or not url:
                continue
            if pid not in product_map:
                product_map[pid] = set()
            product_map[pid].add(url)

        results = [{"product_id": pid, "list_url": list(urls)} for pid, urls in product_map.items()]
        save_pid_url_by_collection(results, col)
