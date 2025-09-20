from src.upload_gcs.download_ip2location_data import download_location
from src.upload_gcs.download_product_data import download_product
from src.upload_gcs.download_raw import download_raw
from src.upload_gcs.yaml_config import load_config
from src.upload_gcs.upload import upload_objects
import logging
import time


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

if __name__ == "__main__":
    start_time = time.time()
    logging.info(f"Start downloading raw, ip2location, production data from MongoDB")

    try:
        download_raw()
    except Exception as e:
        logging.exception(f"Error when downloading raw data")

    try:
        download_location()
    except Exception as e:
        logging.exception(f"Error when downloading location data")

    try:
        download_product()
    except Exception as e:
        logging.exception(f"Error when downloading product data")

    logging.info(f"Start uploading raw, ip2location, production data into GCS")
    try:
        upload_objects("glamira-project", "data/ip_location_data.jsonl", "ip_location_data.jsonl")
        upload_objects("glamira-project", "data/product_data.jsonl", "product_data.jsonl")
        upload_objects("glamira-project", "data/raw_data.jsonl", "raw_data.jsonl")
    except Exception as e:
        logging.exception(f"Error when uploading data")

    end_time = time.time()
    logging.info(f"Completed in {start_time-end_time:.2f} seconds")
