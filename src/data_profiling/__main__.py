from src.data_profiling.location_profiling import location_analyze
from src.data_profiling.product_profiling import product_analyze
from yaml_config import load_config
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
    logging.info(f"Start analyzing location and product data")

    try:
        location_analyze()
    except Exception as e:
        logging.exception(f"Error when analyzing location data")

    try:
        product_analyze()
    except Exception as e:
        logging.exception(f"Error when analyzing product data")

    end_time = time.time()
    logging.info(f"Completed in {start_time-end_time:.2f} seconds")