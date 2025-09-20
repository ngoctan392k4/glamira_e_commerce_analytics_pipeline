from src.data_enrichment.product_enrichment.get_pid import get_product_id
from src.data_enrichment.product_enrichment.total_pid import in_total_pid
from src.data_enrichment.product_enrichment.collects_product import collect_products_data
from src.data_enrichment.product_enrichment.upload import upload_product_data
from src.data_enrichment.product_enrichment.split_data import split
from src.data_enrichment.product_enrichment.processing_product_data import re_process_product_data
import time
import logging
import asyncio

if __name__ == "__main__":
    start_time = time.time()
    logging.info(f"Starting crawling product data to enrich data")

    try:
        get_product_id()
    except Exception as e:
        logging.exception(f"Error when collecting product id")

    try:
        in_total_pid()
    except Exception as e:
        logging.exception(f"Error when merging product id and urls")

    try:
        asyncio.run(collect_products_data("data/pid_url_in_total.json"))
    except Exception as e:
        logging.exception(f"Error when crawling product information")


    try:
        split()
    except Exception as e:
        logging.exception(f"Error when splitting product information")

    try:
        upload_product_data()
    except Exception as e:
        logging.exception(f"Error when uploading product information into MongoDB")

    try:
        re_process_product_data()
    except Exception as e:
        logging.exception(f"Error when re processing product information")

    end_time = time.time()
    logging.info(f"Completed product enrichment in {end_time - start_time:.2f} seconds")