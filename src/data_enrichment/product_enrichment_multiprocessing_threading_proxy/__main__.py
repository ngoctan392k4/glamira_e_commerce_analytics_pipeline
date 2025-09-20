from src.data_enrichment.product_enrichment_multiprocessing_threading_proxy.get_pid import get_product_id
from src.data_enrichment.product_enrichment_multiprocessing_threading_proxy.total_pid import in_total_pid
from src.data_enrichment.product_enrichment_multiprocessing_threading_proxy.collects_product import collect_products_data
import time
import os
import logging

if __name__ == "__main__":
    start_time = time.time()
    logging.info(f"Starting crawling product data to enrich data")
    try:
        get_product_id()
        in_total_pid()
    except Exception as e:
        logging.exception("Error when collecting product_id:", e)

    try:
        collect_products_data("data/pid_url_in_total.json")
    except Exception as e:
        logging.exception("Error when collecting product data:", e)

    end_time = time.time()
    logging.info(f"Completed product enrichment in {end_time - start_time:.2f} seconds")
