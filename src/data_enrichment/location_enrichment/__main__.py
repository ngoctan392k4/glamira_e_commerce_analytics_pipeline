from src.data_enrichment.location_enrichment.enrich_location import enrich_location_info
from src.data_enrichment.location_enrichment.get_ip import get_ip_address
from src.data_enrichment.location_enrichment.upload import upload_ip_location
import logging
import time

if __name__ == "__main__":
    start_time = time.time()
    try:
        get_ip_address()
    except Exception as e:
        logging.exception("Error when collecting IP address")

    try:
        enrich_location_info("data/ip_address.csv")
    except Exception as e:
        logging.exception("Error when collecting ip location")

    try:
        upload_ip_location()
    except Exception as e:
        logging.exception("Error when uploading ip location data")

    end_time = time.time()
    duration = end_time - start_time
    logging.info(f"Completed in {duration:.2f} seconds")