from dotenv import load_dotenv
import os

load_dotenv("src/data_enrichment/product_enrichment_multiprocessing_threading_proxy/.env")

WS_HOST   = os.getenv("WS_HOST")
WS_PORT   = os.getenv("WS_PORT")
WS_USER   = os.getenv("WS_USER")
WS_PASS   = os.getenv("WS_PASS")


def build_rotating_proxy(connection_close=False):
    proxy_url = f"http://{WS_USER}:{WS_PASS}@{WS_HOST}:{WS_PORT}"
    return {"http": proxy_url, "https": proxy_url}
