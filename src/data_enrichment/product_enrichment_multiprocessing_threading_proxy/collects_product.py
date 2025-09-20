from src.data_enrichment.product_enrichment_multiprocessing_threading_proxy.get_product_data import collect_product_info
from src.data_enrichment.product_enrichment_multiprocessing_threading_proxy.save_files import save_product_detail,save_check_point, save_pid_error_jsonl, save_pid_error_403, save_pid_error_404, save_pid_error_retries
from src.data_enrichment.product_enrichment_multiprocessing_threading_proxy.yaml_config import load_config
from src.data_enrichment.product_enrichment_multiprocessing_threading_proxy.proxy_config import build_rotating_proxy
from concurrent.futures import ThreadPoolExecutor, as_completed
from multiprocessing import Pool
from tqdm import tqdm
import os
import json
import tldextract
import logging
import time
import random

config = load_config()
log_cf = config.get("LOGGING", {})

# ----------------
# Logging setup
# ----------------

logging.basicConfig(
    level=getattr(logging, log_cf.get("level","INFO").upper(), logging.INFO),
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.FileHandler(log_cf["log_file"], encoding='utf-8'),
        logging.StreamHandler() if log_cf.get("to_console", False) else logging.NullHandler()
    ]

)

# ----------------
# Supporter function
# ----------------

def extract_tag(url):
    ext = tldextract.extract(url)
    return ext.suffix

def read_file(file):
    product_ids = []
    with open(file, "r", encoding="utf-8") as rf:
        product_ids = json.load(rf)
    logging.info(f"Have read {len(product_ids)} PID from {file}")
    return product_ids


def load_checkpoint(path):
    if not os.path.exists(path):
        logging.info(f"Start batch now is 1 with {path}")
        return 1

    with open(path, "r", encoding='utf-8') as rf:
        content = rf.read().strip()
        if content:
            logging.info(f"Start batch now is {int(content)} with file {path}")
            return int(content)
        else:
            logging.info(f"Start batch now is 1 with {path}")
            return 1


def batch_reader(start_batch, batch_size, input_file):
    start_index = (start_batch-1) * batch_size
    batch_num = start_batch

    pids = read_file(input_file)

    for i in range(start_index, len(pids), batch_size):
        batch = pids[i:i+batch_size]
        yield batch, batch_num
        batch_num+=1


# ------------------------------
# Process a single tag
# ------------------------------

def crawl_product_data(pid, tag, url):
    logging.info(f"Crawling {pid} with ({tag}) and url {url} via proxy")
    for attempt in range(3):
        try:
            data = collect_product_info(url)
            if isinstance(data, dict):
                if "product_id" in data:
                    return data
                elif "status" in data and data["status"] == 403:
                    if attempt < 2:
                        logging.warning(f"{pid} ({tag}) got 403 code on attempt {attempt + 1}/3 fetching again")
                        continue
                    else:
                        logging.error(f"{pid} ({tag}) got 403 after 3 attempts.")
                        save_pid_error_403(pid, tag, url)
                        break
                elif "status" in data and data["status"] == 404:
                    save_pid_error_404(pid, tag, url)
                    break
                elif "status" in data and data["status"] == "fail many attempt":
                    save_pid_error_retries(pid, tag, url)
                    break
                elif "status" in data and data["status"] == "not load":
                    if attempt < 2:
                        logging.warning(f"{pid} ({tag}) not load attempt {attempt + 1}/3; fetching again")
                        continue
                    else:
                        logging.error(f"{pid} ({tag}) not load after 3 attempts.")
                        save_pid_error_jsonl(pid, tag, url)
                        break
                elif "href" in data:
                    save_pid_error_jsonl(pid, tag, url)
                    break
                else:
                    logging.warning(f"Unknown data format: {data}")
                    save_pid_error_jsonl(pid, tag, url)
            else:
                logging.warning(f"Data is not a dict: {data}")
                save_pid_error_jsonl(pid, tag, url)
                break

        except Exception as e:
            logging.exception(f"Error with {pid} and domain {tag}: {e}")
            if attempt < 2:
                continue
            else:
                save_pid_error_jsonl(pid, tag, url)
                break

    return None

# ------------------------------------------------------------------
# Process a single tag using multithreading across URLs
# ------------------------------------------------------------------
def process_tag_product(pid, tag, url_list):
    if not url_list:
        return None

    with ThreadPoolExecutor(max_workers=10) as executor:
        future_to_url = {
            executor.submit(crawl_product_data, pid, tag, url): url for url in url_list
        }
        for future in as_completed(future_to_url):
            data_ret = future.result()

            if data_ret:
                for f in future_to_url:
                    if not f.done():
                        f.cancel()
                return data_ret

    logging.warning(f"All URLs failed for PID={pid}, tag={tag}")
    return None


# ------------------------------------------------------------------
# Process a single product (runs in process)
# ------------------------------------------------------------------

def process_pid(item):
    pid = item["product_id"]
    urls = item.get("list_url", [])
    result = {"product_id": pid}
    tag_to_urls = {}

    for url in urls:
        tag = extract_tag(url)
        tag_to_urls.setdefault(tag, []).append(url)

    for tag, url_list in tag_to_urls.items():
        data = process_tag_product(pid, tag, url_list)
        if data:
            result[tag] = data
            time.sleep(random.uniform(0.1, 0.3))

    return result if len(result) > 1 else None


def collect_products_data(file_path):
    start_batch = load_checkpoint(f"checkpoint/checkpoint_crawl_product_data.txt")

    for batch, batch_num in batch_reader(start_batch, batch_size=10, input_file=file_path):
        with Pool(processes=10) as pool:
            parameters = [item for item in batch]
            results = list(tqdm(pool.imap_unordered(process_pid, parameters), total=len(batch), desc=f"Batch {batch_num}"))

        all_data = [res for res in results if res]

        save_product_detail(all_data, batch_num)
        save_check_point(batch_num + 1)
