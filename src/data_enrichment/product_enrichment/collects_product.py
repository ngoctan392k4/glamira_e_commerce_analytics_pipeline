import os
import json
import tldextract
import logging
import random
import aiohttp
import asyncio
from asyncio import Queue
from tqdm.asyncio import tqdm_asyncio
from src.data_enrichment.product_enrichment.get_product_data import collect_product_info
from src.data_enrichment.product_enrichment.save_files import save_product_detail,save_check_point, save_pid_error_jsonl, save_pid_error_403, save_pid_error_404, save_pid_error_retries, save_pid_error_429
from src.data_enrichment.product_enrichment.yaml_config import load_config

config = load_config()

# -----------------------
# Setup logging
# -----------------------

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

# -----------------------
# Supporter function for main crawling
# -----------------------

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

def save_pid_error_batch(errors):
    for err_type, pid, tag, url in errors:
        if err_type == "403":
            save_pid_error_403(pid, tag, url)
        elif err_type == "404":
            save_pid_error_404(pid, tag, url)
        elif err_type == "429":
            save_pid_error_429(pid, tag, url)
        elif err_type == "fail many attempt":
            save_pid_error_retries(pid, tag, url)
        else:
            save_pid_error_jsonl(pid, tag, url)

# -----------------------
# Async function for main crawling
# -----------------------

async def write_sync(func, *args, **kwargs):
    return await asyncio.to_thread(func, *args, **kwargs)

async def error_writer_task(error_queue):
    while True:
        errors = []
        try:
            for _ in range(50):
                item = await asyncio.wait_for(error_queue.get(), timeout=10)
                if item is None:  # The signal that there is no more item
                    if errors:
                        await asyncio.to_thread(save_pid_error_batch, errors)
                    logging.info("Error writer stopped.")
                    return
                errors.append(item)
        except asyncio.TimeoutError:
            pass  # Timeout -> save all errors in the queue

        if errors:
            await asyncio.to_thread(save_pid_error_batch, errors)
            errors.clear()

# -----------------------
# Crawling data
# -----------------------

async def process_tag_product(error_queue, pid, tag, url_list, session):
    for url in url_list:
        logging.info(f"Crawling {pid} with ({tag}) and url {url}")
        data = await collect_product_info(session, url)
        if "product_id" in data:
            return data
        status = data.get("status")

        if status == 404:
            logging.warning(f"The pid {pid} with tag encountered url {url} as 404 error. The pid {pid} in {tag} no longer exists")
            for u in url_list:
                await error_queue.put(("404", pid, tag, u))
            return None

        try:
            if status == 403:
                await error_queue.put(("403", pid, tag, url))
            elif status == 404:
                await error_queue.put(("404", pid, tag, url))
            elif status == 429:
                await error_queue.put(("429", pid, tag, url))
            elif status == "fail many attempt":
                await error_queue.put(("fail many attempt", pid, tag, url))
            elif status == "not load":
                await error_queue.put(("not load", pid, tag, url))
            elif "href" in data:
                await error_queue.put(("href", pid, tag, url))
            else:
                logging.warning("Unknown data format: %s", data)
                await error_queue.put(("Unknown data format", pid, tag, url))
        except Exception:
            logging.exception("Error saving error record for pid=%s tag=%s url=%s", pid, tag, url)

        await asyncio.sleep(random.uniform(0.25, 0.5))
    logging.warning("All URLs failed for PID %s and tag %s", pid, tag)
    return None

async def process_pid(error_queue, item, session):
    pid = item["product_id"]
    urls = item.get("list_url", [])
    result = {"product_id": pid}
    tag_to_urls = {}
    for url in urls:
        tag = extract_tag(url)
        tag_to_urls.setdefault(tag, []).append(url)

    for tag, url_list in tag_to_urls.items():
        data = await process_tag_product(error_queue, pid, tag, url_list, session)
        if data:
            result[tag] = data
    return result if len(result) > 1 else None


# -----------------------
# Main function for crawling
# -----------------------

async def collect_products_data(file_path, concurrency = 20, per_host = 5):

    error_queue = Queue()
    writer_task = asyncio.create_task(error_writer_task(error_queue))

    start_batch = load_checkpoint("checkpoint/checkpoint_crawl_product_data.txt")

    connector = aiohttp.TCPConnector(limit=concurrency, limit_per_host=per_host, ttl_dns_cache=3600)

    async with aiohttp.ClientSession(connector=connector) as session:
        # Semaphore ensures we don't create more active tasks than desired
        sem = asyncio.Semaphore(concurrency)

        async def wrapped_process_pid(item):
            async with sem:
                return await process_pid(error_queue, item, session)

        for batch, batch_num in batch_reader(start_batch, batch_size=10, input_file=file_path):
            parameters = list(batch)
            coros = [wrapped_process_pid(item) for item in parameters]

            results = await tqdm_asyncio.gather(*coros, desc=f"Batch {batch_num}")
            final_results = [result for result in results if result]

            await write_sync(save_product_detail, final_results, batch_num)
            await write_sync(save_check_point, batch_num + 1)

        # Send signal to stop writer
        await error_queue.put(None)
        await writer_task

    logging.info("All batches complete.")
