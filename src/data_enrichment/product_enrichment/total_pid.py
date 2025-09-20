import os
import json
from collections import defaultdict
import logging
import tldextract

def in_total_pid():
    folder_path = "data/pid_by_collection"

    product_data = defaultdict(set)

    for file in os.listdir(folder_path):
        if file.endswith(".json"):
            file_path = os.path.join(folder_path, file)
            with open(file_path, "r", encoding="utf-8") as rf:
                try:
                    data = json.load(rf)
                    for item in data:
                        pid = item.get("product_id")
                        urls = item.get("list_url", [])
                        for url in urls:
                            ext = tldextract.extract(url)
                            subdomain = ext.subdomain
                            if "checkout" in url or (subdomain and subdomain != "www"):
                                continue
                            product_data[pid].add(url)
                except json.JSONDecodeError as e:
                    logging.exception(f"Cannot load JSON with file {file_path}")

    total_data = [
        {"product_id": pid, "list_url": sorted(list(urls))}
        for pid, urls in product_data.items()
    ]

    output_path = "data/pid_url_in_total.json"
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(total_data, f, indent=2, ensure_ascii=False)

    logging.info(f"Have merged {len(total_data)} into {output_path}")
