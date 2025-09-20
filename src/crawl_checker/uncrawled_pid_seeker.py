import os
import json
import csv
from tqdm import tqdm

def invalid_product_id():
    files = os.listdir("data/product_detail")
    crawl_pid = set()
    origin_pid = set()

    for file in tqdm(files, total=len(files)):
        with open(os.path.join("data/product_detail", file), "r", encoding='utf-8') as rf:
            data = json.load(rf)
            for item in data:
                crawl_pid.add(item["product_id"])
    with open ("crawled_pid.csv", "w", encoding='utf-8', newline="") as wf:
        writer = csv.writer(wf)
        writer.writerow(["product_id"])
        for pid in crawl_pid:
            writer.writerow([pid])

    with open("data/pid_url_in_total.json", "r", encoding='utf-8') as rf:
            data = json.load(rf)
            for item in tqdm(data, total=len(data)):
                origin_pid.add(item["product_id"])

    with open ("origin_pid.csv", "w", encoding='utf-8', newline="") as wf:
        writer = csv.writer(wf)
        writer.writerow(["product_id"])
        for pid in origin_pid:
            writer.writerow([pid])

    difference_pid = origin_pid - crawl_pid
    with open ("uncrawled_pid.csv", "w", encoding='utf-8', newline="") as wf:
        writer = csv.writer(wf)
        writer.writerow(["product_id"])
        for pid in difference_pid:
            writer.writerow([pid])

    print(len(origin_pid))
    print(len(crawl_pid))
    print(len(difference_pid))

invalid_product_id()