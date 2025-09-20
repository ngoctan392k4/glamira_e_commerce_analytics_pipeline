import json
import os
import logging
import csv

def save_location(data, batch_num):
    os.makedirs("data/location", exist_ok=True)
    with open (f"data/location/loc_batch_{batch_num}.json", "w", encoding='utf-8') as wf:
        json.dump(data, wf, ensure_ascii=False, indent=2)
    logging.info(f"Have saved {len(data)} into data/location/loc_batch_{batch_num}.json")

def save_check_point(batch_num):
    os.makedirs("checkpoint", exist_ok=True)
    with open ("checkpoint/checkpoint_ip_location.txt", "w", encoding='utf-8') as wf:
        wf.write(str(batch_num))
    logging.info(f"Have saved checkpoint {batch_num}")

def save_error(data):
    os.makedirs("data/error", exist_ok=True)
    check_not_exist = not os.path.exists("data/error/ip_error.csv")
    with open ("data/error/ip_error.csv", "a", encoding='utf-8', newline="") as wf:
        writer = csv.writer(wf)
        if check_not_exist:
            writer.writerow(["ip_address", "error_message"])
        for ip, message in data:
            writer.writerow([ip, message])
    logging.info(f"Have saved {len(data)} into data/error/ip_error.csv")

def save_ip_address(data):
    os.makedirs("data", exist_ok=True)
    check_not_exist = not os.path.exists("data/ip_address.csv")
    with open ("data/ip_address.csv", "w", encoding='utf-8', newline='') as wf:
        writer = csv.writer(wf)
        if check_not_exist:
            writer.writerow(["ip_address"])
        for ip in data:
            writer.writerow([ip])
    logging.info(f"Have saved {len(data)} ip_address into data/ip_address.csv")
