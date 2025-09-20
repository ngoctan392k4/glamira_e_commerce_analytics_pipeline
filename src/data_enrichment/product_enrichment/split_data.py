import os
import json
import logging
from tqdm import tqdm

def split():
    input_folder = "data/product_detail"
    output_folder = "data/product_detail_v2"
    os.makedirs(output_folder, exist_ok=True)

    file_list = [f for f in os.listdir(input_folder) if f.endswith(".json")]

    for filename in tqdm(file_list, desc="Splitting per product_id"):
        file_path = os.path.join(input_folder, filename)
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                data = json.load(f)
                if not isinstance(data, list):
                    continue

                for item in data:
                    product_id = item.get("product_id")
                    if not product_id:
                        continue

                    for key, value in item.items():
                        if key == "product_id":
                            continue

                        output_file = os.path.join(output_folder, f"{product_id}_{key}.json")
                        obj = {"product_id": product_id, key: value}
                        with open(output_file, "w", encoding="utf-8") as out_f:
                            json.dump(obj, out_f, ensure_ascii=False, indent=2)
        except Exception as e:
            logging.info(f"Error when reading file {filename}: {e}")
