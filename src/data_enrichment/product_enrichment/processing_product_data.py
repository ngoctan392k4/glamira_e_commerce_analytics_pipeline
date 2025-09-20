from pymongo import MongoClient
from tqdm import tqdm

def re_process_product_data():
    client = MongoClient("mongodb://localhost:27017")
    db = client["glamira"]
    col = db["product_data"]

    output = []
    total_docs = col.count_documents({})

    for doc in tqdm(col.find({}, {"_id": 0}), total=total_docs, desc="Processing documents"):
        product_id = doc.get("product_id")
        for key, value in doc.items():

            if isinstance(value, dict) and key != "product_id":

                sub_fields = {k: v for k, v in value.items() if k != "product_id"}
                new_doc = {
                    "product_id": product_id,
                    "suffix": key,
                    **sub_fields
                }
                output.append(new_doc)

            if len(output) >= 10000 :
                db["product_info"].insert_many(output)
                output.clear()

    if output:
        db["product_info"].insert_many(output)

