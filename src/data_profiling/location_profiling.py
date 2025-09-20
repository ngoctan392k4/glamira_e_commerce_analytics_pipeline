import os
from pymongo import MongoClient
import csv
from src.data_profiling.yaml_config import load_config
config = load_config()

def location_analyze():
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = config["CREDENTIAL"]
    client = MongoClient(config["MONGODB_URL"], serverSelectionTimeoutMS=5)
    database = client["glamira"]
    collection = database["ip_location"]


    pipeline_country = [
        {"$group": {"_id": "$country"}},
        {"$project": {"country": "$_id", "_id":0}},
        {"$count": "unique_country_count"}
    ]

    pipeline_country_short = [
        {"$group": {"_id": "$country_short"}},
        {"$project": {"country_short": "$_id", "_id":0}},
        {"$count": "unique_country_short_count"}
    ]

    pipeline_region = [
        {"$group": {"_id": "$region"}},
        {"$project": {"region": "$_id", "_id":0}},
        {"$count": "unique_region_count"}
    ]

    pipeline_ip = [
        {"$group": {"_id": "$ip"}},
        {"$project": {"ip": "$_id", "_id":0}},
        {"$count": "unique_ip_count"}
    ]

    pipeline_country_region = [
        {"$group": { "_id": {"country": "$country", "region": "$region" }, "doc_count": { "$sum": 1 }}},
        {"$project": { "_id": 0, "country": "$_id.country", "region": "$_id.region", "doc_count": 1}},
        {"$sort": { "doc_count": -1 }}
    ]

    pipeline_country_null = [
        { "$match": { "$or": [ { "country": None }, { "country": "-" } ] } },
        { "$count": "country_null" }
    ]
    country_null = list(collection.aggregate(pipeline_country_null, allowDiskUse=True))
    if country_null:
        print(f"Country Null data: {country_null[0]['country_null']}")
    else:
        print("Country Null data: 0")

    pipeline_region_null = [
        { "$match": { "$or": [ { "region": None }, { "region": "-" } ] } },
        { "$count": "region_null" }
    ]
    region_null = list(collection.aggregate(pipeline_region_null, allowDiskUse=True))
    if region_null:
        print(f"Region Null data: {region_null[0]["region_null"]}")
    else:
        print(f"Region Null data: 0")

    pipeline_country_region_null = [
        {
            "$match": {
                "$and": [
                    { "$or": [ { "region": None }, { "region": "-" } ] },
                    { "$or": [ { "country": None }, { "country": "-" } ] }
                ]
            }
        },
        { "$count": "country_region_null" }
    ]

    country_region_null = list(collection.aggregate(pipeline_country_region_null, allowDiskUse=True))
    if country_region_null:
        print(f"Country Region Null data: {country_region_null[0]["country_region_null"]}")
    else:
        print(f"Country Region Null data: 0")


    country_short = list(collection.aggregate(pipeline_country_short, allowDiskUse=True))
    country = list(collection.aggregate(pipeline_country, allowDiskUse=True))
    region = list(collection.aggregate(pipeline_region, allowDiskUse=True))
    ip = list(collection.aggregate(pipeline_ip, allowDiskUse=True))

    print(f"Country short: {country_short[0]["unique_country_short_count"]}")
    print(f"Country: {country[0]["unique_country_count"]}")
    print(f"Region: {region[0]["unique_region_count"]}")
    print(f"IP: {ip[0]["unique_ip_count"]}")

    country_region = list(collection.aggregate(pipeline_country_region, allowDiskUse=True))
    with open ("country_region.csv", "w", encoding='utf-8', newline="") as wf:
        writer = csv.writer(wf)
        writer.writerow(["country", "region", "number_of_doc"])
        for cr in country_region:
            writer.writerow([cr['country'], cr['region'], cr['doc_count']])
