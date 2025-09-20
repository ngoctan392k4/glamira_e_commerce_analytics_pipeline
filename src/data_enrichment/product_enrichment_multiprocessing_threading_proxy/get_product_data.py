from bs4 import BeautifulSoup
import requests
import re
import json
import time
import logging


def collect_product_info(href, retries=3, wait_time=2):
    data = {}
    # headers = {
    #     "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
    # }

    #Websahre.io - expired - should be in environment yml file 
    proxies={
        "http": "http://user-passwordwl@p.webshare.io:80",
        "https": "http://user-passwordwl@p.webshare.io:80"
    }

    #Cherry - expired
    # proxies={
    #     "http": "http://user-passwordwl@aus.360s5.com:3680"
    # }

    for attempt in range(retries):
        try:
            response = requests.get(href, proxies=proxies, timeout=30)
            if response.status_code == 404:
                return {"status": 404}
            if response.status_code == 403:
                return {"status": 403}
            html = response.text

            match = re.search(r"var\s+react_data\s*=\s*(\{.*?\});", html, re.DOTALL)
            if match:
                json_text = match.group(1)
                try:
                    react_data = json.loads(json_text)
                except json.JSONDecodeError as e:
                    logging.exception(f"Error when loading json file with the link {href}")
                    return {
                        "href": href
                    }
                else:
                    if react_data.get("product_id"):
                        data = {
                            "product_id": react_data.get("product_id"),
                            "product_name": react_data.get("name"),
                            "sku": react_data.get("sku"),
                            "attribute_set_id": react_data.get("attribute_set_id"),
                            "attribute_set": react_data.get("attribute_set"),
                            "type_id": react_data.get("type_id"),
                            "min_price": react_data.get("min_price_format"),
                            "max_price": react_data.get("max_price_format"),
                            "gold_weight": react_data.get("gold_weight"),
                            "none_metal_weight": react_data.get("none_metal_weight"),
                            "fixed_silver_weight": react_data.get("fixed_silver_weight"),
                            "material_design": react_data.get("material_design"),
                            "collection": react_data.get("collection"),
                            "collection_id": react_data.get("collection_id"),
                            "product_type": react_data.get("product_type"),
                            "product_type_value": react_data.get("product_type_value"),
                            "category": react_data.get("category"),
                            "category_name": react_data.get("category_name"),
                            "store_code": react_data.get("store_code"),
                            "gender": react_data.get("gender")
                        }

                        options = react_data.get("options", [])
                        for option in options:
                            group = option.get("group")
                            value = option.get("values")
                            if group == "stone":
                                data["stone"] = value
                            elif group == "alloy":
                                data["color"] = value
                            elif group == "custom":
                                data["custom"] = value

                        data["media_image"] = react_data.get("media_image")
                        data["media_video"] = react_data.get("media_video")

                        break

                    else:
                        return {
                            "href": href
                        }
            else:
                logging.info(f"Attempt {attempt+1}: react_data variable cannot be found, wait {wait_time} sec and try again")
                time.sleep(wait_time)
        except Exception as e:
            logging.exception(f"Cannot load the url {href}")
            return {
                "status": "not load"
            }

    if not data:
        logging.exception(f"Cannot find the react_variable after {attempt+1} attempt")
        return {
            "status": "fail many attempt"
        }
    else:
        return data
