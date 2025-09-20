import json
import logging
import random
import re
import asyncio
import aiohttp
from aiohttp import ClientSession


find_react_data = re.compile(r"var\s+react_data\s*=\s*(\{.*?\});", re.DOTALL)

async def fetch_text_html(session: ClientSession, url, timeout = 30):
    try:
        async with session.get(url, timeout=timeout) as resp:
            if resp.status in (403, 404, 429):
                return None, resp.status
            text = await resp.text(errors="ignore")
            return text, resp.status
    except Exception as e:  # network / timeout
        logging.exception(f"Cannot load the url {url}")
        return None, None


async def collect_product_info(session: ClientSession, href, retries = 3, base_wait = 1.5, jitter = 0.75, timeout = 20):
    for attempt in range(1, retries + 1):
        text, status_code = await fetch_text_html(session, href, timeout=timeout)
        if status_code == 404:
            return {"status": 404}
        if status_code == 403:
            return {"status": 403}
        if status_code == 429:
            if attempt == retries:
                logging.exception(f"Error 429 after 3 attempts: {href}")
                return {"status": 429}
            wait_time = base_wait * (2 ** (attempt - 1))
            wait_time += random.uniform(0, jitter) #random wait time => avoid same wait time => be detected
            await asyncio.sleep(wait_time)
            continue
        if text is None:  # network error; retry
            if attempt == retries:
                logging.exception(f"Cannot load the url {href}")
                return {"status": "not load"}
            wait_time = base_wait * (2 ** (attempt - 1))
            wait_time += random.uniform(0, jitter) #random wait time => avoid same wait time => be detected
            await asyncio.sleep(wait_time)
            continue
        else:
            match = find_react_data.search(text)
            if match:
                json_text = match.group(1)
                try:
                    react_data = json.loads(json_text)
                except Exception as e:
                    logging.exception(f"Error when loading json file with the link {href}")
                    return {"href": href}

                pid = react_data.get("product_id")
                if not pid:
                    return {"href": href}

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
                    data[group] = value
                data["media_image"] = react_data.get("media_image")
                data["media_video"] = react_data.get("media_video")

                return data
            else:
                logging.info(f"Attempt {attempt}: react_data variable cannot be found with {href}, wait and retry")

        if attempt < retries:
            wait_time = base_wait * (2 ** (attempt - 1))
            wait_time += random.uniform(0, jitter) #random wait time => avoid same wait time => be detected

            await asyncio.sleep(wait_time)

    # fail after retries times
    logging.error(f"Cannot find react_data after {retries} attempts: {href}")
    return {"status": "fail many attempt"}
