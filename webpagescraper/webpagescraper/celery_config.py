from webpagescraper.database_utility import database_utility
from webpagescraper.log_utility import error_logger
from fetch_keywords_from_url import get_keyword_search_volume
from datetime import datetime
from celery import Celery
import base64
import os
import time
import pytz
import logging
import pandas as pd
import json
import requests

# RUN_IN_LOCAL: celery -A celery_config worker -l info -Q queue2 -c 2

from dotenv import load_dotenv
load_dotenv()

AWS_URL = os.getenv('AWS_URL')
USER_AGENT = os.getenv('USER_AGENT')
AWS_HEADERS = {
    'Content-Type': 'application/json'
}
RUN_IN_LOCAL = os.getenv('RUN_IN_LOCAL')

custon_headers = {
    'user-agent': USER_AGENT
}

app = Celery(
    'tasks',
    broker=os.getenv('REDIS_URL'),
    backend=os.getenv('REDIS_URL'),
)

app.conf.update(
    result_expires=3600,
)

# Add logging configuration for Celery worker
logger = app.log.get_default_logger()
logger.setLevel(logging.INFO)

timezone = pytz.timezone('Asia/Kolkata')
custom_headers = {
    'user-agent': USER_AGENT
}


@app.task(queue='queue')
def get_alt_image_size(input_dict):
    image_id = input_dict['image_id']
    image_url = input_dict['image_url']
    seo_id = input_dict['seo_id']

    try:
        if (
            image_url.startswith('data:image/') and 
            ('base64,' in image_url)
        ):  
            base64_svg_gif_data = image_url.split('base64,')[1]
            decoded_svg_gif_data = base64.b64decode(base64_svg_gif_data)
            size_in_bytes = len(decoded_svg_gif_data)
        else:
            payload = {
                "url": image_url,
                "method": "get",
                "headers": custom_headers,
                "payload": "",
                "timeout": 10,
                "proxies": {},
                "response_type": "content"
            }
            response = requests.post(AWS_URL, headers=AWS_HEADERS, data=json.dumps(payload))
            try: size_in_bytes = response.json()["body"]
            except Exception as e: 
                error_logger.error(f"Error during the getting the lambda response from celery 2: {image_url}, {e}")
                return None

        # image = Image.open(BytesIO(response.content))
        # (width, height) = image.size

        original_size_in_kb = size_in_bytes / 1024.0
        original_size_in_mb = original_size_in_kb / 1024.0
        if original_size_in_kb < 1:
            original_size_in_kb = custom_round(original_size_in_kb, 2)
            size_data_in_kb = str(original_size_in_kb) + ' KB'
            size_data_in_mb = str(original_size_in_mb) + ' MB'
        else:
            size_data_in_kb = str(round(original_size_in_kb)) + ' KB'
            size_data_in_mb = str(round(original_size_in_mb)) + ' MB'

        image_size = size_data_in_mb if original_size_in_mb > 1 else size_data_in_kb
        image_data = {
            'id': image_id,
            'size': image_size,
            'seo_id': seo_id,
        }
        if RUN_IN_LOCAL: 
            print(image_data)
    
    except Exception as e:
        error_logger.error(f"Error during the generating the size of alt image: {image_url}, {e}")


def custom_round(number, decimals=0):
    multiplier = 10 ** decimals
    return int(number * multiplier + 0.5) / multiplier