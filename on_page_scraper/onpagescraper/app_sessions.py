from onpagescraper.log_utility import info_logger, error_logger
from onpagescraper.spiders.op_teer import OpTeerSpider, RUN_IN_LOCAL
from onpagescraper.spiders.onpage import OnpageSpider, RUN_IN_LOCAL
from flask import Flask, request
from celery_config import app as celery_app

from scrapy.crawler import CrawlerRunner
from urllib.parse import urlparse
import uuid
import os

import crochet
crochet.setup() # initialize crochet

from dotenv import load_dotenv
load_dotenv()

BASE_ROUTE = os.getenv('BASE_ROUTE')

app = Flask(__name__)
crawl_runner = CrawlerRunner()


def uri_validator(url):
    try:
        result = urlparse(url)
        return all([result.scheme, result.netloc])
    except: return False


@app.route(f'{BASE_ROUTE}/ping', methods=['GET'])
def ping():
    return {"result": True}, 200


@app.route(f'{BASE_ROUTE}/scrape', methods=['POST'])
def scrape_target_url():
    try:
        payload = request.get_json()
        target_url = payload['target_url']
        page_limit = int(payload['page_limit'])

        url_status = uri_validator(target_url)
        if not url_status:
            return {'request_recieved': False, 'messsage': 'target_url is not valid'}, 400
        
        if not RUN_IN_LOCAL: session_id = payload['id']
        else: session_id = str(uuid.uuid4())
        
        info_logger.info(f'------------------------------------------------------------------------------------------------')
        info_logger.info(f'Target URL: {target_url}')
        info_logger.info(f'Session received: {session_id}')
    except KeyError as e:
        error_logger.error(f'In `scrape_target_url` KeyError: {e}')
        return {'request_recieved': False, 'messsage': f'Feild {e} is missing!'}, 400
    except Exception as e: 
        error_logger.error(f'In `scrape_target_url` Exception: {e}')
        return {'request_recieved': False, 'messsage': f'{e}'}, 400

    scrape_with_crochet(target_url, session_id, page_limit) # Passing that URL to our Scraping Function
    return {'request_recieved': True, 'messsage': 'success'}, 200


def scrape_with_crochet(target_url, session_id, page_limit):   
    # This will connect to the OnpageSpider function in our scrapy file and after each yield will pass to the crawler_result function.
    # eventual = crawl_runner.crawl(OnpageSpider, url_to_scrape=target_url, user_id=session_id, limit_of_urls=page_limit)
    eventual = crawl_runner.crawl(OpTeerSpider, url_to_scrape=target_url, user_id=session_id, limit_of_urls=page_limit)
    return eventual


@app.route(f'{BASE_ROUTE}/search_volume_keyword_difficulty_alt_image', methods=['POST'])
def fetch_keywords_with_celery():
    try:
        payload_list = request.get_json()
        info_logger.info(f'------------------------------------------------------------------------------------------------')
        info_logger.info('Session received to extract keywords')
        info_logger.info(f'Target URLs: {payload_list}')

        if type(payload_list) != list:
            payload_list = [payload_list]
        
        for payload in payload_list:
            url = payload['url']
            on_page_seo_id = payload['id']
            added_date = payload['added_date']
            celery_arg_for_search_volume = {
                "url": url,
                "on_page_seo_id": on_page_seo_id,
                "added_date": added_date
            }
            celery_app.send_task('celery_config.get_search_volume_keyword_difficulty', args=(celery_arg_for_search_volume, ))

            alt_image_data = payload['image_urls']
            for image_data in alt_image_data:            
                image_id = image_data['id']
                image_url = image_data['image_url']

                if (
                    image_url.startswith('data:image/') and 
                    (not 'base64,' in image_url)
                ): continue

                celery_arg_for_alt_image = {
                    'image_id': image_id,
                    'image_url': image_url,
                    'on_page_seo_id': on_page_seo_id
                }
                try: celery_app.send_task('celery_config.get_alt_image_size', args=(celery_arg_for_alt_image, ), queue='queue2')
                except Exception as e: error_logger.error(f'Target url: {image_url} In `celery_config.get_alt_image_size` section: {e}')
                
        return {'request_recieved': True, 'messsage': 'success'}, 200
    except KeyError as e:
        error_logger.error(f'In `fetch_keywords_with_celery`: {e}')
        return {'request_recieved': False, 'messsage': f'Field {e} is missing!'}, 400
    except Exception as e:
        error_logger.error(f'In `fetch_keywords_with_celery`: {e}')
        return {'request_recieved': False, 'messsage': 'Fields changed!!!'}, 400


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=9222)
