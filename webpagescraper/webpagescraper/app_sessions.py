from webpagescraper.log_utility import info_logger, error_logger
from webpagescraper.spiders.op_teer import OpTeerSpider, RUN_IN_LOCAL
from webpagescraper.spiders.onpage import OnpageSpider, RUN_IN_LOCAL
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


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=9222)
