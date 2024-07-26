from scrapy.http import HtmlResponse
from scrapy.spidermiddlewares.httperror import HttpError
from urllib.parse import urljoin, urlparse, quote
from urllib.parse import quote
from dotenv import load_dotenv
import scrapy
import os
load_dotenv()
import logging
from lxml import html

USER_AGENT = os.getenv('USER_AGENT')
AWS_URL = os.getenv('AWS_URL')
AWS_HEADERS = {
    'Content-Type': 'application/json'
}
TRASH_DATA = ['', '#', 'https://www.instagram.com/', 'https://twitter.com/', 'https://www.facebook.com/', 'https://www.linkedin.com/']



class FailedTestSpider(scrapy.Spider):
    name = 'failed_test'
    custom_headers = {
            'user-agent': USER_AGENT
    }
    def __init__(self, url_to_scrape: str, **kwargs):
        super().__init__(**kwargs)
        self.url_to_scrape = url_to_scrape.strip('/') + '/'
        self.subprocess_instance = SubProcesses(self.url_to_scrape)
        self.footer_links = list()
        self.footer_links_scraped = False
        
    def start_requests(self):
        try:
            payload = {
                "url": self.url_to_scrape,
                "method": "get",
                "headers": self.custom_headers,
                "payload": "",
                "timeout": 10,
                "proxies": {},
                "response_type": "text"
            }
            AWS_URL = f'http://localhost:3000/url/request?url={quote(self.url_to_scrape)}'
            # yield scrapy.Request(AWS_URL, headers=AWS_HEADERS, body=json.dumps(payload), method="POST", callback=self.parse, errback=self.errback_httpbin)
            yield scrapy.Request(AWS_URL, headers=self.custom_headers, callback=self.parse, errback=self.errback_httpbin)
        except Exception as e:
            print(e)


    def parse(self, response: HtmlResponse):
        lambda_response = response.json()
        try: response_html: HtmlResponse = lambda_response["body"]
        except: response_html: HtmlResponse = response.text
        html_response: html.HtmlElement = html.fromstring(response_html)
        if not self.footer_links_scraped:
            try:
                footer_links = list(set(html_response.xpath('//footer//a')))
                # self.subprocess_instance.get_all_footer_links(footer_links)
                
                unique_footer_links = list(self.subprocess_instance.get_all_footer_links(footer_links))
                print('unique_footer_links', len(unique_footer_links))
                unique_footer_links = []
                for foo_sublink in unique_footer_links[:]:
                    footer_link_meta = {
                        'f_link': foo_sublink,
                        'f_text': 'test'
                    }
                    if self.subprocess_instance.uri_validator(foo_sublink):
                        AWS_URL = f'http://localhost:3000/url/request?url={quote(foo_sublink)}&wait_forcefully=1500'
                        yield scrapy.Request(AWS_URL, headers=self.custom_headers, callback=self.get_footer_links, meta=footer_link_meta)
                        # yield scrapy.Request(AWS_URL, headers=AWS_HEADERS, body=json.dumps(payload), method="POST", callback=self.get_footer_links, meta=footer_link_meta)
                    else:
                        self.footer_links.append((foo_sublink, 'test', ''))

                self.footer_links_scraped = True
                # data['footer_links'] = unique_footer_links
            except Exception as e:
                error_logger.error(f'Target url: {self.url_to_scrape}, sub URL: {self.url_to_scrape} In `footer_links` section: {e}')


    def get_footer_links(self, response: HtmlResponse):
        link_text = response.meta
        try:
            link = link_text['f_link']
            text = link_text['f_text']

            lambda_response = response.json()
            # print(lambda_response, 'e.message')
            status_code = lambda_response['status_code']
            request_url = lambda_response['current_url']
            print(request_url, status_code)
            self.footer_links.append((link, text, status_code))
        except Exception as e:
            print(e)

    def errback_httpbin(self, failure: HtmlResponse):
        request_url = failure.request.meta['request_url']

        if failure.check(HttpError):
            # you can get the non-200 response
            response = failure.value.response

            # if response.status == 301:
            #     data['links_301'].add(request_url)
            #     logger.info(f'Found 301: {request_url}')
            # elif response.status == 404:
            #     data['links_404'].add(request_url)
            #     logger.info(f'Found 404: {request_url}')
            # else:
            #     data['broken_links'].add(request_url)
            #     logger.info(f'Found broken {response.status}: {request_url}')

            if response.status == 404 or response.status == 501:
                print(f'Found {response.status}: {request_url}')
                
        if True:
            print(response.status, request_url)

    def close(self, spider):
        print(self.footer_links)
        print(len(self.footer_links))

class SubProcesses(FailedTestSpider):
    def __init__(self, url_to_scrape=None):
        self.url_to_scrape = url_to_scrape

    def add_schema_domain_in_link(self, link: str):
        if (
            not link.startswith('mailto:') and
            not link.startswith('tel:')
        ):
            if link.startswith('//'): link = 'https:' + link
            if link.startswith('/'): link = self.url_to_scrape.strip('/') + link
            if not link.startswith('http'): link = self.url_to_scrape + link
        return link.strip()
        
    def get_all_footer_links(self, footer_links: list[html.HtmlElement]):
        unique_footer_links = set()
        for foo_link in footer_links:
            foo_sublink = foo_link.xpath('@href')
            foo_sublink = foo_sublink[0] if foo_sublink else ''
            foo_sublink = self.add_schema_domain_in_link(foo_sublink)

            if foo_sublink and foo_link not in TRASH_DATA:
                foo_sublink_text = foo_link.xpath('text()')
                if not foo_sublink_text:
                    foo_sublink_text = foo_link.xpath('descendant::text()')
                foo_sublink_text:str = foo_sublink_text[0] if foo_sublink_text else ''
                if foo_sublink_text: foo_sublink_text = foo_sublink_text.strip()
                unique_footer_links.add((foo_sublink, foo_sublink_text))
        return unique_footer_links
    
    def check_crawled_url_status(self, lambda_response: dict, data: dict):
        crawl_url_status, move_foward = False, False
        if 'status_code' in lambda_response.keys():
            status_code = lambda_response['status_code']
            request_url = lambda_response["request_url"]
            if status_code == 200:
                crawl_url_status, move_foward = True, True
            elif status_code in [404, 501]:
                data['broken_links'] = [request_url, status_code]
                crawl_url_status, move_foward = True, False
            else: # 301, 403, 429
                crawl_url_status, move_foward = True, False

            print('##################################################################')
            print(self.url_to_scrape, status_code, request_url, crawl_url_status, move_foward)
        return crawl_url_status, move_foward

    def uri_validator(self, url):
        try:
            result = urlparse(url)
            return all([result.scheme, result.netloc])
        except: return False


def setup_logger(name, log_file, level):
    formatter = logging.Formatter('%(asctime)s : %(message)s',  datefmt='%m-%d-%Y %I:%M')
    
    """To setup as many loggers as you want"""
    handler = logging.FileHandler(log_file)        
    handler.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(handler)

    return logger

logger = setup_logger('logger', f'status_log.log', logging.INFO)
error_logger = setup_logger('error_logger', 'error_log.log', logging.ERROR)

if __name__ == '__main__':
    import logging
    from scrapy.crawler import CrawlerRunner, CrawlerProcess
    from scrapy.utils.project import get_project_settings

    settings = get_project_settings()
    url_to_scrape = 'ADD_YOUR_SEO_WEBSITE'
    process = CrawlerProcess(settings)
    process.crawl(FailedTestSpider, url_to_scrape)
    process.start()
    
    # crawl_runner = CrawlerRunner()
    # crawl_runner.crawl(FailedTestSpider)
    
