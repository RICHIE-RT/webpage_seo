from scrapy_splash import SplashRequest

from scrapy.http import HtmlResponse
from scrapy.exceptions import CloseSpider
from scrapy.selector.unified import SelectorList

from twisted.internet.error import DNSLookupError
from scrapy.spidermiddlewares.httperror import HttpError
from twisted.internet.error import TimeoutError, TCPTimedOutError

from lxml import html
from typing import List # FOR UBUNTU SERVER, ERROR: TypeError: 'type' object is not subscriptable - list[dict]
from bs4 import BeautifulSoup
from datetime import datetime
from dotenv import load_dotenv
from collections import OrderedDict
from urllib.parse import urljoin, urlparse, quote
from onpagescraper.log_utility import info_logger, error_logger

import re
import os 
import copy
import json
import scrapy
import requests
import concurrent.futures as ccf

load_dotenv()
AWS_URL = os.getenv('AWS_URL')
USER_AGENT = os.getenv('USER_AGENT')
RUN_IN_LOCAL = os.getenv('RUN_IN_LOCAL')
BASE_DIR_OF_LOGGER = os.getenv('BASE_DIR_OF_LOGGER')
BASE_DIR_OF_RESULTS_JSON = os.getenv('BASE_DIR_OF_RESULTS_JSON')
RESULT_ENDPOINT = os.getenv('RESULT_ENDPOINT')
ENDPOINT_RESULT_CHUNKS = int(os.getenv('ENDPOINT_RESULT_CHUNKS'))
LIMIT_OF_URL = os.getenv('LIMIT_OF_URLS_IN_DEEP_SCRAPING')
if LIMIT_OF_URL is not None:
    LIMIT_OF_URL = int(LIMIT_OF_URL)

TRASH_DATA = ['', '#', 'https://www.instagram.com/', 'https://twitter.com/', 'https://www.facebook.com/', 'https://www.linkedin.com/']
ROBOTS_TAG_PATTERNS = [r'meta name=\'robots\' content=\'([^\']*)\'', r'meta name=\"robots\" content=\"([^\"]*)\"']
ROBOTS_TXT_PATTERNS = [r'sitemap:\s*(.+)', r'(https?://\S+sitemap?[/_a-zA-Z0-9.-]+)']
URL_IGNORE_PATTERN = r'\.(jpg|png|gif|csv|pdf|jpeg|webp)$'
RUN_FOOTER_SITEMAP_REQUEST = True
AWS_HEADERS = {
    'Content-Type': 'application/json'
}


class OpTeerSpider(scrapy.Spider):
    name = 'op_teer'
    custom_settings = {
        'CONCURRENT_REQUESTS': 10,
        'DOWNLOAD_TIMEOUT': 10,
        'DOWNLOADER_MIDDLEWARES': {
            # Engine side
            'scrapy_splash.SplashCookiesMiddleware': 723,
            'scrapy_splash.SplashMiddleware': 725,
            'scrapy.downloadermiddlewares.httpcompression.HttpCompressionMiddleware': 810,
            # Downloader side
        },
        'SPIDER_MIDDLEWARES': {
            'scrapy_splash.SplashDeduplicateArgsMiddleware': 100,
        },
        'SPLASH_URL': 'http://127.0.0.1:8050/',
        'DUPEFILTER_CLASS': 'scrapy_splash.SplashAwareDupeFilter',
        'HTTPCACHE_STORAGE': 'scrapy_splash.SplashAwareFSCacheStorage',
        'ROBOTSTXT_OBEY': False
    }

    def __init__(self, url_to_scrape: str, user_id: str, limit_of_urls: int, **kwargs):
        super().__init__(**kwargs)

        self.url_to_scrape = url_to_scrape.strip('/') + '/'
        self.user_id = user_id
        self.limit_of_urls = limit_of_urls
        self.domain = urlparse(self.url_to_scrape).netloc

        self.custom_headers = {
            'user-agent': USER_AGENT
        }
        self.visited_links = set()
        self.meta_data = {
            'url': '',
            'title': '',
            'title_len': 0,
            'desc': '',
            'desc_len': 0,
            'h1_tags': dict(),
            'h2_tags': dict(),
            'h3_tags': dict(),
            'h4_tags': dict(),
            'h5_tags': dict(),
            'h6_tags': dict(),
            'int_links': set(),
            'ext_links': set(),
            'temp_links': set(),
            'yt_links': set(),
            'contact_links': set(),
            'alt_links': set(),
            'video_count': 0,
            'faqs': False,
            'schema_urls': 0,
            'schema_types': set(),
            'cta': '',
            'social_links': set(),
            'footer_links': set(),
            'index_type': None,
            'lang_tag': False,
            'robots': False,
            'robots_string': '',
            'robots_content': set(),
            'sitemap': list(),
            'links_301': '',
            'links_404': '',
            'broken_links': list(),
            'canonical': '',
            'mobile': ''
        }
        self.robots_txt_string = ''
        self.sitemap_page_urls = set()
        self.footer_links = list()
        self.results = list()
        self.footer_links_scraped = False
        self.sitemap_page_urls_scraped = True
        self.subprocesses_instance = SubProcesses(self.url_to_scrape)
        self.lua_script = """
        function main(splash)
        splash:init_cookies(splash.args.cookies)
        assert(splash:go{
            splash.args.url,
            headers=splash.args.headers,
            http_method=splash.args.http_method,
            body=splash.args.body,
            })
        assert(splash:wait(0.5))

        local entries = splash:history()
        local last_response = entries[#entries].response
        return {
            url = splash:url(),
            headers = last_response.headers,
            http_status = last_response.status,
            cookies = splash:get_cookies(),
            html = splash:html(),
        }
        end
        """


    def start_requests(self):
        if RUN_IN_LOCAL:
            print('Target URL:', self.url_to_scrape)
        sub_data = copy.deepcopy(self.meta_data)
        sub_data['url'] = self.url_to_scrape
        try:
            sub_meta_data = {
                'session_id': f'{self.user_id}',
                'data': sub_data,
                'request_url': self.url_to_scrape
            }
            # scrapy_request_url = f'http://localhost:3000/url/request?url={quote(self.url_to_scrape)}'
            # yield scrapy.Request(scrapy_request_url, headers=self.custom_headers, callback=self.parse, errback=self.errback_httpbin, meta=sub_meta_data)
            scrapy_request_url = self.url_to_scrape
            yield SplashRequest(scrapy_request_url, headers=self.custom_headers, callback=self.parse, errback=self.errback_httpbin, meta=sub_meta_data, args={'wait': 0.5, 'lua_source': self.lua_script}, endpoint='execute', magic_response=True)
        except Exception as e:
            error_logger.error(f'Target url: {self.url_to_scrape} In `start_requests` section: {e}')


    def parse(self, response: HtmlResponse):
        data = response.meta['data']
        response_json = {
            "current_url": response.url,
            "status_code": response.status,
            "body": response.text
        }
        lambda_response = response_json
        # lambda_response = response.json()
        current_url: str = lambda_response['current_url']
        redirect_url_status: bool = current_url in self.visited_links
        
        if not current_url.endswith('/'): 
            current_url = current_url + '/'
            
        data['url'] = current_url # Re-write the url to get latest url (incase of redirect)
        crawl_url_status, move_forward, data = self.subprocesses_instance.check_crawled_url_status(lambda_response, data) # No need of this as we shift to docker and scrapy splash
        if crawl_url_status and not move_forward and not current_url in self.visited_links:
            self.visited_links.add(current_url)
            self.results.append(data)

        if move_forward and not redirect_url_status and not current_url in self.visited_links:
            self.visited_links.add(current_url)
            response_html: HtmlResponse = lambda_response["body"]
            html_response: html.HtmlElement = html.fromstring(response_html)

            try:
                title_string = html_response.xpath('//title/text()')
                if title_string:
                    data['title'] = title_string[0]
                    data['title_len'] = len(title_string[0])

                description_string = html_response.xpath('//meta[@name="description" or @property="og:description"]/@content')
                if description_string:
                    data['desc'] = description_string[0]
                    data['desc_len'] = len(description_string[0])

                pattern = re.compile(r'<h([1-6])', re.IGNORECASE)
                matches = pattern.findall(response_html)
                sequence_of_h1_h6_tags = ['h' + match for match in matches]
                unique_sequence_of_h1_h6_tags = list(OrderedDict.fromkeys(sequence_of_h1_h6_tags))

                h1_tag_strings = html_response.xpath('//h1//text()')
                len_strings_position_of_h1_tag = self.subprocesses_instance.get_len_strings_and_position(h1_tag_strings, target_tag='h1', reference_sequence=unique_sequence_of_h1_h6_tags)
                data['h1_tags'] = len_strings_position_of_h1_tag

                h2_tag_strings = html_response.xpath('//h2//text()')
                len_strings_position_of_h2_tag = self.subprocesses_instance.get_len_strings_and_position(h2_tag_strings, target_tag='h2', reference_sequence=unique_sequence_of_h1_h6_tags)
                data['h2_tags'] = len_strings_position_of_h2_tag

                h3_tag_strings = html_response.xpath('//h3//text()')
                len_strings_position_of_h3_tag = self.subprocesses_instance.get_len_strings_and_position(h3_tag_strings, target_tag='h3', reference_sequence=unique_sequence_of_h1_h6_tags)
                data['h3_tags'] = len_strings_position_of_h3_tag

                h4_tag_strings = html_response.xpath('//h4//text()')
                len_strings_position_of_h4_tag = self.subprocesses_instance.get_len_strings_and_position(h4_tag_strings, target_tag='h4', reference_sequence=unique_sequence_of_h1_h6_tags)
                data['h4_tags'] = len_strings_position_of_h4_tag

                h5_tag_strings = html_response.xpath('//h5//text()')
                len_strings_position_of_h5_tag = self.subprocesses_instance.get_len_strings_and_position(h5_tag_strings, target_tag='h5', reference_sequence=unique_sequence_of_h1_h6_tags)
                data['h5_tags'] = len_strings_position_of_h5_tag

                h6_tag_strings = html_response.xpath('//h6//text()')
                len_strings_position_of_h6_tag = self.subprocesses_instance.get_len_strings_and_position(h6_tag_strings, target_tag='h6', reference_sequence=unique_sequence_of_h1_h6_tags)
                data['h6_tags'] = len_strings_position_of_h6_tag

                # all_ancher_links = html_response.xpath('//a') # update - Nov 20, 2023, Only required internal links of content
                all_ancher_links = html_response.xpath('//a')
                raw_reference_for_int_links: List[str] = html_response.xpath('//a[not(ancestor::header) and not(ancestor::footer)]/@href') # Avoid anchor links in the header and footer for internal links
                reference_for_int_links = list()
                for link in raw_reference_for_int_links:
                    if link:
                        link = self.subprocesses_instance.add_schema_domain_in_link(link)
                        reference_for_int_links.append(link)
                
            except Exception as e:
                error_logger.error(f'Target url: {self.url_to_scrape} In `title, desc, H-tags and ancher/referece` section: {e}')
            
            data, links_to_crawl = self.subprocesses_instance.get_all_ancher_links(data, all_ancher_links, reference_for_int_links)
            if not self.sitemap_page_urls_scraped:
                links_to_crawl = links_to_crawl.union(self.sitemap_page_urls)
                self.sitemap_page_urls_scraped = True

            try:
                alt_links_list = html_response.xpath('//*[@alt]')
                unique_alt_links = self.subprocesses_instance.get_all_alt_links(alt_links_list)
                data['alt_links'] = unique_alt_links
            except Exception as e:
                error_logger.error(f'Target url: {self.url_to_scrape}, sub URL: {current_url} In `alt_links_list` section: {e}')

            if not self.footer_links_scraped and RUN_FOOTER_SITEMAP_REQUEST:
                try:
                    footer_links = list(set(html_response.xpath('//footer//a')))                    
                    unique_footer_links = self.subprocesses_instance.get_all_footer_links(footer_links)
                    with ccf.ThreadPoolExecutor(max_workers=20) as executer:
                        executer.map(self.request_on_footer_links, unique_footer_links)
                        
                    self.footer_links_scraped = True
                except Exception as e:
                    error_logger.error(f'Target url: {self.url_to_scrape}, sub URL: {current_url} In `footer_links` section: {e}')

            videos = html_response.xpath('//video[@alt]')
            data['video_count'] = len(videos)

            canonical_link = html_response.xpath('//*[@rel="canonical"]/@href')
            if canonical_link: data['canonical'] = canonical_link[0]
            
            if html_response.xpath('//*[@lang]/@lang'): data['lang_tag'] = True
            if 'faqs' in response_html.lower() or 'Frequently Asked Questions' in response_html.lower(): data['faqs'] = True

            check_index = html_response.xpath('//*[@name="robots" and contains(@content, "index")]/@content')
            if not check_index: pass
            elif 'noindex' in check_index[0]: data['index_type'] = 'noindex'
            else: data['index_type'] = 'index'

            schema_jsons = html_response.xpath('//script[@type="application/ld+json"]/text()')
            for schema_json in schema_jsons:
                try:
                    schema_dict =  json.loads(schema_json)
                    data['schema_types'].add(schema_dict['@type'])
                except Exception as e:
                    pass     

            schema_urls = re.findall('https://schema.org', response_html)
            data['schema_urls'] = len(schema_urls)

            try:
                for pattern in ROBOTS_TAG_PATTERNS:
                    matching_string = re.findall(pattern, response_html)
                    if matching_string: [data['robots_content'].add(string) for string in matching_string]
            except Exception as e:
                error_logger.error(f'Target url: {self.url_to_scrape} In `ROBOTS_TAG_PATTERNS` section: {e}')  

            if (
                not (len(self.results) >= self.limit_of_urls)
                # not (len(self.visited_links) > self.limit_of_urls)
            ):  
                # If scraped url is not `url_to_scrape` then data will be added in result else it will check for `robots.txt` URL.
                if data['url'] != self.url_to_scrape:
                    self.results.append(data)
                    if RUN_IN_LOCAL:
                        print(data['url'])
                else:
                    robots_url = urljoin(data['url'], 'robots.txt')
                    # scrapy_request_url = f'http://localhost:3000/url/request?url={quote(robots_url)}'
                    # yield scrapy.Request(scrapy_request_url, headers=self.custom_headers, callback=self.robots_sitemap_parser, errback=self.errback_httpbin, meta=response.meta)
                    scrapy_request_url = robots_url
                    yield SplashRequest(scrapy_request_url, headers=self.custom_headers, callback=self.robots_sitemap_parser, errback=self.errback_httpbin, meta=response.meta, args={'wait': 0.5})

                # Crawling starts here for URLs inside scraped URL.
                next_check_url = list(links_to_crawl)
                for link in next_check_url[:LIMIT_OF_URL]:
                    if link not in self.visited_links:
                        sub_data = copy.deepcopy(self.meta_data)
                        try:
                            sub_meta_data = {
                                'session_id': f'{self.user_id}',
                                'data': sub_data,
                                'request_url': link
                            }
                            # scrapy_request_url = f'http://localhost:3000/url/request?url={quote(link)}'
                            # yield scrapy.Request(scrapy_request_url, headers=self.custom_headers, callback=self.parse, errback=self.errback_httpbin, meta=sub_meta_data)
                            scrapy_request_url = link
                            yield SplashRequest(scrapy_request_url, headers=self.custom_headers, callback=self.parse, errback=self.errback_httpbin, meta=sub_meta_data, args={'wait': 0.5, 'lua_source': self.lua_script}, endpoint='execute', magic_response=True)
                        except Exception as e:
                            error_logger.error(f'Target URL: {self.url_to_scrape}, sub URL: {link} In `looping of next_urls` section: {e}')

            else:
                raise CloseSpider('Stopping spider: Required result scraped')
        

    def robots_sitemap_parser(self, response: HtmlResponse):
        response_json = {
            "current_url": response.url,
            "status_code": response.status,
            "body": response.text
        }
        lambda_response = response_json
        # lambda_response = response.json()
        response_html: str = lambda_response["body"]

        try:
            status_code = lambda_response['status_code']
            html_response: html.HtmlElement = html.fromstring(response_html)
            data = response.meta['data']
            data['robots'] = True if status_code in [200, 304] else False
            
            robots_string_text = html_response.xpath('//body/p/text()')
            if not robots_string_text: robots_string_text = html_response.xpath('//body/pre/text()')
            self.robots_txt_string = robots_string_text[0] if robots_string_text else ''
            
            if 'sitemap' in response_html.lower():
                unique_sitemap_urls = self.subprocesses_instance.extract_sitemap_urls_from_robots_txt(self.robots_txt_string)
                data['sitemap'] = unique_sitemap_urls
                if RUN_FOOTER_SITEMAP_REQUEST:
                    self.extract_page_urls_from_sitemap_url(unique_sitemap_urls)
                    self.sitemap_page_urls_scraped = False

            self.results.append(data)
            if RUN_IN_LOCAL:
                print("In robots_sitemap_parser:", data['url'], status_code)
        except Exception as e:
            error_logger.error(f'Target url: {self.url_to_scrape} In `robots_sitemap_parser` function: {e}')
            if RUN_IN_LOCAL:
                print('In robots:', e)
    

    def extract_page_urls_from_sitemap_url(self, sitemap_url):
        for smap_url in sitemap_url:
            response = requests.get(smap_url, headers=self.custom_headers)
            response_data = BeautifulSoup(response.text, 'lxml-xml')
            for loc_element in response_data.find_all('loc'):
                url = loc_element.text
                if '.xml' in url and url not in smap_url: 
                    self.extract_page_urls_from_sitemap_url([url])
                elif re.compile(URL_IGNORE_PATTERN).search(url): continue
                else: self.sitemap_page_urls.add(url)


    def request_on_footer_links(self, unique_footer_link: tuple):
        foo_sublink, foo_sublink_text = unique_footer_link
        payload = {
            "url": foo_sublink,
            "method": "get",
            "headers": self.custom_headers,
            "payload": "",
            "timeout": 10,
            "proxies": {},
            "response_type": "text"
        }
        if self.subprocesses_instance.uri_validator(foo_sublink):
            response = requests.request('POST', AWS_URL, headers=AWS_HEADERS, data=json.dumps(payload))
            try:
                lambda_response = response.json()
                status_code = lambda_response['statusCode']
                self.footer_links.append((foo_sublink, foo_sublink_text, status_code))
            except Exception as e:
                self.footer_links.append((foo_sublink, foo_sublink_text, ''))
        else:
            self.footer_links.append((foo_sublink, foo_sublink_text, ''))


    def filter_data_set_to_list(self, master_data: List[dict]):
        try:
            for raw_data in master_data:
                for key, value in raw_data.items():
                    if type(value) is set:
                        new_value = list(value)
                        raw_data[key] = new_value
        except Exception as e:
            error_logger.error(f'Target url: {self.url_to_scrape} In `filter_data_set_to_list` function: {e}')
        return master_data


    def filter_temp_links_to_int_links(self, master_data: list):
        try:
            check_data = dict()
            for raw_data in master_data:
                check_data[raw_data['url']] = {"temp_links": raw_data['temp_links']}

            for key, value in check_data.items():
                check_data[key]['int_links'] = list()
                for key2, value2 in check_data.items():
                    if key == key2: continue
                    for link in value2['temp_links']:
                        if key == link[0]:
                            check_data[key]['int_links'].append([key2, link[1]])
                        
            for raw_data in master_data:
                raw_data['int_links'] = check_data[raw_data['url']]['int_links']
                raw_data['footer_links'] = self.footer_links
                raw_data['robots_string'] = self.robots_txt_string
                del raw_data['temp_links']

        except Exception as e:
            error_logger.error(f'Target url: {self.url_to_scrape} In `filter_temp_links_to_int_links` function: {e}')
        return master_data


    def errback_httpbin(self, failure: HtmlResponse):
        data = failure.request.meta['data']
        request_url = failure.request.meta['request_url']
        request_url = failure.value.response.url if hasattr(failure.value, 'response') and failure.value.response else request_url
        check_url = 'robots.txt' in request_url
        if check_url:
            data['robots'] = False
            self.results.append(data)

        elif failure.check(HttpError):
            status_code = failure.value.response.status
            if status_code in [404, 501]:
                data['broken_links'] = [request_url, status_code]
                info_logger.info(f'Found {status_code}: {request_url}')
                self.results.append(data) 
            

        elif failure.check(DNSLookupError):
            request = failure.request
            error_logger.error(f"DNSLookupError on {request}")

        elif failure.check(TimeoutError, TCPTimedOutError):
            request = failure.request
            error_logger.error(f"TimeoutError on {request}")


    def send_data_in_chunks(self, chunk_size):
        tried_chunks, succeded_chunks =  0, 0
        for length in range(0, len(self.results), chunk_size):
            chunk = self.results[length:length + chunk_size]
            response = requests.post(f'{RESULT_ENDPOINT + self.user_id}/callback', headers=AWS_HEADERS, json=chunk)
            
            if response.status_code == 200: succeded_chunks+=1
            else: info_logger.error(f"Chunk request to endpoint for {self.url_to_scrape} URL failed. Chunk: {length}:{length + chunk_size} | Status code: {response.status_code} | {response.text}")
            tried_chunks+=1

        info_logger.info(f"Chunk request to endpoint for {self.url_to_scrape} URL was completed | tried_chunks: {tried_chunks} | succeded_chunks: {succeded_chunks}")


    def close(self, spider):
        
        # if RUN_IN_LOCAL:
        self.filter_data_set_to_list(self.results)
        self.filter_temp_links_to_int_links(self.results)

        try:
            info_logger.info(f"Testing in Local: Total visited URLs - {len(self.visited_links)} | Limit of URLs - {self.limit_of_urls}")
            try: result_json_file_name = urlparse(self.url_to_scrape).netloc.strip('www.').replace('.', '-')
            except:
                error_logger.error(f"Error in making name of result_json_file_name. Target URL - {self.url_to_scrape}")
                result_json_file_name = str(self.url_to_scrape).replace('://', '-').replace('.', '-')
            with open(f"{BASE_DIR_OF_RESULTS_JSON}/{datetime.now().strftime('%Y-%m-%d--%H-%M-%S') + ' | ' + result_json_file_name}.json", 'w') as js_file:
                json.dump(self.results, js_file, indent=4)
        except Exception as e:
            info_logger.error(f"Testing in Local: During writing a json for {self.url_to_scrape} URL. Error - {e} Visited URLs - {len(self.visited_links)}")

        if not RUN_IN_LOCAL:
            self.send_data_in_chunks(chunk_size=ENDPOINT_RESULT_CHUNKS)
            info_logger.info(f"Total visited URLs {self.visited_links}")
            # headers = {
            #     'Content-Type': 'application/json'
            # }
            # data = json.dumps(self.results)
            # response = requests.post(f'{RESULT_ENDPOINT + self.user_id}/callback', data=data, headers=headers)
            # if response.status_code == 200:
            #     info_logger.info(f"Request to endpoint for {self.url_to_scrape} URL was successful. Response: {response.status_code}")
            # else:
            #     info_logger.error(f"Request to endpoint for {self.url_to_scrape} URL failed. Status code: {response.status_code} | {response.text}")


class SubProcesses:
    def __init__(self, url_to_scrape=''):
        self.url_to_scrape = url_to_scrape
        self.domain = urlparse(self.url_to_scrape).netloc
        self.custom_headers = {
            'user-agent': USER_AGENT
        }


    def uri_validator(self, url):
        try:
            result = urlparse(url)
            return all([result.scheme, result.netloc])
        except: return False


    def check_crawled_url_status(self, lambda_response: dict, data: dict):
        crawl_url_status, move_forward = False, False
        if 'status_code' in lambda_response.keys():
            status_code = lambda_response['status_code']
            request_url = lambda_response["current_url"]
            if status_code == 200:
                crawl_url_status, move_forward = True, True
            elif status_code in [404, 501]:
                data['broken_links'] = [request_url, status_code]
                info_logger.info(f'Found {status_code}: {request_url}')
                crawl_url_status, move_forward = True, False
            else: # 301, 403, 429
                crawl_url_status, move_forward = True, False

        return crawl_url_status, move_forward, data
  
    
    def extract_sitemap_urls_from_robots_txt(self, robots_txt_content):
        unique_sitemap_urls = set()
        for pattern in ROBOTS_TXT_PATTERNS:
            sitemap_pattern = re.compile(pattern, re.IGNORECASE)
            matches = sitemap_pattern.findall(robots_txt_content)
            {unique_sitemap_urls.add(url.strip()) for url in matches}
        return list(unique_sitemap_urls)


    def get_len_strings_and_position(self, tag_list: SelectorList, target_tag: str, reference_sequence: list):
        list_of_strings_of_tags = []
        for tag in tag_list:
            tag_text = tag
            if tag_text and tag_text.strip():
                tag_text = tag_text.strip()
                list_of_strings_of_tags.append(tag_text)

        length_of_tag_list = len(list_of_strings_of_tags)
        try: tag_position = reference_sequence.index(target_tag) + 1
        except: tag_position = None
        return {'count': length_of_tag_list, 'strings': list_of_strings_of_tags, 'position': tag_position}


    def get_all_ancher_links(self, data: dict, ancher_link_list: List[html.HtmlElement], reference_for_int_links: List[str]):
        currnet_url = data.get('url')
        links_to_crawl = set()
        for link in ancher_link_list:
            try:
                sublink = link.xpath('@href')
                sublink:str = sublink[0] if sublink else ''
                if not sublink: continue
                if not sublink.split('#')[0]: continue
                if not sublink.split('javascript')[0]: continue
                if sublink.startswith('https://web.whatsapp.com'): continue
                if sublink in TRASH_DATA: continue
                if re.compile(URL_IGNORE_PATTERN).search(sublink): continue
                sublink = self.add_schema_domain_in_link(sublink)

                sublink_text = link.xpath('text()')
                if not sublink_text:
                    sublink_text = link.xpath('descendant::text()')
                
                sublink_text = sublink_text[0] if sublink_text else ''
                if sublink_text != None: 
                    sublink_text = sublink_text.strip()
                if sublink_text == None: 
                    sublink_text = ''
                if str(sublink).startswith('/'): 
                    sublink = urljoin(data['url'], sublink)
                
                if 'youtube.com/' in sublink: data['yt_links'].add((sublink, sublink_text))
                elif 'youtu.be/' in sublink: data['yt_links'].add((sublink, sublink_text))

                elif str(sublink).startswith('tel:'): data['contact_links'].add((sublink, sublink_text))
                elif str(sublink).startswith('mailto:'): data['contact_links'].add((sublink, sublink_text))

                elif 'twitter.com/' in sublink: data['social_links'].add((sublink, sublink_text))
                elif 'facebook.com/' in sublink: data['social_links'].add((sublink, sublink_text))
                elif 'instagram.com/' in sublink: data['social_links'].add((sublink, sublink_text))
                elif 'linkedin.com/' in sublink: data['social_links'].add((sublink, sublink_text))
                elif 'wa.me/' in sublink: data['social_links'].add((sublink, sublink_text))

                elif 'goo.gl/maps/' in sublink: data['ext_links'].add((sublink, sublink_text))
                elif 'googl/maps/' in sublink: data['ext_links'].add((sublink, sublink_text))
                elif 'search.google.com/local/writereview' in sublink: data['ext_links'].add((sublink, sublink_text))
                elif (
                    self.domain not in sublink and 
                    self.domain.replace('www.', '') not in sublink and
                    'http' in sublink
                ): 
                    data['ext_links'].add((sublink, sublink_text))

                # Following condition is for removing header and footer links from temp_links list by comparing it with link list of content section of the webpage.
                elif sublink in reference_for_int_links:
                    data['temp_links'].add((sublink, sublink_text))
                    links_to_crawl.add(sublink)
                else:
                    links_to_crawl.add(sublink)

            except Exception as e:
                error_logger.error(f'Target url: {self.url_to_scrape}, sub URL: {currnet_url} In `all_ancher_links` section: {e}')
        return data, links_to_crawl


    def get_all_alt_links(self, alt_links_list: List[html.HtmlElement]):
        unique_alt_links = set()
        for alt_sub_link in alt_links_list:
            url = alt_sub_link.xpath('@src')
            url = url[0].strip() if url else ''
            if url and len(url)>5:
                text = alt_sub_link.xpath('@alt')
                text = text[0].strip() if text else ''
                
                width = alt_sub_link.xpath('@width')
                width = width[0] if width else ''

                height = alt_sub_link.xpath('@height')
                height = height[0] if height else ''

                size = f'{width} x {height}'
                if not width or not height:
                    size = ''

                unique_alt_links.add((url, text, size))
        return unique_alt_links


    def get_all_footer_links(self, footer_links: List[html.HtmlElement]):
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
                           

    def add_schema_domain_in_link(self, link: str):
        if (
            not link.startswith('mailto:') and
            not link.startswith('tel:')
        ):
            if link.startswith('//'): link = 'https:' + link
            if link.startswith('/'): link = self.url_to_scrape.strip('/') + link
            if not link.startswith('http'): link = self.url_to_scrape + link
        return link.strip()
            

if __name__ == '__main__':
    import uuid
    from scrapy.crawler import CrawlerProcess
    from scrapy.utils.project import get_project_settings
    settings = get_project_settings()

    url_to_scrape = 'ADD_YOUR_SEO_WEBSITE'
    session_id = str(uuid.uuid4())
    process = CrawlerProcess(settings)
    process.crawl(OpTeerSpider, url_to_scrape, session_id, 5)
    process.start()
