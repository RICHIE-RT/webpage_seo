from scrapy.http import HtmlResponse
from scrapy.selector.unified import SelectorList
from scrapy.spidermiddlewares.httperror import HttpError
from webpagescraper.log_utility import info_logger, error_logger
# from twisted.internet.error import DNSLookupError
from scrapy.exceptions import CloseSpider
from collections import OrderedDict
from datetime import datetime
from lxml import html
from urllib.parse import urljoin, urlparse
from typing import List # FOR UBUNTU SERVER, ERROR: TypeError: 'type' object is not subscriptable - list[dict]
import copy
import json
import os
import re
import requests
import scrapy
import concurrent.futures as ccf
import time

from dotenv import load_dotenv
load_dotenv()

AWS_URL = os.getenv('AWS_URL')
USER_AGENT = os.getenv('USER_AGENT')
RUN_IN_LOCAL = os.getenv('RUN_IN_LOCAL')
BASE_DIR_OF_LOGGER = os.getenv('BASE_DIR_OF_LOGGER')
BASE_DIR_OF_RESULTS_JSON = os.getenv('BASE_DIR_OF_RESULTS_JSON')
RESULT_ENDPOINT = os.getenv('RESULT_ENDPOINT')
LIMIT_OF_URL = os.getenv('LIMIT_OF_URLS_IN_DEEP_SCRAPING')
if LIMIT_OF_URL is not None:
    LIMIT_OF_URL = int(LIMIT_OF_URL)

TRASH_DATA = ['', '#', 'https://www.instagram.com/', 'https://twitter.com/', 'https://www.facebook.com/', 'https://www.linkedin.com/']
ROBOTS_TAG_PATTERNS = [r'meta name=\'robots\' content=\'([^\']*)\'', r'meta name=\"robots\" content=\"([^\"]*)\"']
AWS_HEADERS = {
    'Content-Type': 'application/json'
}


class OnpageSpider(scrapy.Spider):
    name = 'onpage'
    custom_settings = {
        'CONCURRENT_REQUESTS': 32,
        'DOWNLOAD_TIMEOUT': 10
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
            'robots_content': set(),
            'robots_string': '',
            'sitemap': '',
            'links_301': '',
            'links_404': '',
            'broken_links': list(),
            'canonical': '',
            'mobile': ''
        }
        self.robots_txt_string = ''
        self.footer_links = list()
        self.results = list()
        self.footer_links_scraped = False


    def start_requests(self):
        sub_data = copy.deepcopy(self.meta_data)
        sub_data['url'] = self.url_to_scrape
        try:
            sub_meta_data = {
                'session_id': f'{self.user_id}',
                'data': sub_data,
                'request_url': self.url_to_scrape
            }
            payload = {
                "url": self.url_to_scrape,
                "method": "get",
                "headers": self.custom_headers,
                "payload": "",
                "timeout": 10,
                "proxies": {},
                "response_type": "text"
            }
            yield scrapy.Request(AWS_URL, headers=AWS_HEADERS, body=json.dumps(payload), method="POST", callback=self.parse, errback=self.errback_httpbin, meta=sub_meta_data)
        except Exception as e:
            error_logger.error(f'Target url: {self.url_to_scrape} In `start_requests` section: {e}')


    def parse(self, response: HtmlResponse):
        data = response.meta['data']
        response_html = response.json()
        current_url: str = response_html['request_url']
        redirect_url_status = current_url in self.visited_links
        
        if not current_url.endswith('/'): 
            current_url = current_url + '/'
            info_logger.error(f'Response URL foudn without slash: {current_url} | {response.url}')
            
        self.visited_links.add(current_url)
        data['url'] = current_url
        crawl_url_status, move_forward, data = self.check_crawled_url_status(response_html, data)
        if crawl_url_status and not move_forward:
            self.results.append(data)

        if move_forward and not redirect_url_status:
        # if redirect_url_status:
            try: lambda_response: HtmlResponse = response_html["body"]
            except: lambda_response: HtmlResponse = response.text
            html_response: html.HtmlElement = html.fromstring(lambda_response)
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
                matches = pattern.findall(lambda_response)
                sequence_of_h1_h6_tags = ['h' + match for match in matches]
                unique_sequence_of_h1_h6_tags = list(OrderedDict.fromkeys(sequence_of_h1_h6_tags))

                h1_tag_strings = html_response.xpath('//h1//text()')
                len_strings_position_of_h1_tag = self.get_len_strings_and_position(h1_tag_strings, target_tag='h1', reference_sequence=unique_sequence_of_h1_h6_tags)
                data['h1_tags'] = len_strings_position_of_h1_tag

                h2_tag_strings = html_response.xpath('//h2//text()')
                len_strings_position_of_h2_tag = self.get_len_strings_and_position(h2_tag_strings, target_tag='h2', reference_sequence=unique_sequence_of_h1_h6_tags)
                data['h2_tags'] = len_strings_position_of_h2_tag

                h3_tag_strings = html_response.xpath('//h3//text()')
                len_strings_position_of_h3_tag = self.get_len_strings_and_position(h3_tag_strings, target_tag='h3', reference_sequence=unique_sequence_of_h1_h6_tags)
                data['h3_tags'] = len_strings_position_of_h3_tag

                h4_tag_strings = html_response.xpath('//h4//text()')
                len_strings_position_of_h4_tag = self.get_len_strings_and_position(h4_tag_strings, target_tag='h4', reference_sequence=unique_sequence_of_h1_h6_tags)
                data['h4_tags'] = len_strings_position_of_h4_tag

                h5_tag_strings = html_response.xpath('//h5//text()')
                len_strings_position_of_h5_tag = self.get_len_strings_and_position(h5_tag_strings, target_tag='h5', reference_sequence=unique_sequence_of_h1_h6_tags)
                data['h5_tags'] = len_strings_position_of_h5_tag

                h6_tag_strings = html_response.xpath('//h6//text()')
                len_strings_position_of_h6_tag = self.get_len_strings_and_position(h6_tag_strings, target_tag='h6', reference_sequence=unique_sequence_of_h1_h6_tags)
                data['h6_tags'] = len_strings_position_of_h6_tag

                # all_ancher_links = html_response.xpath('//a') # update - Nov 20, 2023, Only required internal links of content
                all_ancher_links = html_response.xpath('//a')
                raw_reference_for_int_links: List[str] = html_response.xpath('//a[not(ancestor::header) and not(ancestor::footer)]/@href') # Avoid anchor links in the header and footer
                reference_for_int_links = list()
                for link in raw_reference_for_int_links:
                    if link:
                        link = self.add_schema_domain_in_link(link)
                        reference_for_int_links.append(link)
                
            except Exception as e:
                error_logger.error(f'Target url: {self.url_to_scrape} In `title, desc, H-tags and ancher/referece` section: {e}')

            for link in all_ancher_links:
                try:
                    sublink = link.xpath('@href')
                    sublink:str = sublink[0] if sublink else ''
                    if not sublink: continue
                    if not sublink.split('#')[0]: continue
                    if not sublink.split('javascript')[0]: continue
                    if sublink.startswith('https://web.whatsapp.com'): continue
                    if sublink in TRASH_DATA: continue
                    if (
                        '.jpg' in sublink or
                        '.png' in sublink or
                        '.jpeg' in sublink or
                        '.pdf' in sublink or 
                        '.csv' in sublink
                    ): continue
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

                    # else: data['temp_links'].add((sublink, sublink_text))
                    # Following condition is for removing header and footer links from temp_links list by comparing it with link list of content section of the webpage.
                    elif sublink in reference_for_int_links:
                        # if not str(sublink).startswith('https://'):
                        #     sublink = urljoin(data['url'], sublink)
                        data['temp_links'].add((sublink, sublink_text))

                except Exception as e:
                    error_logger.error(f'Target url: {self.url_to_scrape} In `all_ancher_links` section: {e}')

            alt_links = html_response.xpath('//*[@alt]')
            for alt_sub_link in alt_links:
                url = alt_sub_link.xpath('@src')
                url = url[0].strip() if url else ''
                url = self.add_schema_domain_in_link(url)

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

                    data['alt_links'].add((url, text, size))

            # footer_links = list(set(html_response.xpath('//footer//a/@href')))
            # [data['footer_links'].append(foo_link) for foo_link in footer_links if foo_link not in TRASH_DATA if foo_link]
            # try:footer_links = list(set(html_response.xpath('//footer//a')))
            #     for foo_link in footer_links:
            #         foo_sublink = foo_link.xpath('@href')
            #         foo_sublink = foo_sublink[0] if foo_sublink else ''
            #         foo_sublink = self.add_schema_domain_in_link(foo_sublink)

            #         if foo_sublink and foo_link not in TRASH_DATA:
            #             foo_sublink_text = foo_link.xpath('text()')
            #             if not foo_sublink_text:
            #                 foo_sublink_text = foo_link.xpath('descendant::text()')
            #             foo_sublink_text = foo_sublink_text[0] if foo_sublink_text else ''
            #             if foo_sublink_text: foo_sublink_text = foo_sublink_text.strip()
            #             data['footer_links'].add((foo_sublink, foo_sublink_text))
            # except Exception as e:
            #     error_logger.error(f'Target url: {self.url_to_scrape} In `footer_links` section: {e}')

                
            if not self.footer_links_scraped:
                try:
                    footer_links = list(set(html_response.xpath('//footer//a')))                    
                    unique_footer_links = self.get_all_footer_links(footer_links)
                    with ccf.ThreadPoolExecutor(max_workers=20) as executer:
                        executer.map(self.request_on_footer_links, unique_footer_links)
                        
                    self.footer_links_scraped = True
                except Exception as e:
                    error_logger.error(f'Target url: {self.url_to_scrape}, sub URL: {current_url} In `footer_links` section: {e}')
            
            videos = html_response.xpath('//video[@alt]')
            data['video_count'] = len(videos)

            # if 'rel="canonical"' in lambda_response: data['canonical'] = True
            canonical_link = html_response.xpath('//*[@rel="canonical"]/@href')
            if canonical_link:
                data['canonical'] = canonical_link[0]
            
            if html_response.xpath('//*[@lang]/@lang'):
                data['lang_tag'] = True

            check_index = html_response.xpath('//*[@name="robots" and contains(@content, "index")]/@content')
            if not check_index:
                pass
            elif 'noindex' in check_index[0]:
                data['index_type'] = 'noindex'
            else:
                data['index_type'] = 'index'

            schema_jsons = html_response.xpath('//script[@type="application/ld+json"]/text()')
            for schema_json in schema_jsons:
                try:
                    schema_dict =  json.loads(schema_json)
                    data['schema_types'].add(schema_dict['@type'])
                except Exception as e:
                    pass     

            if 'faqs' in lambda_response.lower() or 'Frequently Asked Questions' in lambda_response.lower():
                data['faqs'] = True

            # if 'https://schema.org' in response.text:
            #     data['schema'] = True
            schema_urls = re.findall('https://schema.org', lambda_response)
            data['schema_urls'] = len(schema_urls)

            try:
                for pattern in ROBOTS_TAG_PATTERNS:
                    matching_string = re.findall(pattern, lambda_response)
                    # print(matching_string)
                    if matching_string: [data['robots_content'].add(string) for string in matching_string]
            except Exception as e:
                error_logger.error(f'Target url: {self.url_to_scrape} In `ROBOTS_TAG_PATTERNS` section: {e}')  


            if (
                not (len(self.results) >= self.limit_of_urls)
            ):
                # If scraped url is not `url_to_scrape` then data will be added in result else it will check for `robots.txt` URL.
                if data['url'] != self.url_to_scrape:
                    # self.filter_data_set_to_list(data)
                    self.results.append(data)
                    if RUN_IN_LOCAL:
                        print(data['url'])
                else:
                    robots_url = urljoin(data['url'], 'robots.txt')
                    payload = {
                        "url": robots_url,
                        "method": "get",
                        "headers": self.custom_headers,
                        "payload": "",
                        "timeout": 10,
                        "proxies": {},
                        "response_type": "text"
                    }
                    yield scrapy.Request(AWS_URL, headers=AWS_HEADERS, body=json.dumps(payload), method="POST", callback=self.robots_sitemap_parser, errback=self.errback_httpbin, meta=response.meta)

                # Crawling starts here for URLs inside scraped URL.
                next_check_url = list(data['temp_links'])
                for link in next_check_url[:LIMIT_OF_URL]:
                    if link[0] not in self.visited_links:
                        sub_data = copy.deepcopy(self.meta_data)
                        try:
                            # sub_data['url'] = link[0]
                            sub_meta_data = {
                                'session_id': f'{self.user_id}',
                                'data': sub_data,
                                'request_url': link[0]
                            }
                            payload = {
                                "url": link[0],
                                "method": "get",
                                "headers": self.custom_headers,
                                "payload": "",
                                "timeout": 10,
                                "proxies": {},
                                "response_type": "text"
                            }
                            yield scrapy.Request(AWS_URL, headers=AWS_HEADERS, body=json.dumps(payload), method="POST", callback=self.parse, errback=self.errback_httpbin, meta=sub_meta_data)
                        except Exception as e:
                            error_logger.error(f'Target URL: {self.url_to_scrape}, sub URL: {link[0]} In `looping of next_urls` section: {e}')

            else:
                raise CloseSpider('Stopping spider: Required result scraped')


    def check_crawled_url_status(self, lambda_response: dict, data: dict):
        crawl_url_status, move_forward = False, False
        if 'statusCode' in lambda_response.keys():
            status_code = lambda_response['statusCode']
            request_url = lambda_response["request_url"]
            if status_code == 200:
                crawl_url_status, move_forward = True, True
            elif status_code in [404, 501]:
                data['broken_links'] = [request_url, status_code]
                info_logger.info(f'Found {status_code}: {request_url}')
                crawl_url_status, move_forward = True, False
            else: # 301, 403, 429
                crawl_url_status, move_forward = True, False

        return crawl_url_status, move_forward, data
    

    def robots_sitemap_parser(self, response: HtmlResponse):
        try: html_response: HtmlResponse = response.json()["body"]
        except: html_response: HtmlResponse = response.text
        html_response_res: html.HtmlElement = html.fromstring(html_response)

        try:
            data = response.meta['data']
            data['robots'] = True

            robots_string_text = html_response_res.xpath('//body/p/text()')
            self.robots_txt_string = robots_string_text[0] if robots_string_text else ''

            pattern = r'(https?://\S+sitemap?[/_a-zA-Z0-9.-]+)'
            if 'sitemap' in html_response:
                match = re.search(pattern, html_response)
                if match:
                    sitemap_url = match.group(1)
                    data['sitemap'] = sitemap_url

            # self.filter_data_set_to_list(data)
            self.results.append(data)
            if RUN_IN_LOCAL:
                print("In robots_sitemap_parser", data['url'])
                # yield response.meta
        except Exception as e:
            error_logger.error(f'Target url: {self.url_to_scrape} In `robots_sitemap_parser` function: {e}')
            if RUN_IN_LOCAL:
                print('In robots:', e)


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


    def filter_temp_links_to_int_links(self, raw_data: list):
        try:
            check_data = dict()
            for raw in raw_data:
                check_data[raw['url']] = {"temp_links": raw['temp_links']}

            for key, value in check_data.items():
                check_data[key]['int_links'] = list()
                for key2, value2 in check_data.items():
                    if key == key2: continue
                    for link in value2['temp_links']:
                        if key == link[0]:
                            check_data[key]['int_links'].append([key2, link[1]])
                        
            for raw in raw_data:
                raw['int_links'] = check_data[raw['url']]['int_links']
                raw['robots_string'] = self.robots_txt_string
                raw['footer_links'] = self.footer_links
                del raw['temp_links']

        except Exception as e:
            error_logger.error(f'Target url: {self.url_to_scrape} In `filter_temp_links_to_int_links` function: {e}')
        return raw_data


    def add_schema_domain_in_link(self, link: str):
        if (
            not link.startswith('mailto:') and
            not link.startswith('tel:')
        ):
            if link.startswith('//'): link = 'https:' + link
            if link.startswith('/'): link = self.url_to_scrape.strip('/') + link
            if not link.startswith('https:'): link = self.url_to_scrape + link
        return link.strip()


    def request_on_footer_links(self, unique_footer_link: tuple):
        try:
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
            if self.uri_validator(foo_sublink):
                # AWS_URL = f'http://localhost:3000/url/request?url={quote(foo_sublink)}'
                # yield scrapy.Request(AWS_URL, headers=self.custom_headers, callback=self.get_footer_links, meta=footer_link_meta)
                AWS_URL = "https://enuo60zivi.execute-api.ca-central-1.amazonaws.com/default/lambda_requests"
                response = requests.request('POST', AWS_URL, headers=AWS_HEADERS, data=json.dumps(payload))

                try:
                    lambda_response = response.json()
                    status_code = lambda_response['statusCode']
                    self.footer_links.append((foo_sublink, foo_sublink_text, status_code))
                except Exception as e:
                    self.footer_links.append((foo_sublink, foo_sublink_text, ''))
            else:
                self.footer_links.append((foo_sublink, foo_sublink_text, ''))
        except Exception as e:
            print(e, '##############################')
                
    
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


    def uri_validator(self, url):
        try:
            result = urlparse(url)
            return all([result.scheme, result.netloc])
        except: return False


    def errback_httpbin(self, failure: HtmlResponse):
        data = failure.request.meta['data']
        request_url = failure.request.meta['request_url']
        check_url = 'robots.txt' in failure.request.url
        if check_url:
            data['robots'] = False

            # self.filter_data_set_to_list(data)
            self.results.append(data)
            # if RUN_IN_LOCAL:
            #     print(failure.request.url)
                # yield failure.request.meta

        elif failure.check(HttpError):
            # you can get the non-200 response
            response = failure.value.response

            # if response.status == 301:
            #     data['links_301'].add(request_url)
            #     info_logger.info(f'Found 301: {request_url}')
            # elif response.status == 404:
            #     data['links_404'].add(request_url)
            #     info_logger.info(f'Found 404: {request_url}')
            # else:
            #     data['broken_links'].add(request_url)
            #     info_logger.info(f'Found broken {response.status}: {request_url}')

            if response.status == 404 or response.status == 501:
                data['broken_links'] = [request_url, response.status]
                info_logger.info(f'Found {response.status}: {request_url}')
            
            # self.filter_data_set_to_list(data)
            self.results.append(data) #No need of failer links: updated at 02 Nov 2023
            if RUN_IN_LOCAL:
                print(response.status, request_url)
                # yield html_response.request.meta #No need of failer links: updated at 02 Nov 2023

        # # elif failure.check(DNSLookupError):
        # #     # this is the original request
        # #     request = html_response.request


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
            time.sleep(10)
            headers = {
                'Content-Type': 'application/json'
            }
            data = json.dumps(self.results)
            response = requests.post(f'{RESULT_ENDPOINT + self.user_id}/callback', data=data, headers=headers)
            info_logger.info(f"Total visited URLs {self.visited_links}")
            if response.status_code == 200:
                info_logger.info(f"Request to endpoint for {self.url_to_scrape} URL was successful. Response: {response.status_code}")
            else:
                info_logger.info(f"Request to endpoint for {self.url_to_scrape} URL failed. Status code: {response.status_code} | {response.text} | Trying one more time")
                response = requests.post(f'{RESULT_ENDPOINT + self.user_id}/callback', data=data, headers=headers)
                if response.status_code == 200:
                    info_logger.info(f"Request to endpoint for {self.url_to_scrape} URL was successful. Response: {response.status_code} | Succeded in second try")
                else:
                    info_logger.info(f"Request to endpoint for {self.url_to_scrape} URL failed. Status code: {response.status_code} | {response.text} | Failed second time")
            

if __name__ == '__main__':
    from scrapy.crawler import CrawlerRunner
    url_to_scrape = 'https://www.rennovate.co.in'
    crawl_runner = CrawlerRunner()
    crawl_runner.crawl(OnpageSpider, url_to_scrape,  'uuid')