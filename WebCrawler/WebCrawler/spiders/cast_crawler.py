import scrapy
import json
import urltools
import re
from bs4 import BeautifulSoup
from scrapy import Request
import requests
from scrapy.exceptions import CloseSpider
import time
from datetime import datetime

def info_extractor(soup):
    b_date = ''
    b_place = ''
    d_date = ''
    d_place = ''
    for h in soup.find_all('h4',class_='inline'):
      if h.get_text() == 'Born:':
        b_date = h.parent.find_all(text=True)[4].strip() + ' ,' + h.parent.find_all(text=True)[6].strip()
        b_place = h.parent.find_all(text=True)[-2].strip()

      

      if h.get_text() == 'Died:':
        d_date = h.parent.find_all(text=True)[4].strip() + ' ,' + h.parent.find_all(text=True)[6].strip()
        d_place = h.parent.find_all(text=True)[-2].strip()

    return b_date,b_place,d_date,d_place

def unique_id_generator(url):
    url = url.split('/')
    return url[-2]

class imdb_spider(scrapy.Spider):
    name = 'cast_scraper'
    url = ['https://www.imdb.com/search/name/?gender=male,female&ref_=rlm']
    for it in range(51,5000,50):
        url.append('https://www.imdb.com/search/name/?gender=male,female&start={}&ref_=rlm'.format(it))
    parse = urltools.parse(url[0])
    domain = parse.domain
    URLS_CRAWLED = []
    start_time = time.time()
    custom_settings = {
        'DEPTH_PRIORITY' : 1,
        'SCHEDULER_DISK_QUEUE' : 'scrapy.squeues.PickleFifoDiskQueue',
        'SCHEDULER_MEMORY_QUEUE' : 'scrapy.squeues.FifoMemoryQueue',
        'DUPEFILTER_CLASS': 'scrapy.dupefilters.RFPDupeFilter',
    }
    def start_requests(self):
        for url in self.url:
            yield scrapy.Request(url,callback = self.imdb_parser) 

    def imdb_parser(self,response):

        end_time = time.time()
        u = urltools.parse(response.url)
        
        if response.status == 200:
            if response.meta['depth'] == 1:
                soup = BeautifulSoup(response.text,'html.parser')
                title = soup.find('title').get_text()
                b_date,b_place,d_date,d_place = info_extractor(soup)
                _id = unique_id_generator(response.url)
                timestamp_crawl = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                yield {"id":_id,"url":response.url,"timestamp_crawl":timestamp_crawl,
                       "name":title,"date_of_birth":b_date,"place_of_birth":b_place,"date_of_death":d_date,
                       "place_of_death":d_place}
                return
            
        for url in response.css("a::attr(href)"):

            u = url.get()
            if re.search('^/name',u):
                next_page = response.urljoin(u)
                if next_page not in self.URLS_CRAWLED:
                    next_page_parse = urltools.parse(next_page)
                    if next_page_parse.domain == self.domain:
                        self.URLS_CRAWLED.append(next_page)
                        yield (Request(next_page, callback = self.imdb_parser))
            
