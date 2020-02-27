import scrapy
import json
import urltools
import re
from bs4 import BeautifulSoup
from scrapy import Request
import requests
from scrapy.exceptions import CloseSpider
import time

def info_extractor(soup):
    r_date = ''
    budget = ''
    g_usa = ''
    runtime = ''
   
    for h in soup.find_all('h4',class_='inline'):
      if h.get_text() == 'Release Date:':
        r_date = h.parent.find_all(text=True)[2].strip()

      if h.get_text() == 'Budget:':
        budget = h.parent.find_all(text=True)[2].strip()
       

      if h.get_text() == 'Gross USA:':
        g_usa = h.parent.find_all(text=True)[2].strip()
      

      if h.get_text() == 'Runtime:':
        runtime = h.parent.find_all(text=True)[3].strip()
     
    return r_date,budget,g_usa,runtime

def unique_id_generator(url):
    url = url.split('/')
    return url[-2]

class imdb_spider(scrapy.Spider):
    name = 'scifi_movies'
    url = ['https://www.imdb.com/search/title/?genres=sci-fi&start=1&explore=title_type,genres']
    for it in range(51,5000,50):
        url.append('https://www.imdb.com/search/title/?genres=sci-fi&start={}&explore=title_type,genres'.format(it))
    parse = urltools.parse(url[0])
    domain = parse.domain
    URLS_CRAWLED = []
    start_time = time.time()
    custom_settings = {
        'DEPTH_PRIORITY' : 1,
        'SCHEDULER_DISK_QUEUE' : 'scrapy.squeues.PickleFifoDiskQueue',
        'SCHEDULER_MEMORY_QUEUE' : 'scrapy.squeues.FifoMemoryQueue',
        'DUPEFILTER_CLASS': 'scrapy.dupefilters.RFPDupeFilter'
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
                r_date,budget,g_usa,runtime = info_extractor(soup)
                _id = unique_id_generator(response.url)
                    
                yield {"id":_id,"url":response.url,"timestamp_crawl":time.time(),
                       "title":title,"release_date":r_date,"budget":budget,"gross_usa":g_usa,
                       "runtime":runtime}
                return

        for url in response.css("a::attr(href)"):

            u = url.get()
            if re.search('^/title',u):
                next_page = response.urljoin(u)
                if next_page not in self.URLS_CRAWLED:
                    next_page_parse = urltools.parse(next_page)
                    if next_page_parse.domain == self.domain:
                        self.URLS_CRAWLED.append(next_page)
                        yield (Request(next_page, callback = self.imdb_parser))
            
                
            
