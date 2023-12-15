import mechanicalsoup
import pandas as pd
import sqlite3
import redis
from elasticsearch import Elasticsearch

def index_to_elasticsearch(url, html, es):
    document = {
        "url": url,
        "html": html
    }
    es.index(index='web_crawled_data', body=document)

def crawl_and_index(url, r, es):
    browser = mechanicalsoup.StatefulBrowser()
    browser.open(url)

    #Get HTML content
    raw_html = str(browser.page)

    #Index document
    index_to_elasticsearch(url, raw_html, es)

    #Parsing
    links = browser.page.find_all("a", href=True)
    for link in links:
        # Add found links to Redis Queue
        r.lpush('link_queue', link['href'])

    th = browser.page.find_all("th", attrs={"class": "table-rh"})

    #Recursion!
    next_link = r.rpop('link_queue')
    if next_link:
        crawl_and_index(next_link, r, es)

#Root URL
starting_url = "https://en.wikipedia.org/wiki/Comparison_of_Linux_distributions"
r = redis.StrictRedis(host='localhost', port=6379, db=0)

#Connecting Elasticsearch
es = Elasticsearch('<Your_Elasticsearch_URL>')

crawl_and_index(starting_url, r, es)
