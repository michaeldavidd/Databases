#Import statements
import mechanicalsoup as ms
from elasticsearch import Elasticsearch
import redis
import configparser
from neo4j import GraphDatabase
from dotenv import load_dotenv
import os

#Webcrawler instance
rediser = redis.Redis()
url = "https://en.wikipedia.org/wiki/Redis"
rediser.lpush("links", url)
browser = ms.StatefulBrowser()
confpars = configparser.ConfigParser()
confpars.read('example.ini')
elasti = Elasticsearch(cloud_id=confpars['ELASTIC']['cloud_id'], basic_auth=(confpars['ELASTIC']['user'], confpars['ELASTIC']['password']))

#Connect to Neo4j
class Neo4JConnector:

    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def add_links(self, page, links):
        with self.driver.session() as session:
           session.execute_write(self._create_links, page, links)

    def flush_data(self):
        with self.driver.session() as session:
           session.execute_write(self._flush_db)

    def write_1(self):
        with self.driver.session() as session:
           session.execute_write(self._write_1)
    
    @staticmethod
    def _create_links(tx, page, links):
        page = page.decode('utf-8')
        print("page ", page)
        tx.run("MERGE (p:Page {url: $page})", page=page)
        for link in links:
            print(link)
            tx.run("MERGE (p:Page {url: $page}) MERGE (l:Page {url: $link}) MERGE (p)-[:LINKS_TO]->(l)", link=str(link), page=page)

    @staticmethod
    def _flush_db(tx):
        tx.run("MATCH (a) -[r]-> () DELETE a, r")
        tx.run("MATCH (a) DELETE a")

    @staticmethod
    def _write_1(tx):
        tx.run("CREATE (:Page)")

load_dotenv()
neo4j_connector = Neo4JConnector("bolt://127.0.0.1:7689","neo4j", os.getenv('neo4j_password'))
neo4j_connector.flush_data()

def elastic_search_write(elasti, url, html):
    url = url.decode('utf-8')
    elasti.index (index= 'webpages',document = {'url': url,'html': html })

def crawl(link, rediser, browser, elasti, connector):
    browser.open(link)

    elastic_search_write(elasti, link, str(browser.page))
    a_tags = browser.page.find_all("a")
    hrefs = [a.get("href") for a in a_tags]
    wikipedia_domain = "https://en.wikipedia.org"
    links = [wikipedia_domain + a for a in hrefs if a and a.startswith("/wiki/")]
    # create a linked list in redis called "links"
    rediser.lpush("links", *links)
    connector.add_links(link, links)

count = 0

while link := rediser.rpop("links"):
    if "jesus" in str(link) or count == 10:
        elasti.indices.refresh(index='webpages')
        break
    crawl(link, rediser, browser, elasti, neo4j_connector)
    count += 1

result = elasti.search(index='webpages', body={'query': {'match': {'html': "html"}}})

#########Modifications###################

from bs4 import BeautifulSoup
import sqlite3

result = elasti.search(index='webpages', body={'query': {'match': {'html': "html"}}})

def extract_and_store_data(html_content, sqlite_conn, elasti):
    soup = BeautifulSoup(html_content, 'html.parser')
    # Example data extraction: extracting article text
    article_text = soup.find('div', {'id': 'content'}).get_text()

    #Data into SQLite
    cursor = sqlite_conn.cursor()
    cursor.execute("INSERT INTO articles (text) VALUES (?)", (article_text,))
    sqlite_conn.commit()

    elasti.index(index='extracted_data', document={'text': article_text})

#SQLite connection
sqlite_connection = sqlite3.connect('extracted_data.db')
sqlite_connection.execute("CREATE TABLE IF NOT EXISTS articles (text TEXT)")

#Store data per page
for hit in result['hits']['hits']:
    html_content = hit['_source']['html']
    extract_and_store_data(html_content, sqlite_connection, elasti)

# Close SQLite connection
sqlite_connection.close()
