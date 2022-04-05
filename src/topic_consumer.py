import faust
import os
import requests
import logging

from records import RssFeed
from pymongo import MongoClient
from bs4 import BeautifulSoup

app = faust.App('topic_consumer', broker='kafka://localhost:9092')
topic = app.topic('rss', value_type=RssFeed)

user = os.environ["MONGO_INITDB_ROOT_USERNAME"]
pw = os.environ["MONGO_INITDB_ROOT_PASSWORD"]
client = MongoClient("localhost", 27017, username=user, password=pw)
db = client['test']
collection = db['rss.content_test']

def remove_old_articles(feed):
    #TODO: fetch last update date from mongo and remove articles from stream that are older than that
    pass

async def fill_content_if_missing(feed):
    if not feed.content:
        try: 
            link = feed.link
            res = requests.get(link, timeout=3)
            feed.content = res.text
        except requests.exceptions.HTTPError as err:
            logging.warn(f"Could not fetch content from {link}")
        except requests.exceptions.Timeout as err:
            logging.warn(f"Timeout while fetching content from {link}")
    return feed    
       
def remove_html_tags(feed):
    soup = BeautifulSoup(feed.content, "html.parser")
    for data in soup(['style', 'script', 'head']):
        data.decompose()
        feed.content = ' '.join(soup.stripped_strings)
    return feed

s = app.stream(topic, processors = [fill_content_if_missing, remove_html_tags])

@app.task       
async def write_feed_to_mongo():
    async for feed in s:
        collection.insert_one(feed.asdict())
