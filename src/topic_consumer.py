import faust
import os
import requests
import logging
import newspaper
import traceback
import pymongo

from records import RssFeed
from pymongo import MongoClient
from bs4 import BeautifulSoup
from newspaper import Article
import nltk
nltk.download('punkt')

app = faust.App('topic_consumer', broker='kafka://localhost:9092')
rss = app.topic('rss', value_type=RssFeed)
# rss_stream = app.stream(rss)

rss_filtered = app.topic('rss_filtered', value_type=RssFeed)

user = os.environ["MONGO_INITDB_ROOT_USERNAME"]
pw = os.environ["MONGO_INITDB_ROOT_PASSWORD"]
client = MongoClient("localhost", 27017, username=user, password=pw)
db = client['data']
collection = db['rss.articles']
collection.create_index([('link', pymongo.TEXT)], name='link_index', unique=True)
links = list(map(lambda link: link['link'], list(collection.find({}, {'link':1, '_id':0}))))


@app.timer(interval=36000.0)
async def fetch_links():
    links = list(map(lambda link: link['link'], list(collection.find({}, {'link':1, '_id':0}))))
    
@app.agent(rss) 
async def remove_old_articles(feeds):
    async for feed in feeds:
        if not feed.link in links:
            await rss_filtered.send(value=feed)

async def fill_content(feed):
    try:   
        link = feed.link
        article = Article(link)
        article.download()
        article.parse()
        feed.content = article.text
        if (not feed.content):
            soup = BeautifulSoup(feed.content, features="html.parser")
            # remove all script and style elements
            for script in soup(["script", "style", "a", "img"]):
                script.extract()
            text = soup.get_text()
            feed.content = text
            article.download(input_html=text)
        article.nlp()
        feed.tags = article.keywords
        feed.summary = article.summary
    except newspaper.article.ArticleException as err:
            logging.warn(f"Could not fetch content from {link}")
    return feed    

async def fill_summary_if_missing(feed):
    try:   
        link = feed.link
        article = Article("")
        # clean existing summary from tags
        if (feed.summary):
            soup = BeautifulSoup(feed.summary, features="html.parser")
            
            # remove all script and style elements
            for script in soup(["script", "style", "a", "img"]):
                script.extract()

            text = soup.get_text()
            feed.summary = text
        # summarize article if no summary exists
        else: 
            article.download(input_html=feed.content)
            article.parse()
            article.nlp()
            feed.summary = article.summary
    except Exception as err:
            logging.warn(f"Could not summarize article: {link}")
            traceback.print_exc()
    return feed  

s = app.stream(rss_filtered, processors = [fill_content, fill_summary_if_missing])

@app.task       
async def write_feed_to_mongo():
    async for feed in s:
        try:
            collection.insert_one(feed.asdict())
        except Exception:
            continue