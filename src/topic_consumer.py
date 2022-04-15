import faust
import os
import logging
import newspaper
import traceback
import pymongo

from records import RssFeed
from pymongo import MongoClient
from bs4 import BeautifulSoup
from newspaper import Article
from newspaper import Config
import nltk
nltk.download('punkt')

user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:99.0) Gecko/20100101 Firefox/99.0'

config = Config()
config.browser_user_agent = user_agent
config.request_timeout = 5

# define faust app
broker_host = os.environ["BROKER_HOST"] if os.environ["BROKER_HOST"] else "localhost:9092"
app = faust.App('topic_consumer', broker=f'kafka://{broker_host}')

# define topics
rss = app.topic('rss', value_type=RssFeed)
rss_filtered = app.topic('rss_filtered', value_type=RssFeed)
rss_with_content = app.topic('rss_with_content', value_type=RssFeed)
rss_without_content = app.topic('rss_without_content', value_type=RssFeed)
full_rss = app.topic("full_rss", value_type=RssFeed)

# define mongo db client
user = os.environ["MONGO_INITDB_ROOT_USERNAME"]
pw = os.environ["MONGO_INITDB_ROOT_PASSWORD"]
mongo_host = os.environ["MONGO_HOST"] if os.environ["MONGO_HOST"] else "localhost"
client = MongoClient(
    mongo_host,
    27017,
    username=user,
    password=pw,
    serverSelectionTimeoutMS=5000,
    socketTimeoutMS=5000,
    waitQueueTimeoutMS=5000)

db = client['data']
collection = db['rss.articles']
collection.create_index([('link', pymongo.ASCENDING)], name='link_index', unique=True)


@app.timer(interval=36000.0)
def fetch_links():
    return list(map(lambda link: link['link'], list(collection.find({}, {'link': 1, '_id': 0}))))


links = fetch_links()


@app.agent(rss)
async def remove_old_articles(feeds):
    async for feed in feeds:
        if feed.link not in links:
            await rss_filtered.send(value=feed)


@app.agent(rss_filtered, concurrency=4)
async def fetch_content(feeds):
    async for feed in feeds:
        try:
            link = feed.link
            article = Article(link, config=config)
            article.download()
            article.parse()
            feed.content = article.text
            if (not feed.content):
                feed.content = article.html
                await rss_without_content.send(value=feed)
            else:
                article.nlp()
                feed.tags = article.keywords
                feed.summary = article.summary
                await rss_with_content.send(value=feed)

        except newspaper.article.ArticleException:
            logging.warn(f"Could not fetch content from {link}")


@app.agent(rss_without_content)
async def fill_content(feeds):
    async for feed in feeds:
        try:
            # send to rss_wo_content
            soup = BeautifulSoup(feed.content, features="html.parser")
            # remove all script and style elements
            for script in soup(["script", "style", "a", "img"]):
                script.extract()
            text = soup.get_text()
            feed.content = text
            article = Article("")
            article.download(input_html=text)
            article.nlp()
            feed.tags = article.keywords
            feed.summary = article.summary
            # send to rss_w_content
            await rss_with_content.send(value=feed)

        except newspaper.article.ArticleException:
            logging.warn("Could not fill content")


@app.agent(rss_with_content)
async def fill_summary_if_missing(feeds):
    async for feed in feeds:
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
        except Exception:
            logging.warn(f"Could not summarize article: {link}")
            traceback.print_exc()
        await full_rss.send(value=feed)


@app.agent(full_rss, concurrency=4)
async def write_feed_to_mongo(feeds):
    # with client.start_session() as session:
    async for feed in feeds:
        try:
            collection.insert_one(feed.asdict())
        except Exception:
            traceback.print_exc()
