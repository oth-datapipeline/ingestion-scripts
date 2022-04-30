import datetime
import faust
import newspaper
import os
import pymongo
import traceback

from records import RssFeed
from pymongo import MongoClient
from bs4 import BeautifulSoup
from newspaper import Article
from newspaper import Config
from ingestion_logger import get_ingestion_logger


import nltk
nltk.download('punkt')

user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:99.0) Gecko/20100101 Firefox/99.0'

# Newspaper3k Config
config = newspaper.Config()
config.browser_user_agent = user_agent
config.request_timeout = 5

# define faust app
broker_host = os.environ["BROKER_HOST"] if "BROKER_HOST" in os.environ.keys() else "localhost:9092"
app = faust.App('rss_consumer', broker=f'kafka://{broker_host}')

logger, log_handler = get_ingestion_logger("rss")
app.logger.addHandler(log_handler)

# define topics
rss = app.topic('rss', value_type=RssFeed)
rss_filtered = app.topic('rss_filtered', value_type=RssFeed)
rss_with_content = app.topic('rss_with_content', value_type=RssFeed)
rss_without_content = app.topic('rss_without_content', value_type=RssFeed)
full_rss = app.topic("full_rss", value_type=RssFeed)
# define mongo db client
user = os.environ["MONGO_INITDB_ROOT_USERNAME"]
pw = os.environ["MONGO_INITDB_ROOT_PASSWORD"]
mongo_host = os.environ["MONGO_HOST"] if "MONGO_HOST" in os.environ else "localhost"
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


def fetch_links():
    return list(map(lambda link: link['link'], list(collection.find({}, {'link': 1, '_id': 0}))))

links = fetch_links()

@app.timer(interval=36000.0)
def set_links():
	links = fetch_links()

@app.agent(rss)
async def remove_old_articles(feeds, concurrency=4):
    async for feed in feeds:
        if feed.link not in links:
            await rss_filtered.send(value=feed)
        else: 
            logger.info(f"Discared Article {feed.link} as Duplicate")


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
            logger.info(f"Fetched Content for Article {feed.link}")
        except newspaper.article.ArticleException:
            logger.warn(f"ArticleException: Could not fetch content from {link}")
        except Exception:
            logger.warn(traceback.format_exc())


@app.agent(rss_without_content, concurrency=4)
async def fill_content(feeds):
    async for feed in feeds:
        try:
            # send to rss_wo_content
            soup = BeautifulSoup(feed.content, features="html.parser")
            # remove all script and style elements
            map(lambda script: script.extract(), soup(["script", "style", "a", "img"]))

            text = soup.get_text()
            feed.content = text
            article = Article("")
            article.download(input_html=text)
            article.nlp()
            feed.tags = article.keywords
            feed.summary = article.summary
            # send to rss_w_content
            await rss_with_content.send(value=feed)
            logger.info(f"Filled Content for Article {feed.link}")
        except newspaper.article.ArticleException:
            logger.warn("ArticleException: Could not fill content")
        except Exception:
            logger.warn(traceback.format_exc())


@app.agent(rss_with_content, concurrency=4)
async def fill_summary_if_missing(feeds):
    async for feed in feeds:
        try:
            article = Article("")
            # clean existing summary from tags
            if (feed.summary):
                soup = BeautifulSoup(feed.summary, features="html.parser")

                # remove all script and style elements
                map(lambda script: script.extract(), soup(["script", "style", "a", "img"]))

                text = soup.get_text()
                feed.summary = text
            # summarize article if no summary exists
            else:
                article.download(input_html=feed.content)
                article.parse()
                article.nlp()
                feed.summary = article.summary
            logger.info(f"Summarized Article {feed.link}")
        except Exception:
            logger.warn(traceback.format_exc())
        await full_rss.send(value=feed)


@app.agent(full_rss, concurrency=1)
async def write_feed_to_mongo(feeds):
    # with client.start_session() as session:
    async for feed in feeds:
        try:
            # convert datetime-string to datetime-object
            if feed.published_parsed:
                feed.published = datetime.datetime(*feed.published_parsed[:-2])
            elif feed.published:
                timezone_string = feed.published.split(" ")[-1]
                timezone_char = "%z" if any(c.isdigit() for c in timezone_string) else "%Z"
                published_date = datetime.datetime.strptime(feed.published, f"%a, %d %b %Y %H:%M:%S {timezone_char}")
                feed.published = published_date
            insert_date = datetime.datetime.now()
            collection.insert_one({**feed.asdict(), "insert_date" : insert_date})
            logger.info(f"Inserted Article {feed.link}")
        except Exception:
            logger.warn(traceback.format_exc())
