import faust
import os

from records import RssFeed
from pymongo import MongoClient

broker_host = os.environ["BROKER_HOST"] if os.environ["BROKER_HOST"] else "localhost:9092"
app = faust.App('topic_consumer', broker=f'kafka://{broker_host}')
topic = app.topic('rss', value_type=RssFeed)

user = os.environ["MONGO_INITDB_ROOT_USERNAME"]
pw = os.environ["MONGO_INITDB_ROOT_PASSWORD"]
mongo_host = os.environ["MONGO_HOST"] if os.environ["MONGO_HOST"] else "localhost"
client = MongoClient(mongo_host, 27017, username=user, password=pw)
db = client['data']
collection = db['rss.articles']


@app.agent(topic)
async def process_rss_feeds(feeds):
    async for feed in feeds:
        collection.insert_one(feed.asdict())
