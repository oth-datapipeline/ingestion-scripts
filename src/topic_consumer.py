import faust
import os

from records import RssFeed
from pymongo import MongoClient


app = faust.App('topic_consumer', broker='kafka://localhost:9092')
topic = app.topic('rss', value_type=RssFeed)

user = os.environ["MONGO_INITDB_ROOT_USERNAME"]
pw = os.environ["MONGO_INITDB_ROOT_PASSWORD"]
client = MongoClient("localhost", 27017, username=user, password=pw)
db = client['data']
collection = db['rss.articles']

@app.agent(topic)
async def process_rss_feeds(feeds): 
    async for feed in feeds:
        collection.insert_one(feed.asdict())

    
