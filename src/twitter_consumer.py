from records import Tweet
from ingestion_logger import get_ingestion_logger

from pymongo import MongoClient
import datetime
import emoji
import faust
import os
import pymongo
import re
import traceback



# define faust app
broker_host = os.environ["BROKER_HOST"] if "BROKER_HOST" in os.environ.keys() else "localhost:9092"
app = faust.App('twitter_consumer', broker=f'kafka://{broker_host}')

logger, log_handler = get_ingestion_logger("twitter")
app.logger.addHandler(log_handler)

# define topics
twitter = app.topic('twitter', value_type=Tweet)
tweets_without_emojis = app.topic('tweet_no_emoji', value_type=Tweet)
final_tweets = app.topic('final_tweet', value_type=Tweet)

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
collection = db['twitter.tweets']
collection.create_index([('tweet_id', pymongo.ASCENDING)], name='tweet_index', unique=True)



@app.agent(twitter)
async def demojify_tweet(tweets, concurrency=4):
    async for tweet in tweets:
        try:
            tweet.text = emoji.demojize(tweet.text, language="en")
            logger.info(f"Removed Emojis from Tweet {tweet.tweet_id}")
            await tweets_without_emojis.send(value=tweet)
        except Exception:
            logger.warn(traceback.format_exc())


@app.agent(tweets_without_emojis)
async def tag_tweet(tweets, concurrency=4):
    async for tweet in tweets:
        try:
            #add hashtags
            pattern = r"#(\w+)"
            hashtags = re.findall(pattern, tweet.text)
            tweet.hashtags = hashtags
            logger.info(f"Extracted Hashtags for Tweet {tweet.tweet_id}")
            await final_tweets.send(value=tweet)
        except Exception:
            logger.warn(traceback.format_exc())


@app.agent(final_tweets)
async def insert_tweet(tweets, concurrency=1):
    async for tweet in tweets:
        try:
            tweet.created_at = datetime.datetime.strptime(tweet.created_at, "%Y-%m-%d %H:%M:%S%z")
            insert_date = datetime.datetime.now()
            collection.insert_one({**tweet.asdict(), "insert_date": insert_date})
            logger.info(f"Inserted Tweet {tweet.tweet_id} into MongoDB")
        except Exception:
            logger.warn(traceback.format_exc())