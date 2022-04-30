import datetime
import emoji
import faust
import os
import pymongo
import traceback

from pymongo import MongoClient
from ingestion_logger import get_ingestion_logger
from records import RedditPost

import nltk
nltk.download('stopwords')
from nltk.corpus import stopwords

# define faust app
broker_host = os.environ["BROKER_HOST"] if "BROKER_HOST" in os.environ.keys() else "localhost:9092"
app = faust.App('reddit_consumer', broker=f'kafka://{broker_host}')

logger, log_handler = get_ingestion_logger("reddit")
app.logger.addHandler(log_handler)

# define topics
reddit = app.topic('reddit', value_type=RedditPost)
posts_without_emojis = app.topic('post_no_emoji', value_type=RedditPost)
final_post = app.topic('final_post', value_type=RedditPost)

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
collection = db['reddit.posts']
collection.create_index([('id', pymongo.ASCENDING), ('insert_date', pymongo.ASCENDING)], name='tweet_index', unique=True)

# set up RAKE for Keyword Extraction
r = Rake()

@app.agent(reddit)
async def demojify_post(posts, concurrency=4):
    async for post in posts:
        try:
            comment_list = post.comments
            for comment in comment_list:
                comment["text"] = emoji.demojize(comment["text"], language="en")
            post.comments = comment_list
            logger.info(f"Cleaned comments of Post {post.id}")
            await posts_without_emojis.send(value=post)
        except Exception:
            logger.warn(traceback.format_exc())

@app.agent(posts_without_emojis)
async def tag_post(posts, concurrency=4):
    async for post in posts:
        try:
            comments = [comment["text"] for comment in post.comments]
            r.extract_keywords_from_sentences(comments)
            unfiltered_kws = r.get_ranked_phrases_with_scores()
            post.keywords = [pair[1] for pair in filter(lambda tup: tup[0] >= 3.0, unfiltered_kws)]
            logger.info(f"Extracted Keywords for Post {post.id}")
            await final_post.send(value=tweet)
        except Exception:
            logger.warn(traceback.format_exc())


@app.agent(final_post)
async def insert_post(posts, concurrency=1):
    async for post in posts:
        try:
            post.created = datetime.datetime.strptime(post.created, "%Y-%m-%d %H:%M:%S")
            insert_date = datetime.datetime.now()
            collection.insert_one({**post.asdict(), "insert_date" : insert_date})
            logger.info(f"Inserted Post {post.id} into MongoDB")
        except Exception:
            logger.warn(traceback.format_exc())