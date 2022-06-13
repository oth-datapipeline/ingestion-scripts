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

from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer


# define faust app
broker_host = os.environ["BROKER_HOST"] if "BROKER_HOST" in os.environ.keys() else "localhost:9092"
app = faust.App('twitter_consumer', broker=f'kafka://{broker_host}')

logger, log_handler = get_ingestion_logger("twitter")
app.logger.addHandler(log_handler)

# define topics
twitter = app.topic('twitter', value_type=Tweet)
tweets_without_emojis = app.topic('tweet_no_emoji', value_type=Tweet)
tweets_with_sentiment = app.topic('tweet_sentiment_analysis', value_type=Tweet)
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
async def analyze_tweet_sentiment(tweets, concurrency=4):
    # Sentiment analyzer taken from:
    # Hutto, C.J. & Gilbert, E.E. (2014). VADER: A Parsimonious Rule-based Model for Sentiment Analysis of Social Media Text.
    # Eighth International Conference on Weblogs and Social Media (ICWSM-14). Ann Arbor, MI, June 2014.
    #
    # FROM THE DOCS:
    # The compound score is computed by summing the valence scores of each word in the lexicon,
    # adjusted according to the rules, and then normalized to be between -1 (most extreme negative)
    # and +1 (most extreme positive). This is the most useful metric if you want a single unidimensional measure
    # of sentiment for a given sentence. Calling it a 'normalized, weighted composite score' is accurate.
    #
    # It is also useful for researchers who would like to set standardized thresholds for classifying sentences
    # as either positive, neutral, or negative. Typical threshold values (used in the literature cited on this page) are:
    # - positive sentiment: compound score >= 0.05
    # - neutral sentiment: (compound score > -0.05) and (compound score < 0.05)
    # - negative sentiment: compound score <= -0.05
    #
    # NOTE: The compound score is the one most commonly used for sentiment analysis by most researchers, including the authors.

    async for tweet in tweets:
        try:
            analyzer = SentimentIntensityAnalyzer()
            scores = analyzer.polarity_scores(tweet.text)

            tweet.sentiment = {
                'negative': scores['neg'],
                'neutral': scores['neu'],
                'positive': scores['pos'],
                'compound': scores['compound']
            }

            logger.info(f"Ran sentiment analysis for Tweet {tweet.tweet_id}")
            await tweets_with_sentiment.send(value=tweet)
        except Exception:
            logger.warn(traceback.format_exc())


@app.agent(tweets_with_sentiment)
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
            # add hashtags
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
