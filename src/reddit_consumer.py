import datetime
import emoji
import faust
import os
import pymongo
import traceback

from pymongo import MongoClient
from nltk.corpus import stopwords
from records import RedditPost

from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

# imports for tagging reddit comments
import nltk
from ingestion_logger import get_ingestion_logger
import collections
import itertools
import re
import string
nltk.download('stopwords')
stop_words = set(stopwords.words('english'))
more_stopwords = ["it's", 'im', 'lol', "i'm", 'got', 'yeah', "it’s", "i’m", "its", "i", "me"]
stop_words.update(more_stopwords)

# define faust app
broker_host = os.environ["BROKER_HOST"] if "BROKER_HOST" in os.environ.keys() else "localhost:9092"
app = faust.App('reddit_consumer', broker=f'kafka://{broker_host}')

logger, log_handler = get_ingestion_logger("reddit")
app.logger.addHandler(log_handler)

# define topics
reddit = app.topic('reddit', value_type=RedditPost)
posts_without_emojis = app.topic('post_no_emoji', value_type=RedditPost)
posts_with_sentiment = app.topic('post_sentiment_analysis', value_type=RedditPost)
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
collection.create_index([('id', pymongo.ASCENDING), ('insert_date', pymongo.ASCENDING)],
                        name='tweet_index', unique=True)


def clean_text(text):
    '''Make text lowercase, remove text in square brackets, remove punctuation and remove words containing numbers.'''
    text = text.lower()
    text = re.sub('\\[.*?\\]', '', text)
    text = re.sub('[%s]' % re.escape(string.punctuation), '', text)
    text = re.sub('\\w*\\d\\w*', '', text)
    return text


def pairwise(iterable):
    """s -> (s0,s1), (s1,s2), (s2, s3), ..."""
    a, b = itertools.tee(iterable)
    next(b, None)
    return ((a, b) if a < b else (b, a) for a, b in zip(a, b))


def structure_keywords(keywords: "list[tuple[tuple, int]]"):
    keyword_tuples_list = [kw[0] for kw in keywords]
    keywords_as_list = sum(keyword_tuples_list, ())
    return list(dict.fromkeys(keywords_as_list))


def analyze_sentiment(text: string):
    analyzer = SentimentIntensityAnalyzer()
    scores = analyzer.polarity_scores(text)
    return {
        'negative': scores['neg'],
        'neutral': scores['neu'],
        'positive': scores['pos'],
        'compound': scores['compound']
    }


@app.agent(reddit)
async def analyze_post_and_comment_sentiment(posts, concurrency=4):
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
    async for post in posts:
        try:
            post.sentiment = analyze_sentiment(post.title)

            comment_list = post.comments
            for comment in comment_list:
                comment["sentiment"] = analyze_sentiment(comment["text"])

            logger.info(f"Ran sentiment analysis for Post {post.id}")
            await posts_with_sentiment.send(value=post)
        except Exception:
            logger.warn(traceback.format_exc())


@app.agent(posts_with_sentiment)
async def demojify_post(posts, concurrency=4):
    async for post in posts:
        try:
            comment_list = post.comments
            for comment in comment_list:
                demojified_comment = emoji.demojize(comment["text"], language="en")
                comment["text"] = clean_text(demojified_comment)
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
            comments_without_stopwords = []
            for comment in comments:
                cleaned_comment = [word for word in comment.split(" ") if word not in stop_words]
                comments_without_stopwords.append(cleaned_comment)

            flattened_comments_without_stopwords = itertools.chain(*comments_without_stopwords)
            keyword_collection = collections.Counter(pairwise(flattened_comments_without_stopwords)).most_common(5)
            post.keywords = structure_keywords(keyword_collection)
            logger.info(f"Extracted Keywords for Post {post.id}")
            await final_post.send(value=post)
        except Exception:
            logger.warn(traceback.format_exc())


@app.agent(final_post)
async def insert_post(posts, concurrency=1):
    async for post in posts:
        try:
            post.created = datetime.datetime.strptime(post.created, "%Y-%m-%d %H:%M:%S")
            insert_date = datetime.datetime.now()
            collection.insert_one({**post.asdict(), "insert_date": insert_date})
            logger.info(f"Inserted Post {post.id} into MongoDB")
        except Exception:
            logger.warn(traceback.format_exc())
