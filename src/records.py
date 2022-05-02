from faust import Record


class RssFeed(Record, serializer='json'):
    feed_source: str
    title: str
    link: str
    published: str = None
    author: str = None
    summary: str = None
    published_parsed: list = None
    authors: list = None
    tags: list = None
    comments: str = None
    content: list = None
    source: dict = None


class TwitterTrend(Record, serializer='json'):
    pass


class Tweet(Record, serializer="json"):
    tweet_id: str
    text: str
    created_at: str
    metrics: dict
    author: dict
    trend: str
    place: str = None
    keywords: list = None


class RedditPost(Record, serializer='json'):
    id: str
    title: str
    author: dict
    created: str
    score: int
    upvote_ratio: float
    reddit: dict
    domain: str = None
    url: str = None
    comments: list = None
    keywords: list = None
