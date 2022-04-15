from faust import Record

class RssFeed(Record, serializer='json'):
    feed_source: str
    title: str
    link: str
    published:str = None
    author: str = None
    summary: str = None
    published_parsed: list = None
    authors: list = None
    tags: list = None
    comments: str = None
    content: list = None
    source: dict = None
