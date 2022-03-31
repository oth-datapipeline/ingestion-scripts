from faust import Record

class RssFeed(Record, serializer='json'):
    feed_source: str
    title: str
    link: str
    comments: str
    authors: list
    published_parsed: list
    tags: list
    summary: str
    content: list
    source: dict
