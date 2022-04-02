import faust

from records import RssFeed


app = faust.App('topic_consumer', broker='kafka://localhost:9092')
topic = app.topic('rss', value_type=RssFeed)

@app.agent(topic)
async def process_rss_feeds(feeds): 
    async for feed in feeds:
        print(feed.asdict())

    
