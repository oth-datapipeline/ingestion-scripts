#!/bin/bash

cd src/
python3 -m faust -A rss_consumer worker -l info
python3 -m faust -A reddit_consumer worker -l info
python3 -m faust -A twitter_consumer worker -l info