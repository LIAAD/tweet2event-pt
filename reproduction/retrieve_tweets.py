import tweepy
import pandas as pd
import time
from datetime import datetime as dt
from datetime import date, timedelta
import os
from dotenv import load_dotenv

load_dotenv('.env') 

CONSUMER_KEY = os.environ.get("TWITTER_CONSUMER_KEY")
CONSUMER_SECRET = os.environ.get("TWITTER_CONSUMER_SECRET")
ACCESS_TOKEN = os.environ.get("TWITTER_ACCESS_TOKEN")
ACCESS_TOKEN_SECRET = os.environ.get("TWITTER_ACCESS_TOKEN_SECRET")
BEARER_TOKEN = os.environ.get("TWITTER_BEARER_TOKEN")

def wait_rate_limit(start_time):

    # Wait for 15-min window to finish
    time_window = 15
    time_elapsed = time.time() - start_time
    print("Reached max requests. Sleeping for " + str((time_window*60) - time_elapsed) + " seconds...")
    time.sleep((time_window*60) - time_elapsed) 


# Create tweet dataset by searching with keywords obtained
def get_tweets(events):

    global tweets_df
    max_requests = 300
    requests = 0

    for index, event in events.iterrows():
        topic = event["topic"]
        print(topic)

        # API allows only 1 search request per second
        time.sleep(1)

        # Filter by Portuguese tweets and exclude retweets
        # Remove some keywords related to Brazil to avoid some tweets about Brazilian events
        query = "(" + topic + ") -Brasil -brasileiro -brasileira -br -is:retweet lang:pt -place_country:br"

        # Time window for tweets: -1 week until day of event
        start_date = date.fromisoformat(str(event["date"])) - timedelta(days=7)
        start_time = dt.combine(start_date, dt.min.time())
        end_date = date.fromisoformat(str(event["date"])) + timedelta(days=1)
        end_time = dt.combine(end_date, dt.min.time())

        first_request_time = time.time()

        if requests == max_requests:
            wait_rate_limit(first_request_time)
            requests = 0
            first_request_time = time.time()

        tweets = client.search_all_tweets(query, max_results=100, start_time=start_time, end_time=end_time, tweet_fields="created_at")
        requests += 1

        if tweets.data == None: continue
        for tweet in tweets.data:
            tweets_df = tweets_df.append({
                "topic_id": index,
                "text": tweet.text,
                "topic": topic,
                "tweet_id": tweet.id,
                "date": str(tweet.created_at),
                "relevance": 0
            }, ignore_index=True)

        while "next_token" in tweets.meta:
            time.sleep(1)
            next_token = tweets.meta["next_token"]
            
            if requests == max_requests:
                wait_rate_limit(first_request_time)
                requests = 0
                first_request_time = time.time()

            tweets = client.search_all_tweets(query, max_results=100, start_time=start_time, end_time=end_time, next_token=next_token, tweet_fields="created_at")
            requests += 1

            if tweets.data == None: continue
            for tweet in tweets.data:
                tweets_df = tweets_df.append({
                    "topic_id": event["id"],
                    "text": tweet.text,
                    "topic": topic,
                    "tweet_id": str(tweet.id),
                    "date": str(tweet.created_at),
                    "relevance": 0
                }, ignore_index=True)
        
        # Write tweets as array of objects
        with open('tweets.json', 'w', encoding='utf-8') as file:
            tweets_df.to_json(file, force_ascii=False, orient="records")

        # Write with line separators for better readability
        with open('tweets.jsonl', 'w', encoding='utf-8') as file:
            tweets_df.to_json(file, force_ascii=False, orient="records", lines=True)


# Twitter API v2 authentication
client = tweepy.Client(BEARER_TOKEN, CONSUMER_KEY, CONSUMER_SECRET, ACCESS_TOKEN, ACCESS_TOKEN_SECRET)

# Retrieve tweets given list of topics
events_df = pd.read_csv("events.csv", sep="|")
tweets_df = pd.DataFrame(columns=["topic_id", "topic","tweet_id","relevance","date", "text"])
get_tweets(events_df)

