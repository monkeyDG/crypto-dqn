import pandas as pd
import tweepy
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType, ArrayType, LongType
from functools import wraps
from time import time

def timing(f):
    @wraps(f)
    def wrap(*args, **kw):
        ts = time()
        result = f(*args, **kw)
        te = time()
        print('func:%r args:[%r, %r] took: %2.4f sec') % (f.__name__, args, kw, te-ts)
        return result
    return wrap

def get_tweets():
    bearer_token = "AAAAAAAAAAAAAAAAAAAAAM5ncgEAAAAAs5SSqiJhDYzbGrMZg8K1gop458g%3Dj0LrQShHBOGwJWvNYsGPw4NjTSxuCPg7pDu7NjC0nFqxc5J9K7"
    
    # Instantiate the Tweepy client and run our search query. The default query is searching for New Year-related Tweets, but you can modify the keywords to search for Tweets on a different topic.
    client = tweepy.Client(bearer_token=bearer_token)
    
    # Instantiate the Tweepy client and run our search query. The default query is searching for New Year-related Tweets, but you can modify the keywords to search for Tweets on a different topic.
    client = tweepy.Client(bearer_token=bearer_token)
    query = 'bitcoin'
    #query = '"#NewYearsResolution" lang:en'

    # Replace with time period of your choice
    start_time = '2020-01-01T00:00:00Z'

    # Replace with time period of your choice
    end_time = '2020-08-01T00:00:00Z'

    # By default, Tweepy will pull a maximum of 10,000 Tweets per search. This can be adjusted by modifying the 'limit=' parameter below. Be mindful of the Twitter's API quotas!
    # when I  get academic access, change the method to search_all_tweets()
    tweets = tweepy.Paginator(client.search_recent_tweets,
                            query=query,
                            tweet_fields=['context_annotations', 'created_at', 'public_metrics', 'entities', 'geo', 'source', 'referenced_tweets', 'conversation_id'],
                            #start_time=start_time,
                            #end_time=end_time,
                            expansions='author_id',
                            max_results=100  # max 100
                            ).flatten(limit=1000)
    
    return tweets

@timing
def process_tweets(tweets):
    tweetlist = []

    for tweet in tweets:
        tweetlist.append((
            int(tweet.id),
            tweet.created_at,
            int(tweet.author_id),
            int(tweet.conversation_id),
            tweet.source,
            tweet.geo["coordinates"]["coordinates"] if tweet.geo and "coordinates" in tweet.geo else None,
            tweet.geo["place_id"] if tweet.geo and "place_id" in tweet.geo else None,
            [int(x["id"]) for x in tweet.entities["mentions"]] if tweet.entities and "mentions" in tweet.entities else None,
            [x["tag"] for x in tweet.entities["hashtags"]] if tweet.entities and "hashtags" in tweet.entities else None,
            [x["unwound"]["url"] if "unwound" in x else x["expanded_url"] for x in tweet.entities["urls"]] if tweet.entities and "urls" in tweet.entities else None,
            tweet.text
            ))

    schema = StructType([
        StructField("id", LongType(), False),
        StructField("created_at", TimestampType(),True),
        StructField("author_id", LongType(),True),
        StructField("conversation_id", LongType(), True),
        StructField("source", StringType(), True),
        StructField("geo_coordinates", ArrayType(DoubleType(), False), True),
        StructField("geo_place_id", StringType(), True),
        StructField("mentions", ArrayType(LongType(), False), True),
        StructField("hashtags", ArrayType(StringType(), False), True),
        StructField("urls", ArrayType(StringType(), False), True),
        StructField("text", StringType(), True)
    ])
    
    spark_df = spark.createDataFrame(data=tweetlist, schema=schema)
    spark_df.printSchema()
    spark_df.show()

def main():
    tweets = get_tweets()
    process_tweets(tweets)

if __name__ == "__main__":
    spark = SparkSession.builder \
                    .appName('testing') \
                    .getOrCreate()
    main()
