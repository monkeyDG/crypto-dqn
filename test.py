# Databricks notebook source
# MAGIC %sh
# MAGIC pip install tweepy

# COMMAND ----------

import sys
import sparknlp
import tweepy

spark = sparknlp.start()
print("Spark NLP version:", sparknlp.version())
print("Apache Spark version:", spark.version)
print("Python version:", sys.version)

# COMMAND ----------

#Replace the value below with the name of your secret scope that you created with the Databricks CLI.
SECRET_SCOPE = "thesis-scope"
 
bearer_token = dbutils.secrets.get(scope = SECRET_SCOPE, key = "tweepy-bearer-token")
 
# Instantiate the Tweepy client and run our search query. The default query is searching for New Year-related Tweets, but you can modify the keywords to search for Tweets on a different topic.
client = tweepy.Client(bearer_token=bearer_token)
query = '(#NewYearsResolution OR #NewYearsResolutions OR #NewYearResolution OR #NewYearResolutions OR #NYResolution OR #NYResolutions OR #NYResolution2022 OR #NYResolutions2022 OR #NewYearsResolution2022 OR #NewYearResolution2022 OR #NewYearsResolutions2022 OR "New Year’s Resolution" OR "New Years Resolution" OR "New Year’s Resolutions" OR "New Years Resolutions") lang:en'
 
# By default, Tweepy will pull a maximum of 10,000 Tweets per search. This can be adjusted by modifying the 'limit=10000' parameter below. Be mindful of the Twitter's API quotas!
tweets = tweepy.Paginator(client.search_recent_tweets, query, max_results=100).flatten(limit=10000)

for tweet in tweets:
    print(tweet)

# COMMAND ----------

#Replace the value below with the name of your secret scope that you created with the Databricks CLI.
SECRET_SCOPE = "thesis-scope"
 
bearer_token = dbutils.secrets.get(scope = SECRET_SCOPE, key = "tweepy-bearer-token")
 
# Instantiate the Tweepy client and run our search query. The default query is searching for New Year-related Tweets, but you can modify the keywords to search for Tweets on a different topic.
client = tweepy.Client(bearer_token=bearer_token)
query = '"#NewYearsResolution" lang:en'
 
# Replace with time period of your choice
start_time = '2020-01-01T00:00:00Z'

# Replace with time period of your choice
end_time = '2020-08-01T00:00:00Z'

# By default, Tweepy will pull a maximum of 10,000 Tweets per search. This can be adjusted by modifying the 'limit=' parameter below. Be mindful of the Twitter's API quotas!
# when I  get academic access, change the method to search_all_tweets()
tweets = tweepy.Paginator(client.search_recent_tweets,
                           query=query,
                           tweet_fields=['context_annotations', 'created_at', 'public_metrics', 'entities', 'geo', ],
                           #start_time=start_time,
                           #end_time=end_time,
                           expansions='author_id',
                           max_results=10  # max 100
                          ).flatten(limit=10)

#once we have a list of tweets, find the top authors by id and then use the users object to visualize details about them (https://developer.twitter.com/en/docs/twitter-api/data-dictionary/object-model/user)



# COMMAND ----------

import pandas as pd
 
tweetList = []

for tweet in tweets:
    print(tweet)
    tweetList.append(tweet)

df = pd.DataFrame(tweetList)
print(df.head())
print(df.info())

# COMMAND ----------

#Create dataframe schema
from pyspark.sql.types import StructType,StructField, StringType
schema = StructType([
  StructField('firstname', StringType(), True),
  StructField('middlename', StringType(), True),
  StructField('lastname', StringType(), True)
  ])

