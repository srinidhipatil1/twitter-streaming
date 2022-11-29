import uuid

import tweepy
import json
import boto3
import time
import csv
# Credentials (INSERT YOUR KEYS AND TOKENS IN THE STRINGS BELOW)
api_key = "mjN9kQaWB65dD2jhE8T7OOEIJ"
api_secret = "9bn40wIejTtARfh5fjCrIiTaDbM1OgD4sHCHRVXgHjxSh08yEf"
bearer_token = r"AAAAAAAAAAAAAAAAAAAAALLJjgEAAAAA9ql%2BXRm3njUiT9ohmhMtFG%2BfWFM%3DoCRCvCtXq2LACCLLdWVBHurz4xhWLDYdlRPptpZlksQP5qjT3Q"
access_token = "1595861149127409668-HhKwcGGIIOMRrNcYEIDQcpGWZ3sX7b"
access_token_secret = "JUiVZO8xbATD5Xra5byinOn2oQkXtkr1DTyJj0PRfpZAW"

kinesis_stream_name = 'twitter-stream'

# Gainaing access and connecting to Twitter API using Credentials
client = tweepy.Client(bearer_token, api_key, api_secret, access_token, access_token_secret)

auth = tweepy.OAuth1UserHandler(bearer_token, api_key, api_secret, access_token, access_token_secret)
api = tweepy.API(auth)

search_terms = ["python", "programming", "coding"]


# Bot searches for tweets containing certain keywords
class MyStream(tweepy.StreamingClient):

    # This function gets called when the stream is working
    def on_connect(self):
        print("Connected")

    # This function gets called when a tweet passes the stream
    def on_tweet(self, tweet):
        # Displaying tweet in console
        print(tweet)
        print(tweet.keys())
        print(tweet.created_at)
        data = {
            'tweet_id': tweet['id'],
            'tweet_text': tweet['text']
        }
        response = kinesis_client.put_record(
            StreamName=kinesis_stream_name,
            Data=json.dumps(data),
            PartitionKey=partition_key)
        print('Status: ' +
              json.dumps(response['ResponseMetadata']['HTTPStatusCode']))


session = boto3.Session()
kinesis_client = session.client('kinesis')
partition_key = str(uuid.uuid4())

# Creating Stream object
stream = MyStream(bearer_token=bearer_token)

stream.add_rules(tweepy.StreamRule("from:mainsrin"))
# # Starting stream
stream.filter(tweet_fields=["referenced_tweets"])

