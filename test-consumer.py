import boto3
import time
import json

## aws creds are stored in ~/.boto

#Kinesis object. 
kinesis = boto3.client("kinesis")
shard_id = "shardId-000000000000" 
pre_shard_it = kinesis.get_shard_iterator(StreamName="twitter-stream", ShardId=shard_id, ShardIteratorType="LATEST")
shard_it = pre_shard_it["ShardIterator"]

comprehend = boto3.client("comprehend")

while 1==1:
     out = kinesis.get_records(ShardIterator=shard_it, Limit=1)
     shard_it = out["NextShardIterator"]
     if out['Records']: 
        for record in out['Records']:
            print("Start Record:")
            tweet_text = json.loads(record['Data'])["text"]
            tweet_sentiment = comprehend.detect_sentiment(Text=tweet_text,LanguageCode='en')
            print(tweet_sentiment["Sentiment"], " - ", tweet_text)
            print("End Record:")
     time.sleep(1.0)