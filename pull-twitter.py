 # Pull Twitter Data and push into kinesis.

from TwitterAPI import TwitterAPI

import json
import boto3
import twitterCreds

## twitter credentials

consumer_key = twitterCreds.consumer_key
consumer_secret = twitterCreds.consumer_secret
access_token_key = twitterCreds.access_token_key
access_token_secret = twitterCreds.access_token_secret

api = TwitterAPI(consumer_key, consumer_secret, access_token_key, access_token_secret)

kinesis = boto3.client('kinesis')

request = api.request('statuses/filter', {'track':'100daysofcode'})
for item in request:
    kinesis.put_record(StreamName="twitter-stream", Data=json.dumps(item), PartitionKey="filler")
    