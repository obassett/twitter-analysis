 # Pull Twitter Data.

from TwitterAPI import TwitterAPI

import json
import twitterCreds

## twitter credentials

consumer_key = twitterCreds.consumer_key
consumer_secret = twitterCreds.consumer_secret
access_token_key = twitterCreds.access_token_key
access_token_secret = twitterCreds.access_token_secret

api = TwitterAPI(consumer_key, consumer_secret, access_token_key, access_token_secret)

request = api.request('statuses/filter', {'track':'100daysofcode'})
for item in request:
    item_json = json.dumps(item)
    print("Start Tweet:")
    print(item_json)
    print("End Tweet:")

