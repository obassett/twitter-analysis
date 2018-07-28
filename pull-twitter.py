 # Pull Twitter Data and push into kinesis.

from TwitterAPI import TwitterAPI
from http.client import IncompleteRead
from TwitterAPI import TwitterRequestError
from TwitterAPI import TwitterConnectionError
from botocore.exceptions import ClientError
from botocore.exceptions import ConnectionError as awsConnectionError


import json
import boto3
import twitterCreds

## twitter credentials

consumer_key = twitterCreds.consumer_key
consumer_secret = twitterCreds.consumer_secret
access_token_key = twitterCreds.access_token_key
access_token_secret = twitterCreds.access_token_secret

api = TwitterAPI(consumer_key, consumer_secret, access_token_key, access_token_secret)

# define comma seperated list of keywords to track
tweetsTotrack = "trump"

# Define maximum number of shards for stream (cost control)
maxStreamShards = 2
maxItemsPerPut = 150

# define name of Kinesis Stream to use
# TODO: This will eventually be passed in from somewhere - maybe config rile.
TwitterKinesisStreamName = "twitter-stream"

kinesis = boto3.client('kinesis')

while True:
    tweetRecords = []
    itemcount = 0
    try: 
        request = api.request('statuses/filter', {'track':tweetsTotrack}).get_iterator()    
        for item in request:
            if itemcount >= maxItemsPerPut:
                try:
                    #put record into Kinesis stream. Use random partitionkey for even distribution (the tweet id should do )
                    #PutResponse = kinesis.put_record(StreamName=TwitterKinesisStreamName, Data=json.dumps(item), PartitionKey=item['id_str'])
                    PutResponse = kinesis.put_records(Records=tweetRecords,StreamName=TwitterKinesisStreamName)
                    print("------------------")
                    print(PutResponse['FailedRecordCount'], " - Records Failed to Write")
                    print(len(PutResponse['Records']), " - Records Written")
                    print("------------------")
                    itemcount = 0
                    tweetRecords = []
                except ClientError as PutError:
                    # TODO: Need to ShardSplit and and then try again
                    # 
                    print("exception in put request")
                    if PutError.respone['Error']['Code'] == 'ProvisionedThroughputExceededException':
                        StrShardToSplit = PutResponse['ShardId']
                        if len(kinesis.list_shards(StreamName = TwitterKinesisStreamName)) >= maxStreamShards:
                            raise('UserStreamLimitReached')
                        else:
                            for shard in kinesis.list_shards(
                                StreamName = TwitterKinesisStreamName,
                                ExclusiveStartShardId=StrShardToSplit
                            ):
                                if shard['ShardId'] == StrShardToSplit:
                                    #Figure out point to split shard.
                                    StartingHashKey = (int(shard['HashkeyRange']['StartingHashKey']) + int(shard['HashkeyRange']['EndingHashKey'])) / 2
                                    SplitResponse = kinesis.split_shard(
                                            StreamName = TwitterKinesisStreamName,
                                            ShardToSplit = StrShardToSplit,
                                            NewStartingHashKey = StartingHashKey
                                    )
                            raise
                except awsConnectionError as awsConErr:
                    print("AWS Connection Error", awsConnErr)    
                    raise
            else:
                if 'text' in item:
                    tweetRecords.append({'Data':json.dumps(item),'PartitionKey':item['id_str']})
                    itemcount += 1
                elif 'disconnect' in item:
                    event = item['disconnect']
                    if event['code'] in [2,5,6,7]:
                        raise Exception(event['reason'])
                    else:
                        # temporary failure, so re-try
                        break
    except IncompleteRead:
        #Ignore Incomplete Reads
        print("IncompleteRead Handling")
        pass
    except TwitterRequestError as e:
        if e.status_code < 500:
            Print("TwitterRequestError ", e.status_code)
            #Something bad has happened. Break out of the loop
            raise
    except TwitterConnectionError:
        # temporary error retry
        print("TwitterConnectionError - ")
        pass    