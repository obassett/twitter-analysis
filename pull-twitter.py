 # Pull Twitter Data and push into kinesis.

from TwitterAPI import TwitterAPI
from TwitterAPI import TwitterRequestError
from TwitterAPI import TwitterConnectionError
from botocore.exceptions import ClientError
from botocore.exceptions import ConnectionError as awsConnectionError

import re
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
maxStreamShards = 1
maxItemsPerPut = 200

# define name of Kinesis Stream to use
# TODO: This will eventually be passed in from somewhere - maybe config rile.
TwitterKinesisStreamName = "twitter-stream"

kinesis = boto3.client('kinesis')
waiter = kinesis.get_waiter('stream_exists')

# -----------------------------------------------------------------------------
# function to handle kinesis put-records reponse
# Takes in the result of boto3.client('kinesis').put_records + StreamName + source data for request
# If there is errors it creates a dictionary object with the failed records and return them
# {ThroughputErrors:int, RecordsToRetry: [ { TweetData: 'json_str', PartitionKey: 'string'}}
# -----------------------------------------------------------------------------
def check_put_response(PutRecordsResult={},PutRecordsSource={}):
    """TODO: Need to write what goes here! Description and Reference info etc.


    """
    ThroughputErrors = 0
    RecordsToRetry = []
    if PutRecordsResult['FailedRecordCount'] >= 1:
        # Make sure Records I tried to write, and the response are the same length
        if len(PutRecordsResult['Records']) == len(PutRecordsSource):
            resultcounter = 0
            # Loop through intial data records and add them into a retry list.
            while resultcounter != len(PutRecordsResult['Records']):
                if 'ErrorCode' in PutRecordsResult['Records'][resultcounter]:
                    RecordsToRetry.append(PutRecordsSource[resultcounter])
                    if PutRecordsResult['Records'][resultcounter]['ErrorCode'] == 'ProvisionedThroughputExceededException':
                        ThroughputErrors += 1
                resultcounter += 1
            print("Failed Records=",PutRecordsResult['FailedRecordCount'], " - ThroughputErrors=", ThroughputErrors)
        else:
            print("ERROR: Length on PutRecordsReponse and PutRecordsSource don't match.")
        return {'ThroughputErrors':ThroughputErrors, 'RecordsToRetry':RecordsToRetry}
    else:
        #No failed Records! Win!
        return {'ThroughputErrors':ThroughputErrors, 'RecordsToRetry':RecordsToRetry}
# -----------------------------------------------------------------------------




while True:
    tweetRecords = []
    itemcount = 0
    try: 
        request = api.request('statuses/filter', {'track':tweetsTotrack}).get_iterator()    
        for item in request:
            if itemcount >= maxItemsPerPut:
                try:
                    #put record into Kinesis stream. Use random partitionkey for even distribution (the tweet id should do )
                    PutResponse = kinesis.put_records(Records=tweetRecords,StreamName=TwitterKinesisStreamName)
                    if PutResponse['FailedRecordCount'] >= 1:
                        #Call my check_result function to get the failed records, and see if there are throughput errors in the failures
                        check_result = check_put_response(PutRecordsResult=PutResponse,PutRecordsSource=tweetRecords)
                        # TODO: Really need to start breaking this code up into modules
                        while check_result['ThroughputErrors'] >=1:
                            # Since there are throughput errors I need to increase shards
                            # since we can only do minimal resharding, we should double shards
                            # and then retry the tweets we just did
                            #Need to get existing shard count
                            currentShardCount = kinesis.describe_stream_summary(StreamName=TwitterKinesisStreamName)['StreamDescriptionSummary']['OpenShardCount']
                            #Then double it and resize
                            newShardCount = currentShardCount * 2
                            print('Trying to set new Shard Count to: ',newShardCount)
                            if newShardCount <= maxStreamShards:
                                shardupdate = kinesis.update_shard_count(StreamName=TwitterKinesisStreamName,TargetShardCount=newShardCount,ScalingType='UNIFORM_SCALING')
                                print('Trying to update Stream ',shardupdate['StreamName'])
                                print('Current Shard Count is: ',shardupdate['CurrentShardCount'])
                                print('Target Shard Count is: ',shardupdate['TargetShardCount'])
                                #Then wait till active again
                                waiter.wait(StreamName=TwitterKinesisStreamName)
                            #Then retry back into PutResponse to that we can reset it's value and stop looping if we need to.
                            RetryResponse = kinesis.put_records(Records=check_result['RecordsToRetry'],StreamName=TwitterKinesisStreamName)
                            check_result = check_put_response(PutRecordsResult=RetryResponse,PutRecordsSource=check_result['RecordsToRetry'])
                    itemcount = 0
                    tweetRecords = []
                except ClientError as PutError:
                    # TODO: Need to ShardSplit and and then try again
                    # 
                    print("exception in put request")
                    if PutError.response['Error']['Code'] == 'ProvisionedThroughputExceededException':
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
                except awsConnectionError as awsConnErr:
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
    except TwitterRequestError as e:
        if e.status_code < 500:
            print("TwitterRequestError ", e.status_code)
            #Something bad has happened. Break out of the loop
            raise
    except TwitterConnectionError:
        # temporary error retry
        print("TwitterConnectionError - ")
        pass    