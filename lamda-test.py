import boto3
import time
import json
import base64
from botocore.exceptions import ClientError
from botocore.exceptions import ConnectionError as awsConnectionError


# -----------------------------------------------------------------------------
# function to write to dynamodb

# -----------------------------------------------------------------------------
def write_to_dynamo(DynamoRecord):
    """TODO: Need to write what goes here! Description and Reference info etc.


    """
    # Pass in collection with records(dict)
    # Check we have everything
    # Write records.

    # tweet records we want to write:
    # tweet_id_str
    # user id_str
    # user screen_name
    # text
    # hashtags
    # Some kinda geo data
    # Full JSON of tweet

    # Comprehend details we want to write
    # Sentiment
    # Postive %
    # Negative %
    # Neutral %
    # Unsure %

    Results =""
    return Results
# -----------------------------------------------------------------------------


def get_tweet_details(tweet):
    """Stuff

    """
    # Take in tweet josn and process tweet and return records
    # Grab following:
        # id_str
        # text / or entended_tweet.full_text 
        # user id_str
        # user screen_name
        # hashtags
        # tweet location (geo co-ords)
        # Full JSON object
    dictTweet = json.loads(tweet)

    # Ignore retweets for sentiment analyis but keep quotes and only do analysis on new text
    if 'retweeted_status' not in dictTweet:
        # pull all the values except text
        if dictTweet['is_quote_status']:
            #Handle Quotes
            TweetText = dictTweet['text']
        elif dictTweet['truncated']:
            # Get the text from extended tweet fields
            TweetText = dictTweet['extended_tweet']['full_text']
        else:
            # Get the normal text
            TweetText = dictTweet['text']
        ReturnedValue = {'text':TweetText,'tweet_json':tweet}
    else:
        #Set RT value to true for retweets and don't return anything else except for raw json.
        ReturnedValue = {'tweet_json':tweet,'RT':True}

    return ReturnedValue

def get_sentiment(text):
    """Stuff

    """
    #Pass is tweet text and return below
    # Grab following:
        # Sentiment
        # Postive %
        # Negative %
        # Neutral %
        # Unsure %
    
    ReturnedSentiment = {}
    
    return ReturnedSentiment

def lambda_handler(event, context):
    
    for record in event['Records']:
        # Decode the kinesis data record into tweet. This should be a json string.
        tweet = base64.b64decode(record['kinesis']['data'])
        ProcessedTweet = get_tweet_details(tweet)
        if not ProcessedTweet['RT']:
            SentimentInfo = get_sentiment(ProcessedTweet['text'])
            DynamoRecord = {
                'tweet_id_str':ProcessedTweet['id_str'],
                'tweet_sentiment':SentimentInfo['Sentiment']
                }
            write_to_dynamo(DynamoRecord)

    return 'Successfully processed {} records.'.format(len(event['Records']))


