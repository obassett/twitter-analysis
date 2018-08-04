import boto3
import time
import json
import base64
from botocore.exceptions import ClientError
from botocore.exceptions import ConnectionError as awsConnectionError


# -----------------------------------------------------------------------------
# function to write to dynamodb

# -----------------------------------------------------------------------------
def write_to_dynamo(tweet_id_str,tweet_sentiment,**kwargs):
    """TODO: Need to write what goes here! Description and Reference info etc.


    """
    import boto3
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('tweets')
    #--------------------------------------------------------------------------
    # DynamoRecord = {
    #             'tweet_id_str':ProcessedTweet['id_str'],
    #             'tweet_sentiment':SentimentInfo['Sentiment'],
    #             'sentiment_pos_pct':SentimentInfo['SentimentScore']['Positive'],
    #             'sentiment_neg_pct':SentimentInfo['SentimentScore']['Negative'],
    #             'sentiment_neu_pct':SentimentInfo['SentimentScore']['Neutral'],
    #             'sentiment_mix_pct':SentimentInfo['SentimentScore']['Mixed'],
    #             'tweet_user_id_str':ProcessedTweet['user_id_str'],
    #             'tweet_user_name':ProcessedTweet['user_name'],
    #             'tweet_text':ProcessedTweet['text'],
    #             'tweet_hashtags':ProcessedTweet['hashtags'],
    #             'tweet_json':ProcessedTweet['tweet_json']
    #             }
    #--------------------------------------------------------------------------

    RecordtoWrite = {'tweet_id_str':tweet_id_str,'tweet_sentiment':tweet_sentiment}
    RecordtoWrite.update(kwargs)


    table.put_item(
        Item=RecordtoWrite
    )

    
    return 
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
        # Full JSON object
    ReturnedValue = {}
    ht_list = []
    dictTweet = json.loads(tweet)
    # Ignore retweets for sentiment analyis but keep quotes and only do analysis on new text
    if 'retweeted_status' not in dictTweet:
        if dictTweet['is_quote_status']:
            #Handle Quotes
            TweetText = dictTweet['text']
        elif dictTweet['truncated']:
            # Get the text from extended tweet fields
            TweetText = dictTweet['extended_tweet']['full_text']
        else:
            # Get the normal text
            TweetText = dictTweet['text']
        for hashtag in dictTweet['entities']['hashtags']:
            ht_list.append(hashtag['text'])
        ReturnedValue = {
            'id_str':dictTweet['id_str'],
            'user_id_str':dictTweet['user']['id_str'],
            'user_name':dictTweet['user']['screen_name'],
            'text':TweetText,
            'hashtags':ht_list,
            'RT':False,
            'tweet_json':tweet
            }
    else:
        #Set RT value to true for retweets and don't return anything else except for raw json.
        ReturnedValue = {'tweet_json':tweet,'RT':True}

    return ReturnedValue

def get_sentiment(text):
    """Stuff

    """
    comprehend = boto3.client("comprehend")
    #Pass is tweet text and return below
    # Grab following:
        # Sentiment
        # Postive %
        # Negative %
        # Neutral %
        # Mixed %
        # {
        #   'Sentiment': 'POSITIVE'|'NEGATIVE'|'NEUTRAL'|'MIXED',
        #   'SentimentScore': {
        #      'Positive': ...,
        #      'Negative': ...,
        #      'Neutral': ...,
        #      'Mixed': ...
        #   }
        # }
    ReturnedSentiment = {}
    ReturnedSentiment = comprehend.detect_sentiment(Text=text,LanguageCode='en')

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
                'tweet_sentiment':SentimentInfo['Sentiment'],
                'sentiment_pos_pct':str(SentimentInfo['SentimentScore']['Positive']),
                'sentiment_neg_pct':str(SentimentInfo['SentimentScore']['Negative']),
                'sentiment_neu_pct':str(SentimentInfo['SentimentScore']['Neutral']),
                'sentiment_mix_pct':str(SentimentInfo['SentimentScore']['Mixed']),
                'tweet_user_id_str':ProcessedTweet['user_id_str'],
                'tweet_user_name':ProcessedTweet['user_name'],
                'tweet_text':ProcessedTweet['text'],
                'tweet_hashtags':ProcessedTweet['hashtags'],
                'tweet_json':ProcessedTweet['tweet_json']
                }
            write_to_dynamo(**DynamoRecord)

    return 'Successfully processed {} records.'.format(len(event['Records']))


