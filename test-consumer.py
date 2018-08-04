import boto3
import time
import json

## aws creds are stored in ~/.boto

#Kinesis object. 
kinesis = boto3.client("kinesis")
shard_id = "shardId-000000000000" 
pre_shard_it = kinesis.get_shard_iterator(StreamName="twitter-stream", ShardId=shard_id, ShardIteratorType="TRIM_HORIZON")
shard_it = pre_shard_it["ShardIterator"]

comprehend = boto3.client("comprehend")

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


while 1==1:
     out = kinesis.get_records(ShardIterator=shard_it, Limit=1)
     shard_it = out["NextShardIterator"]
     if out['Records']: 
        for record in out['Records']:
            ProcessedTweet = get_tweet_details(record['Data'])
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
                'tweet_hashtags':ProcessedTweet['hashtags']
                }
                write_to_dynamo(**DynamoRecord)

            else:
                print("This was a ReTweet")
     time.sleep(1.0)