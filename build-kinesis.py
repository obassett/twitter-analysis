


import boto3

kinesis = boto3.client('kinesis')

TwitterStreamName = "twitter-stream2"
ShardCount = 1
TwitterStreamExists = 0

# check to see if Stream Already Exists
GetKinesisStreams = kinesis.list_streams(Limit=2)

ExtendedStreamList = GetKinesisStreams['StreamNames']

while GetKinesisStreams['HasMoreStreams']:
    GetKinesisStreams = kinesis.list_streams(Limit=2, ExclusiveStartStreamName=GetKinesisStreams['StreamNames'][-1])
    ExtendedStreamList += GetKinesisStreams['StreamNames']

for KinesisStreamName in ExtendedStreamList:
    if KinesisStreamName == TwitterStreamName:
        TwitterStreamExists += 1
    
if TwitterStreamExists > 0:
    print('A stream with that name already exists')
else:
    kinesis.create_stream(StreamName=TwitterStreamName,ShardCount=ShardCount)

StreamDescription = kinesis.describe_stream_summary(StreamName=TwitterStreamName)
print(StreamDescription['StreamDescriptionSummary']['StreamName'], " - ", StreamDescription['StreamDescriptionSummary']['StreamStatus'])