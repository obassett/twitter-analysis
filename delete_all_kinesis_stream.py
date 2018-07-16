#
# Quick Script to remove all my Kinesis Streams
#

import boto3


kinesis = boto3.client('kinesis')

# Get all Kinesis Streams
GetKinesisStreams = kinesis.list_streams(Limit=10)
ExtendedStreamList = GetKinesisStreams['StreamNames']

while GetKinesisStreams['HasMoreStreams']:
    GetKinesisStreams = kinesis.list_streams(Limit=2, ExclusiveStartStreamName=GetKinesisStreams['StreamNames'][-1])
    ExtendedStreamList += GetKinesisStreams['StreamNames']

for KinesisStreamName in ExtendedStreamList:
    kinesis.delete_stream(StreamName=KinesisStreamName)