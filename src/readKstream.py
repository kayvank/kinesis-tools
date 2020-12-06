#!/usr/bin/env python3
"""
Des: Reads from a kinesis stream
Usage: {programname} -s stream-name 
Assumption: kinsis stream contins JSON data

"""

import boto3
import argparse
import time
import base64
import json

kinesis_client = boto3.client(
    'kinesis',
    region_name='us-west-2')

def compose2(f, g):
    return lambda x: f(g(x))

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-s',
        help='kinesis stream name to read from')
    cArgs = parser.parse_args()

    processStream(cArgs.s)

def processStream(streamName):
    streamDescription = kinesis_client.describe_stream(
        StreamName=streamName)
    # print('Reading from stream {} with Description{}'.format(streamName, streamDescription))

    sid = streamDescription['StreamDescription']['Shards'][0]['ShardId']
    shard_iterator = kinesis_client.get_shard_iterator(
        StreamName=streamName,
        ShardId=sid,
        ShardIteratorType='LATEST')
    my_shard_iterator = shard_iterator['ShardIterator']
    record_response = kinesis_client.get_records(
        ShardIterator=my_shard_iterator,
        Limit=100)
    while 'NextShardIterator' in record_response:
        record_response = kinesis_client.get_records(
            ShardIterator=record_response['NextShardIterator'],
            Limit=100)
        processRecord(record_response)

def asString(kclRec):
        return (kclRec['Records'][0]['Data']).decode("utf-8")

def asJson(str):
    return json.loads(str)

def eventToJson(j):
    return compose2(asJson, asString)(j)

def prettyPrint(jsonObj):
    return json.dumps(jsonObj, indent=2)

def process(data):
    return compose2(prettyPrint, eventToJson)(data)
def processRecord(kclRec):
    if(kclRec['Records']):
        # j = eventToJson(kclRec)
        # jj = json.dumps(j, indent=2 )
        j = process(kclRec)
        print (j)
        time.sleep(1)

if __name__ == '__main__':
    main()
