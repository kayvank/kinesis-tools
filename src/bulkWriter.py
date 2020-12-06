#!/usr/bin/env python3
#
"""
Des: Bulk publish to kinesis stream
Usage: {program-name} -s stream-name -f file name
"""

import boto3
import argparse
import json
import sys

k_client = boto3.client(
    'kinesis',
    region_name='us-west-2')

def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '-s',
        help='kinesis stream name to publish to')

    parser.add_argument(
        '-f',
        metavar='in-file',
        type=argparse.FileType('rt'),
        help='file name containing Json data for publicationg to kinesis')

    parser.add_argument('--version', action='version', version='%(prog)s 1.0')

    cArgs = parser.parse_args()
    print ('streamName     =', cArgs.s)
    print ('fileName   =', cArgs.f)

    try:
        for cnt, line in enumerate(cArgs.f):
            j = json.loads(line.rstrip())
            publish(cArgs.s, json.dumps(j) + '\n')
    finally:
        cArgs.f.close()

def publish(stream_name, json_string):
    print("publish got stream_name={}".format(stream_name))
    put_response = k_client.put_record(
        StreamName=stream_name,
        Data=json_string,
        PartitionKey='1')
    print(put_response)
    

if __name__ == '__main__':
    main()
