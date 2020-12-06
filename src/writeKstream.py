#!/usr/bin/env python3
"""
Des: a quick script to start/end trip
Usage: {programname} 
"""
from __future__ import print_function
import boto3
from datetime import datetime
import calendar
import time
import json

# The kinesis stream I defined in asw console
stream_name = 'mpgm-trip-v1'

k_client = boto3.client(
    'kinesis',
    region_name='us-west-2')

def main():

    trip_timestamp = calendar.timegm(datetime.utcnow().timetuple())
    tripEvent = {
        'user': {
            'id':'user-123'
        },
        'load':{
            'label':'Up to 80,000lbs',
            'weight':{
                'uom':'lbs',
                'value':80000
            }
        },
        'trip':{
            'id':'trip-123',
            'status':'ENDING'
        },
        'truck':{
            'label':'fast-truck',
            'deviceId': '20003e000247373336373936'
        },
        'timestamp': trip_timestamp
    }

    print (tripEvent)

    put_response = k_client.put_record(
                    StreamName=stream_name,
                    Data=json.dumps(tripEvent),
                    PartitionKey='1')

    print(put_response)

if __name__ == '__main__':
    main()
