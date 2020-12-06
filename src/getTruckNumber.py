#!/usr/bin/env python3

"""
Desc: get truck number from carbonConnect
"""
import json
import boto3
from boto3.dynamodb.conditions import Key
from boto3.dynamodb.conditions import Attr

def main():
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('core')
    response = table.scan(
        FilterExpression=
        Attr("type").eq("TRK") and
        Attr("carbonConnectUnit").eq("test0000000000000000000000")
        # Attr("carbonConnectUnit").eq("3e0031001847373432363933")
    )
    results = response['Items']
    print(results)

if __name__== '__main__':
    main()

