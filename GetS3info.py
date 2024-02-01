#-------------------------------------------------------------------------------
# Name:        module1
# Purpose:     create csv file with s3 bucket size and number of files
#
# Author:      Dmitriy.Burtsev
#
# Created:     30/01/2024
# Copyright:   (c) Dmitriy.Burtsev 2024
# Licence:     <your licence>
#-------------------------------------------------------------------------------
import boto3, botocore
import json, csv

AWS_ACCESS_KEY_ID=""
AWS_SECRET_ACCESS_KEY=""
AWS_SESSION_TOKEN=""
AWS_DEFAULT_REGION="us-east-1"

client = boto3.client('s3',region_name=AWS_DEFAULT_REGION,aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY,aws_session_token=AWS_SESSION_TOKEN)

response = client.list_buckets()

buckets = [] # empty list
buckets_dict = {} # empty dictionary
for bucket in response['Buckets']:
    buckets.append(bucket['Name'])

for bucket in buckets:
    prefix = ''
    print(bucket)
    try:
        response = client.head_bucket(Bucket=bucket)
    except botocore.exceptions.ClientError as e:
        error_code = e.response['Error']['Code']
        print(bucket + ' Error code ' + error_code)
        continue
    #is_empty = [] == [i for i in bucket.objects.limit(1).all()]
    resp = client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    if int(resp.get('KeyCount')) == 0:
        print(bucket + ' is empty')
        continue
    #print(resp.get('KeyCount'))
    bucket_size_gb = 0
    KeyCount = int(resp.get('KeyCount'))
    bucket_size = float(sum([obj.get('Size') for obj in resp.get('Contents')]))
    while resp.get('NextContinuationToken'):
        resp = client.list_objects_v2(Bucket=bucket, Prefix=prefix, ContinuationToken=resp.get('NextContinuationToken'))
        KeyCount += int(resp.get('KeyCount'))
        bucket_size += float(sum([obj.get('Size') for obj in resp.get('Contents')]))
        # convert bytes to GB
        bucket_size_gb = bucket_size // 1024 // 1024

    buckets_dict[bucket] = F"{KeyCount} objects, {bucket_size_gb} MB"

json.dump( buckets_dict, open( "buckets.json", 'w' ) )

with open('buckets.csv', 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerows(buckets_dict.items())

