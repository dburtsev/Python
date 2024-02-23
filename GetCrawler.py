#-------------------------------------------------------------------------------
# Name:        GetCrawler
# Purpose:  Get Crawler defenition from AWS
#
# Author:      Dmitriy.Burtsev
#
# Created:     11/01/2024
#-------------------------------------------------------------------------------
import yaml
import boto3
import traceback
import shutil
import os
from botocore.exceptions import ClientError
import awskeys

def main():
    pass

if __name__ == '__main__':
    main()

client = boto3.client('glue', region_name=awskeys.AWS_DEFAULT_REGION,aws_access_key_id=awskeys.AWS_ACCESS_KEY_ID, aws_secret_access_key=awskeys.AWS_SECRET_ACCESS_KEY,aws_session_token=awskeys.AWS_SESSION_TOKEN)

def GetCrawlerInfo(crawlername):
    try:
        response = client.get_crawler(Name=crawlername)
    except ClientError as e:
        raise e
    #print(response)
    # delete extra info
    if 'CrawlElapsedTime' in response['Crawler']:
        del response["Crawler"]["CrawlElapsedTime"]
    del response["Crawler"]["CreationTime"]
    del response["Crawler"]["LastCrawl"]
    del response["Crawler"]["LastUpdated"]
    del response["Crawler"]["State"]
    dict0 = {'Type': 'AWS::Glue::Crawler'}
    dict0.update({'Properties' : response['Crawler']})
    output = yaml.dump(dict0, default_flow_style=False)
    cur_dir = os.path.realpath(os.path.dirname(__file__))
    fixed_fname = crawlername
    pdir = os.path.join(cur_dir, fixed_fname)
    isExist = os.path.exists(pdir)
    if isExist:
        shutil.rmtree(pdir)
    os.mkdir(pdir)
    yaml_file = os.path.join(pdir, (fixed_fname +".yaml"))
    text_file = open(yaml_file, "w")
    n = text_file.write(output)
    text_file.close()

#response = client.list_jobs()

crawlers=[]
response = client.list_crawlers()
for crawler in response['CrawlerNames']:
    crawlers.append(crawler)
while "NextToken" in response:
    response = client.list_crawlers(NextToken=response["NextToken"])
    for crawler in response['CrawlerNames']:
        crawlers.append(crawler)


crawler_name = input("Enter Crawler name:")
if crawler_name in crawlers:
    GetCrawlerInfo(crawler_name)
else:
    if crawler_name == "":
        print('process all Crawlers')
        for crawler in crawlers:
            GetCrawlerInfo(crawler)
    else:
        print(F'Crawler name {crawler_name} not found')
print('Done')