#-------------------------------------------------------------------------------
# Name:        module1
# Purpose:  Get Glue job code and properties and save it to directory
# Author:      Dmitriy.Burtsev
#-------------------------------------------------------------------------------
import yaml # PyYAML
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
s3_client = boto3.client('s3', region_name=awskeys.AWS_DEFAULT_REGION,aws_access_key_id=awskeys.AWS_ACCESS_KEY_ID, aws_secret_access_key=awskeys.AWS_SECRET_ACCESS_KEY,aws_session_token=awskeys.AWS_SESSION_TOKEN)

def DownloadS3(files, local_dir):
    file_list = files.split(',')
    for file in file_list:
        if file == "":
            continue
        tmp_str = file[5::] # remove s3://
        bucket_name = tmp_str.split('/')[0]
        i = 0
        file_name = local_dir
        tmp_arr = tmp_str.split('/')
        for member in tmp_arr:
            i = i + 1
            if i == 1:
                bucket_name = member
                continue
            if i < len(tmp_arr):
                file_name = os.path.join(file_name, member)
                os.mkdir(file_name)
            else:
                file_name = os.path.join(file_name, member)
        object_name = tmp_str[(len(bucket_name) + 1)::]
        #print(object_name)
        try:
            s3_client.download_file(bucket_name,object_name,file_name)
        except ClientError as e:
            if e.response['Error']['Code'] == "404":
                print("The object does not exist.")
            else:
                raise
        # Maximum of 6 MB for any individual file when using the CodeCommit console, APIs, or the AWS CLI.
        file_size = os.path.getsize(file_name) / (1024*1024.0) # bytes -> megabyte
        if file_size > 6.0:
            CHUNK_SIZE = 6*1024*1024 # 6MB
            file_number = 1
            with open(file_name) as f:
                chunk = f.read(CHUNK_SIZE)
                while chunk:
                    with open(os.path.basename(file_name) + str(file_number)) as chunk_file:
                        chunk_file.write(chunk)
                    file_number += 1
                    chunk = f.read(CHUNK_SIZE)
            os.remove(file_name)

def GetJobInfo(jobname):
    try:
        response = client.get_job(JobName=jobname)
    except ClientError as e:
        raise e
    dict0 = {'Type': 'AWS::Glue::Job'}
    dict0.update({'Properties' : response['Job']})
    output = yaml.dump(dict0, default_flow_style=False)
    cur_dir = os.path.realpath(os.path.dirname(__file__))
    fixed_fname = jobname
    pdir = os.path.join(cur_dir, fixed_fname)
    # if subdir alrady exust - delete it ewith file
    isExist = os.path.exists(pdir)
    if isExist:
        shutil.rmtree(pdir)
    os.mkdir(pdir)
    yaml_file = os.path.join(pdir, (fixed_fname +".yaml"))
    text_file = open(yaml_file, "w")
    n = text_file.write(output)
    text_file.close()
    script_loc = dict0['Properties']['Command']['ScriptLocation']
    DownloadS3(script_loc,pdir)
    if '--extra-jars' in dict0['Properties']['DefaultArguments']:
        DownloadS3(dict0['Properties']['DefaultArguments']['--extra-jars'],pdir)
    if '--additional-python-modules' in dict0['Properties']['DefaultArguments']:
        DownloadS3(dict0['Properties']['DefaultArguments']['--additional-python-modules'],pdir)
    if '--extra-files' in dict0['Properties']['DefaultArguments']:
        DownloadS3(dict0['Properties']['DefaultArguments']['--extra-files'],pdir)
    if '--extra-py-files' in dict0['Properties']['DefaultArguments']:
        DownloadS3(dict0['Properties']['DefaultArguments']['--extra-py-files'],pdir)

jobs=[]
response = client.list_jobs()
for job in response['JobNames']:
    jobs.append(job)
while "NextToken" in response:
    response = client.list_jobs(NextToken=response["NextToken"])
    for job in response['JobNames']:
        jobs.append(job)

#job_name = "JDBC_Direct" #
job_name = input("Enter Glue job name:")
if job_name in jobs:
    GetJobInfo(job_name)
else:
    raise Exception(F"job {job_name} not found)")
print('Done')