#-------------------------------------------------------------------------------
# Name:        GetLambda
# Purpose:  This script will get propertyis of lambda function and save it into directory
# Author:      Dmitriy.Burtsev
#-------------------------------------------------------------------------------
import yaml # PyYAML
import boto3
import traceback
import shutil
import os
from botocore.exceptions import ClientError
import requests
import awskeys

def main():
    pass

if __name__ == '__main__':
    main()

client = boto3.client('lambda',region_name=awskeys.AWS_DEFAULT_REGION,aws_access_key_id=awskeys.AWS_ACCESS_KEY_ID, aws_secret_access_key=awskeys.AWS_SECRET_ACCESS_KEY,aws_session_token=awskeys.AWS_SESSION_TOKEN)

def DownloadS3(files, local_dir):
    s3_client = boto3.client('s3',region_name=awskeys.AWS_DEFAULT_REGION,aws_access_key_id=awskeys.AWS_ACCESS_KEY_ID, aws_secret_access_key=awskeys.AWS_SECRET_ACCESS_KEY,aws_session_token=awskeys.AWS_SESSION_TOKEN)
    file_list = files.split(',')
    for file in file_list:
        if file == "":
            continue
        tmp_str = file[5::] # remove s3://
        bucket_name = tmp_str.split('/')[0]
        object_name = file[6+len(bucket_name)::]
        x = object_name.rfind('/') + 1
        file_name = os.path.join(local_dir,object_name[x::])
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
            with open(file_name,'rb') as f:
                chunk = f.read(CHUNK_SIZE)
                while chunk:
                    with open(file_name + str(file_number),"wb") as chunk_file:
                        chunk_file.write(chunk)
                    file_number += 1
                    chunk = f.read(CHUNK_SIZE)
            os.remove(file_name)

def GetLayerInfo(layer_arn):
    cur_dir = os.path.realpath(os.path.dirname(__file__))
    l_list = layer_arn.split(":")
    fixed_fname = l_list[6]
    pdir = os.path.join(cur_dir, fixed_fname)
    isExist = os.path.exists(pdir)
    if isExist:
        shutil.rmtree(pdir)
    os.mkdir(pdir)
    layer_file = os.path.join(pdir, (fixed_fname))
    yaml_file = os.path.join(pdir, (fixed_fname +".yaml"))
    try:
        response =client.get_layer_version_by_arn(Arn=layer_arn)
##        print(response['Content']['Location'])
    except ClientError as e:
        raise e
    r = requests.get(response['Content']['Location'], allow_redirects=True)
    open(layer_file, 'wb').write(r.content)
    dict0 = {'Type': 'AWS::Lambda::LayerVersion'}
    dict1 = {'LayerName' : response['LayerArn']}
    dict1.update({'Description' : response['Description']})
    dict1.update({'Version' : response['Version']})
    dict1.update({'CompatibleRuntimes' : response['CompatibleRuntimes']})
    dict0.update({'Properties' : dict1})
    output = yaml.dump(dict0, default_flow_style=False)
    text_file = open(yaml_file, "w")
    n = text_file.write(output)
    text_file.close()
    # Maximum of 6 MB for any individual file when using the CodeCommit console, APIs, or the AWS CLI.
    file_size = os.path.getsize(layer_file) / (1024*1024.0) # bytes -> megabyte
    if file_size > 6.0:
        CHUNK_SIZE = 6*1024*1024 # 6MB
        file_number = 1
        with open(layer_file,'rb') as f:
            chunk = f.read(CHUNK_SIZE)
            while chunk:
                with open(layer_file + str(file_number) + ".part","wb") as chunk_file:
                    chunk_file.write(chunk)
                file_number += 1
                chunk = f.read(CHUNK_SIZE)
        os.remove(layer_file)

def getLambdaInfo(lambdaname):
    try:
        response = client.get_function(FunctionName=lambdaname)
    except ClientError as e:
        raise e
    dict0 = {'Type': 'AWS::Lambda::Function'}
    dict0.update({'Properties' : response['Configuration']})
    # remove VpcId from VpcConfig
    if 'VpcConfig' in dict0['Properties']:
        if 'VpcId' in dict0['Properties']['VpcConfig']:
            del dict0['Properties']['VpcConfig']['VpcId']
    # get role name from Arn : arn:aws:iam::685276224587:role/MOCSDW_IAM_ODSFMS_LAMBDA_GLUE
    if 'Role' in dict0['Properties']:
        tmp_list = dict0['Properties']['Role'].split(':')
        dict0['Properties']['Role'] = (tmp_list[5])[5:]
    cur_dir = os.path.realpath(os.path.dirname(__file__))
    fixed_fname = lambdaname
    pdir = os.path.join(cur_dir, fixed_fname)
    # if subdir alrady exust - delete it ewith file
    isExist = os.path.exists(pdir)
    if isExist:
        shutil.rmtree(pdir)
    os.mkdir(pdir)
    lambda_code = os.path.join(pdir, (fixed_fname + ".zip"))
    r = requests.get(response['Code']['Location'], allow_redirects=True)
    open(lambda_code, 'wb').write(r.content)
    # do we have Layers?
    if 'Layers' in dict0['Properties']:
        lyr_str = ""
        l_list = dict0['Properties']['Layers']
        for layer_arn in l_list:
            GetLayerInfo(layer_arn['Arn'])
            tmp_list = layer_arn['Arn'].split(':')
            if lyr_str == "":
                lyr_str = tmp_list[6]
            else:
                lyr_str = "," + tmp_list[6]
        dict0['Properties']['Layers'] = lyr_str
    yaml_file = os.path.join(pdir, (fixed_fname + ".yaml"))
    output = yaml.dump(dict0, default_flow_style=False)
    text_file = open(yaml_file, "w")
    n = text_file.write(output)
    text_file.close()

functions=[]
response = client.list_functions()
for function in response['Functions']:
    functions.append(function['FunctionName'])
while "NextMarker" in response:
    response = client.list_functions(Marker=response["NextMarker"])
    for function in response['Functions']:
        functions.append(function['FunctionName'])

# function_name = "ll_test" #
function_name = input("Enter Glue job name:")
if function_name in functions:
    getLambdaInfo(function_name)
else:
    raise Exception(F"function {function_name} not found)")

print('Done')
