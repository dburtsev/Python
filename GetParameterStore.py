#-------------------------------------------------------------------------------
# Name:        module1
# Purpose:  Script parameter into directory
# Author:      Dmitriy.Burtsev
#-------------------------------------------------------------------------------
import boto3
import yaml
import os
import shutil
import sys
import awskeys

def main():
    pass

if __name__ == '__main__':
    main()

ssm_client = boto3.client('ssm',region_name=awskeys.AWS_DEFAULT_REGION,aws_access_key_id=awskeys.AWS_ACCESS_KEY_ID, aws_secret_access_key=awskeys.AWS_SECRET_ACCESS_KEY,aws_session_token=awskeys.AWS_SESSION_TOKEN)
next_token = ' '
param_names = []

response = ssm_client.describe_parameters()
for param in response['Parameters']:
    param_names += [param['Name']] # convert string to list
while "NextToken" in response:
    response = ssm_client.describe_parameters(NextToken=response["NextToken"])
    for param in response['Parameters']:
        param_names += [param['Name']]

param_name = input("Enter Parameter Store name:") # "test_param" #
if param_name in param_names:
    response = ssm_client.get_parameter(Name=param_name, WithDecryption=True)
    dict0 = {'Type': 'AWS::SSM::Parameter'}
    dict0.update({'Properties': response['Parameter']})
    response = ssm_client.describe_parameters(Filters=[{'Key': 'Name', 'Values': [param_name]}])
    if 'Description' in response['Parameters'][0]:
        dict0['Properties']['Description'] = response['Parameters'][0]['Description']
    dict0['Properties']['Policies'] = response['Parameters'][0]['Policies']
    dict0['Properties']['Tier'] = response['Parameters'][0]['Tier']
    del dict0['Properties']['LastModifiedDate']
    output = yaml.dump(dict0, default_flow_style=False)
    cur_dir = os.path.realpath(os.path.dirname(__file__))
    fixed_fname = param_name.replace('/','_')
    pdir = os.path.join(cur_dir, fixed_fname)
    # if subdir alrady exust - delete it ewith file
    isExist = os.path.exists(pdir)
    if isExist:
        shutil.rmtree(pdir)
    os.mkdir(pdir)
    yaml_file = os.path.join(pdir, (fixed_fname +".yaml"))
    text_file = open(yaml_file, "w")

    #n = text_file.write("AWSTemplateFormatVersion: 2010-09-09\nType: AWS::Glue::Workflow\n")
    n = text_file.write(output)
    #n = text_file.writelines(nodes)
    text_file.close()
else:
    print(param_name + ' not found')
print('Done')



