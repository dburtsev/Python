#-------------------------------------------------------------------------------
# Name:        GetSecretsManager
# Purpose:      Script secret manager to directory
# Author:      Dmitriy.Burtsev
#-------------------------------------------------------------------------------
import yaml # PyYAML
import boto3
import traceback
#import time
import shutil
import os
from botocore.exceptions import ClientError
import awskeys

def main():
    pass

if __name__ == '__main__':
    main()

client = boto3.client('secretsmanager',region_name=awskeys.AWS_DEFAULT_REGION,aws_access_key_id=awskeys.AWS_ACCESS_KEY_ID, aws_secret_access_key=awskeys.AWS_SECRET_ACCESS_KEY,aws_session_token=awskeys.AWS_SESSION_TOKEN)

next_token = ' '
secrets = []

response = client.list_secrets()
for scrt in response['SecretList']:
    secrets += [scrt['Name']] # convert string to list
while "NextToken" in response:
    response = client.list_secrets(NextToken=response["NextToken"])
    for scrt in response['SecretList']:
        secrets = secrets + [scrt['Name']]

#print(secrets)
secret_name = input("Enter Secret name:")
if secret_name in secrets:
    try:
        response = client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        raise e
    dict0 = {'Type': 'AWS::SecretsManager::Secret'}
    dict1 = {'Name': response['Name']}
    dict1.update({'SecretString': response['SecretString']})
    response = client.list_secrets(Filters=[{'Key': 'name', 'Values': [secret_name]}])
    dict1.update({'Description': response['SecretList'][0]['Description']})
    dict1.update({'Tags': response['SecretList'][0]['Tags']})
    dict0.update({'Properties': dict1})
    output = yaml.dump(dict0, default_flow_style=False)
    cur_dir = os.path.realpath(os.path.dirname(__file__))
    fixed_fname = secret_name.replace('/','_')
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
else:
    print(secret_name + ' not found')
print('Done')