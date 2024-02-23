#-------------------------------------------------------------------------------
# Name:        GetWorkflows
# Purpose:  script workflow with triggers to directory
# Author:      Dmitriy.Burtsev
#-------------------------------------------------------------------------------
import boto3
import sys
import yaml
import os
import shutil
import awskeys

def main():
    pass

if __name__ == '__main__':
    main()

client = boto3.client('glue',region_name=awskeys.AWS_DEFAULT_REGION,aws_access_key_id=awskeys.AWS_ACCESS_KEY_ID, aws_secret_access_key=awskeys.AWS_SECRET_ACCESS_KEY,aws_session_token=awskeys.AWS_SESSION_TOKEN)

def getworkflow(wfname):
    response = client.get_workflow(Name=wfname,IncludeGraph=True)
    nodes = response['Workflow']['Graph']['Nodes']
    #print(response)
    dict0 = {'Type': 'AWS::Glue::Workflow'}
    dict0.update({'Name': response['Workflow']['Name']})
    if 'Description' in response['Workflow']:
        dict0.update({'Description': response['Workflow']['Description']})
    if 'DefaultRunProperties' in response['Workflow']:
        dict0.update({'DefaultRunProperties': response['Workflow']['DefaultRunProperties']})
    if 'MaxConcurrentRuns' in response['Workflow']:
        dict0.update({'MaxConcurrentRuns' : response['Workflow']['MaxConcurrentRuns']})
    dict0.update({'Properties' : nodes})
    output = yaml.dump(dict0, default_flow_style=False)
    cur_dir = os.path.realpath(os.path.dirname(__file__))
    fixed_wfname = wfname.replace('/','_')
    wfdir = os.path.join(cur_dir, fixed_wfname)
    # if subdir alrady exust - delete it ewith file
    isExist = os.path.exists(wfdir)
    if isExist:
        shutil.rmtree(wfdir)
    os.mkdir(wfdir)
    yaml_file = os.path.join(wfdir, (fixed_wfname +".yaml"))
    text_file = open(yaml_file, "w")
    #n = text_file.write("AWSTemplateFormatVersion: 2010-09-09\nType: AWS::Glue::Workflow\n")
    n = text_file.write(output)
    text_file.close()

response = client.list_workflows()
wfs = response['Workflows'] # list
while "NextToken" in response:
    response = client.list_workflows(NextToken=response["NextToken"])
    wfs = wfs + response['Workflows']

wfName = input("Enter workflow name:") # 'Test-Workflow'
if wfName == "":
    print('process all workflows')
    for wf in wfs:
        print(wf)
        getworkflow(wf)
else:
    if wfName in wfs:
        print('found ' + wfName)
        getworkflow(wfName)
    else:
        print('Workflow ' + wfName + ' not found')
        sys.exit(1)
print('Done')