# gl_jb_deploy
# This job deploy AWS objects from CodeCommit
import yaml
import boto3
import traceback
import base64
import json
import sys
import time
import os
from awsglue.utils import getResolvedOptions
from botocore.errorfactory import ClientError
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from urllib.parse import urlparse

args = getResolvedOptions(sys.argv, ['JOB_NAME','release'])
sc = SparkContext()
glueContext = GlueContext(sc)
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
logger = glueContext.get_logger()
logger.info("start")
sts_client = boto3.client('sts')
sts_response = sts_client.get_caller_identity()
Account = sts_response["Account"] # Get-STSCallerIdentity 685276224587 dev
logger.info("The Amazon Web Services account ID number: " + Account)

def deploy_job (release, git_client, job_name, yamlFile, s3client, repName):
    job_descr = ""
    parsedurl = None
    logger.info("Start with " + job_name)
    if "Description" in yamlFile["Properties"]:
        job_descr = yamlFile["Properties"]["Description"]
    job_role = ""
    if "Role" in yamlFile["Properties"]:
        job_role = yamlFile["Properties"]["Role"]
    max_concurrent_runs = ""
    if "ExecutionProperty" in yamlFile["Properties"]:
        if "MaxConcurrentRuns" in yamlFile["Properties"]["ExecutionProperty"]:
            max_concurrent_runs = yamlFile["Properties"]["ExecutionProperty"]["MaxConcurrentRuns"]
    command_name = ""
    command_script_location = ""
    command_python_version = ""
    if "Command" in yamlFile["Properties"]:
        if "Name" in yamlFile["Properties"]["Command"]:
            command_name = yamlFile["Properties"]["Command"]["Name"]
        if "ScriptLocation" in yamlFile["Properties"]["Command"]:
            command_script_location = yamlFile["Properties"]["Command"]["ScriptLocation"]
            # change script location for prod
            if Account != '685276224587':
                command_script_location.replace('685276224587', Account)
        if "PythonVersion" in yamlFile["Properties"]["Command"]:
            command_python_version = yamlFile["Properties"]["Command"]["PythonVersion"]
    timeout = int(2880) # default
    if "Timeout" in yamlFile["Properties"]:
        timeout = int(yamlFile["Properties"]["Timeout"])
    dic_def_arguments = {}
    if "DefaultArguments" in yamlFile["Properties"]:
        for x in yamlFile["Properties"]["DefaultArguments"]:
            dic_def_arguments[x] = yamlFile["Properties"]["DefaultArguments"][x]
    connections = ""
    if "Connections" in yamlFile["Properties"]:
        #logger.info('yaml connections: ' + str(yamlFile["Properties"]["Connections"]["Connections"]))
        for conn in yamlFile["Properties"]["Connections"]["Connections"]:
            if connections == "":
                connections = connections + conn
            else:
                connections = connections + "," + conn
        logger.info('job connections will be ' + str(connections))
    #max_capacity - Do not set Max Capacity if using WorkerType and NumberOfWorkers.
    number_of_workers = int(10) # default
    if "NumberOfWorker" in yamlFile["Properties"]:
        number_of_workers = int(yamlFile["Properties"]["NumberOfWorker"])
    worker_type = ""
    if "WorkerType" in  yamlFile["Properties"]:
        worker_type = yamlFile["Properties"]["WorkerType"]
    glue_version = ""
    if "GlueVersion" in yamlFile["Properties"]:
        glue_version = yamlFile["Properties"]["GlueVersion"]
    if command_script_location != "":
        indx = command_script_location.index('-us-east-1') + 11
        tmp_str = command_script_location[indx:]
        script_path = 'Release' + str(release) + '/' + job_name + '/' + command_script_location[indx:]
        scrresponse = cclient.get_file(repositoryName = 'Deployment', filePath = script_path)
        job_script = scrresponse["fileContent"].decode('utf-8')
        parsedurl = urlparse(command_script_location, allow_fragments=False)
    # do we have extra python libraries?
    lib_list = [] # empty list
    jar_list = []
    file_list = []
    total_list = [] # lib_list + jar_list + file_list
    if "DefaultArguments" in yamlFile["Properties"]:
        if "--extra-py-files" in yamlFile["Properties"]["DefaultArguments"]:
                lib_list = (yamlFile["Properties"]["DefaultArguments"]["--extra-py-files"]).split(",")
    if "DefaultArguments" in yamlFile["Properties"]:
        if "--extra-jars" in yamlFile["Properties"]["DefaultArguments"]:
                jar_list = (yamlFile["Properties"]["DefaultArguments"]["--extra-jars"]).split(",")
    if "DefaultArguments" in yamlFile["Properties"]:
        if "--extra-files" in yamlFile["Properties"]["DefaultArguments"]:
                file_list = (yamlFile["Properties"]["DefaultArguments"]["--extra-files"]).split(",")
    total_list = lib_list + jar_list + file_list
    if total_list != []:
        for f_name in total_list:
            #logger.info(F'f_name is {f_name}')
            pos = f_name[5:].find('/') + 5
            bucket = f_name[5:pos]
            f_name_short = f_name[6 + len(bucket):]
            logger.info(F"trying to delpoy file {f_name_short}")
            # check if library already exist
            found_file = True
            try:
                s3client.head_object(Bucket = bucket, Key = f_name_short)
                logger.info(F"file {f_name_short} already exist")
            except ClientError:
                found_file = False
                logger.info(F"file {f_name_short} not exist")

            # check if we have files in codeCommit
            if found_file == False:
                file_response = git_client.get_file(repositoryName = repName, filePath = "{0}/{1}/{2}".format(('Release' + str(release)), job_name, f_name_short))
                response = s3client.put_object(Body=file_response["fileContent"], Bucket=bucket, Key=f_name_short)
                logger.info(F"New File {f_name_short} ETag = " + response["ETag"])
            else: # file already exist on s3, do we have this file in deployment codeCommit?
                try:
                    file_response = git_client.get_file(repositoryName = repName, filePath = "{0}/{1}/{2}".format(('Release' + str(release)), job_name, f_name_short))
                except ClientError:
                    logger.info(F"File {f_name_short} already exists in s3 but not exists in CodeCommit. Will use old version")
                    continue
                response = s3client.put_object(Body=file_response["fileContent"], Bucket=bucket, Key=f_name_short)
                logger.info(F"Updated File {f_name_short} ETag = " + response["ETag"])
                
    # create parameters            
    dic_job = {} # empty dictionary
    dic_job["Description"] = job_descr
    dic_job["Role"] = job_role
    dic_job["ExecutionProperty"] = { 'MaxConcurrentRuns': int(max_concurrent_runs) }
    dic_job["Command"] = { 'Name': command_name, 'ScriptLocation': command_script_location, 'PythonVersion': command_python_version}
    dic_job["DefaultArguments"] = dic_def_arguments
    if len(connections) > 0:
        dic_job["Connections"]= {'Connections': connections.split(',')}
    dic_job["Timeout"] = timeout
    dic_job["WorkerType"] = worker_type
    dic_job["NumberOfWorkers"] = number_of_workers
    dic_job["GlueVersion"] = glue_version

    new_job = False
    # check if job exist in dest
    glue_client = boto3.client('glue')
    try:
      response = glue_client.get_job(JobName=job_name)
    except:
        new_job = True
    response = {}
    if(new_job):
        dic_job["Name"] = job_name
        response = glue_client.create_job(**dic_job)
        print('create job ' + job_name)
    else:
        response = glue_client.update_job(
        JobName = job_name,
        JobUpdate = dic_job)
        print('update job ' + job_name)
    #deploy job command ScriptLocation
    response = s3client.put_object(Body=job_script, Bucket=parsedurl.netloc, Key=parsedurl.path[1:])
    logger.info('load job command script to s3, Bucket=' + parsedurl.netloc + ", Key=" + parsedurl.path[1:] + ', ETag=' + response["ETag"])
    logger.info("done with " + job_name + ", HTTPStatusCode:" + str((response["ResponseMetadata"])["HTTPStatusCode"]))
    print((response["ResponseMetadata"])["HTTPStatusCode"]) # must be 200

def deploy_lambda_layer (release, git_client, layer_name, yamlFile, repName):
    lambda_client = boto3.client('lambda')
    LayerName = 'arn:aws:lambda:us-east-1:' + Account + ':layer:' + layer_name
    CompatibleArchitectures = []
    if "CompatibleArchitectures" in yamlFile["Properties"]:
        CompatibleArchitectures = yamlFile["Properties"]["CompatibleArchitectures"]
    CompatibleRuntimes = []
    if "CompatibleRuntimes" in yamlFile["Properties"]:
        CompatibleRuntimes = yamlFile["Properties"]["CompatibleRuntimes"]
    Description = ""
    if "Description" in yamlFile["Properties"]:
        Description = yamlFile["Properties"]["Description"]
    # Get layer file info. Do we have one zip file or many?
    folder_info = git_client.get_folder(repositoryName=repName,folderPath=('Release' + str(release) + '/' + layer_name))
    one_file = True
    for file in folder_info["files"]:
        if file["relativePath"].endswith(".part"):
            one_file = False
            break
    BlobId = ""
    ZipFiles = {} # empty dict
    BlobIds = [] # empty list
    bytes_var = bytes()
    print("check zip")
    if (one_file):
        print('one zip file')
        for file in folder_info["files"]:
            if not file["relativePath"].endswith(".yaml"):
                BlobId = file["blobId"]
                response = git_client.get_blob(repositoryName=repName, blobId=BlobId)
                bytes_var = response['content']
    else:
        print('many zip files')
        for file in folder_info["files"]:
            if file["relativePath"].endswith(".part"):
                ZipFiles[file["relativePath"]] = file["blobId"]
                print(file["relativePath"] + " " + file["blobId"])
        for Key in sorted(ZipFiles):
            response = git_client.get_blob(repositoryName=repName, blobId=ZipFiles[Key])
            bytes_var = bytes_var + response['content']
    # deploy Layers
    try:
        response = lambda_client.publish_layer_version(LayerName=LayerName,Description=Description,Content={'ZipFile': bytes_var},CompatibleRuntimes=CompatibleRuntimes)
    except Exception as e:
        logger.info('Error in deploy_lambda_layer publish_layer_version : {}'.format(str(e)))
        raise e
    logger.info("Deploy {0} version {1}".format(response["LayerArn"], response["Version"]))

def deploy_lambda (release, git_client, lambda_name, yamlFile, repName):
    lambda_client = boto3.client('lambda')
    logger.info(lambda_name)
    folder_info = git_client.get_folder(repositoryName=repName,folderPath=('Release' + release + '/' + lambda_name))
    Code_ZipFile = bytes()
    for file in folder_info["files"]:
        if (file["relativePath"] == (lambda_name + '.zip')):
            response = git_client.get_blob(repositoryName=repName, blobId=file['blobId'])
            Code_ZipFile = response["content"]
    Handler = ""
    if "Handler" in yamlFile["Properties"]:
        Handler = yamlFile["Properties"]["Handler"]
    Runtime = ""
    if "Runtime" in yamlFile["Properties"]:
        logger.info("Runtime is " + yamlFile["Properties"]["Runtime"])
        Runtime = yamlFile["Properties"]["Runtime"]
    Role = '' ## arn:aws:iam::685276224587:role/MOCSDW_IAM_ODSFMS_LAMBDA_GLUE
    if "Role" in yamlFile["Properties"]:
        Role = 'arn:aws:iam::' + Account + ':role/' + yamlFile["Properties"]["Role"]
    Architectures = []
    if "Architectures" in yamlFile["Properties"]:
        Architectures = yamlFile["Properties"]["Architectures"]
    Description = ""
    if "Description" in yamlFile["Properties"]:
        Description = yamlFile["Properties"]["Description"]
    Timeout = 3 #Default
    if Timeout in yamlFile["Properties"]:
        Timeout = yamlFile["Properties"]["Timeout"]
    TracingConfig = {}
    if "TracingConfig" in yamlFile["Properties"]:
        TracingConfig = yamlFile["Properties"]["TracingConfig"]
    VpcConfig = {}
    if "VpcConfig" in yamlFile["Properties"]:
        VpcConfig = yamlFile["Properties"]["VpcConfig"]
    EphemeralStorage = {}
    if "EphemeralStorage" in yamlFile["Properties"]:
        EphemeralStorage = yamlFile["Properties"]["EphemeralStorage"]
    MemorySize = 512 # default
    if "MemorySize" in yamlFile["Properties"]:
        MemorySize = yamlFile["Properties"]["MemorySize"]
    if "FunctionName" in yamlFile["Properties"]:
        FunctionName = yamlFile["Properties"]["FunctionName"]
    Layers = []
    if "Layers" in yamlFile["Properties"]:
        Layers = []
        for layer in yamlFile["Properties"]["Layers"].split(","):
            layer_arn = 'arn:aws:lambda:us-east-1:' + Account + ':layer:' + layer
            # get latest layer version
            response = lambda_client.list_layer_versions(LayerName=layer)
            layer_version = 0
            for l_v in response['LayerVersions']:
                if l_v['Version'] > layer_version:
                    layer_version = l_v['Version']
            layer_arn = layer_arn + ':' + str(layer_version)
            Layers.append(layer_arn)
    Environment = {}
    if "Environment" in yamlFile["Properties"]:
         Environment = yamlFile["Properties"]["Environment"]
    # Is this new function?
    new_funct = True
    try:
        response = lambda_client.get_function(FunctionName=FunctionName)
        new_funct = False
    except ClientError as e:
        pass
    if new_funct:
        response = lambda_client.create_function(FunctionName=FunctionName,Runtime=Runtime,Role=Role,Handler=Handler,Code={'ZipFile':Code_ZipFile},Description=Description,Timeout=Timeout,MemorySize=MemorySize,Publish=True,VpcConfig=VpcConfig,PackageType='Zip',TracingConfig=TracingConfig,Layers=Layers,Architectures=Architectures,Environment=Environment)
        update_state = response['State']
        logger.info(F"create_function {FunctionName}, State is {update_state}")
    else:
        response = lambda_client.update_function_configuration(FunctionName=FunctionName,Role=Role,Handler=Handler,Description=Description,Timeout=Timeout,MemorySize=MemorySize,VpcConfig=VpcConfig,Runtime=Runtime,Layers=Layers,Environment=Environment)
        update_state = response['State']
        logger.info(F"update_function_configuration for {FunctionName}, State is {update_state}" )
        update_status = ''
        while update_status != 'Successful':
            response = lambda_client.get_function(FunctionName=FunctionName)
            update_status = response['Configuration']['LastUpdateStatus']
        response = lambda_client.update_function_code(FunctionName=FunctionName,ZipFile=Code_ZipFile,Publish=True,Architectures=Architectures)
        logger.info(F"update_function_code for {FunctionName} " + response['State'])

def deploy_workflow (wf_name, yamlFile):
    glue_client = boto3.client('glue')
    # get the key name - we have only one element in this dict
    wf_name = ""
    tr_type = ""
    tr_name = ""
    tr_actions = []
    Description = ""
    MaxConcurrentRuns = None
    wf_name = yamlFile['Name']

    # delete wf and triggers it they exists
    response = glue_client.list_workflows()
    wfs = response['Workflows'] # list
    while "NextToken" in response:
        response = glue_client.list_workflows(NextToken=response["NextToken"])
        wfs = wfs + response['Workflows']
    #print(type(yamlFile['Resources'][wf_name]['Properties'])) # list
    DefaultRunProperties = yamlFile['DefaultRunProperties']
    Description = ''
    if 'Description' in yamlFile:
        Description = yamlFile["Description"]
    if 'MaxConcurrentRuns' in yamlFile:
        MaxConcurrentRuns = yamlFile['MaxConcurrentRuns']
    wf_list = yamlFile['Properties']
    for dic in wf_list:
        #print(type(dic))
        if "Name" in dic:
            #print(dic["Name"])
            if "Type" in dic:
                if dic["Type"] == "TRIGGER":
                    tr_name = dic["Name"]
                    #print('delete trigger ' + tr_name)
                    glue_client.delete_trigger(Name=tr_name)
                    # Adding wait logic to make sure deletes are completed
                    try:
                        response=glue_client.get_trigger(Name=tr_name)
                        if response['Trigger']['State']=='DELETING':
                            while True:
                                #print('Wait for ' + tr_name)
                                time.sleep(5) # Unexpected error: An error occurred (ThrottlingException) when calling the GetTrigger operation (reached max retries: 4): Rate exceeded
                                response=glue_client.get_trigger(Name=tr_name)
                                if response['Trigger']['State']=='DELETING':
                                    continue
                    except ClientError as e:
                        if e.response['Error']['Code'] != 'EntityNotFoundException':
                            #print("Expected: Entity Not Found Exception")
                            logger.info("Unexpected error: %s" % e)
                            raise
    logger.info("delete " + wf_name)
    glue_client.delete_workflow(Name=wf_name)
    # wait for workflow deletion
    while True:
        try:
            response = glue_client.get_workflow(Name=wf_name, IncludeGraph=False)
        except ClientError as e:
            if e.response['Error']['Code'] == 'EntityNotFoundException':
                break
            else:
                raise
    # create workflow
    if MaxConcurrentRuns == None:
        response = glue_client.create_workflow(Name=wf_name,Description=Description,DefaultRunProperties=DefaultRunProperties)
    else:
        response = glue_client.create_workflow(Name=wf_name,Description=Description,DefaultRunProperties=DefaultRunProperties,MaxConcurrentRuns=MaxConcurrentRuns)
    logger.info("create " + response['Name'])

    # create triggers
    for dic in wf_list:
        if "Name" in dic:
            if "Type" in dic:
                if dic["Type"] == "TRIGGER":
                    tr_name = dic["Name"]
                    logger.info('create trigger ' + tr_name)
                    tr_details_dic = dic["TriggerDetails"]
                    tr_dic = tr_details_dic["Trigger"]
                    tr_actions = tr_dic["Actions"]
                    tr_type = tr_dic["Type"]
                    response = {} # empty dic
                    Description = ""
                    if "Description" in tr_dic:
                        Description = tr_dic["Description"]
                    if tr_type == "ON_DEMAND":
                        response = glue_client.create_trigger(Name=tr_name, Type=tr_type, WorkflowName=wf_name, Actions=tr_actions,Description=Description)
                    elif tr_type == "CONDITIONAL":
                        tr_predicate = tr_dic["Predicate"]
                        response = glue_client.create_trigger(Name=tr_name,Type=tr_type,WorkflowName=wf_name,Actions=tr_actions,Predicate=tr_predicate,StartOnCreation=True,Description=Description)
                    elif tr_type == "SCHEDULED":
                        Schedule = tr_dic["Schedule"]
                        response = glue_client.create_trigger(Name=tr_name,Type=tr_type,WorkflowName=wf_name,Actions=tr_actions,Schedule=Schedule,StartOnCreation=True,Description=Description)
                    else : # "EVENT"
                        response = glue_client.create_trigger(Name=tr_name, Type=tr_type, WorkflowName=wf_name, Actions=tr_actions,Description=Description)
                    # check if trigger was created
                    response=glue_client.get_trigger(Name=response['Name'])
                    logger.info(response['Trigger']['Name'] + ' State is ' + response['Trigger']['State'])

def deploy_param (param_name, yamlFile):
    ssm_client = boto3.client('ssm')
    # create parameter
    Description=""
    if 'Description' in yamlFile['Properties']:
        Description = yamlFile['Properties']['Description']
    Value = yamlFile['Properties']['Value']
    Type = yamlFile['Properties']['Type']
    KeyId = ""
    if 'KeyId' in yamlFile['Properties']:
        KeyID = yamlFile['Properties']['KeyId']
    Overwrite=True
    AllowedPattern = ""
    if 'AllowedPattern' in yamlFile['Properties']:
        AllowedPattern = yamlFile['Properties']['AllowedPattern']
    Tags=[]
    if 'Tags' in yamlFile['Properties']:
        Tags = yamlFile['Properties']['Tags']
    DataType = yamlFile['Properties']['DataType']
    Policies = None
    if Policies in yamlFile['Properties']:
        Policies = yamlFile['Properties']['Pplicies']
    Tier = ""
    if 'Tier' in yamlFile['Properties']:
        Tier = yamlFile['Properties']['Tier']
    if Policies is None:
        response = ssm_client.put_parameter(Name=param_name,Description=Description,Value=Value,Type=Type,Overwrite=True,AllowedPattern=AllowedPattern,Tags=Tags,Tier=Tier,DataType=DataType)
    else:
        response = ssm_client.put_parameter(Name=param_name,Description=Description,Value=Value,Type=Type,Overwrite=True,AllowedPattern=AllowedPattern,Tags=Tags,Tier=Tier,Policies=Policies,DataType=DataType)
    logger.info('Create parameter {0} version {1}'.format(param_name, response['Version']))

def deploy_secret(secret_name, yamlFile):
    ssm_client = boto3.client('secretsmanager')
    Name = yamlFile['Properties']['Name']
    Description = None
    if 'Description' in yamlFile['Properties']:
        Description = yamlFile['Properties']['Description']
    SecretString = yamlFile['Properties']['SecretString']
    Tags = None
    if 'Tags' in yamlFile['Properties']:
        Tags = yamlFile['Properties']['Tags']
    isNew = True
    SecretId = None
    response = None
    try:
        response = ssm_client.get_secret_value(SecretId=Name)
        SecretId = response['ARN']
        isNew = False
    except ClientError as e:
        pass

    if isNew:
        response = ssm_client.create_secret(Name=Name,Description=Description,SecretString=SecretString,Tags=Tags)
    else:
        response = ssm_client.update_secret(SecretId=SecretId,Description=Description,SecretString=SecretString)
    logger.info("Create or update secret " + response['Name'])

def deploy_crawler(crawler_name, yamlFile):
    glue_client = boto3.client('glue')
    Name = yamlFile['Properties']['Name']
    Role = yamlFile['Properties']['Role']
    DatabaseName = yamlFile['Properties']['DatabaseName']
    Description = ''
    if 'Description' in yamlFile['Properties']:
        Description = yamlFile['Properties']['Description']
    Schedule = ''
    if 'Schedule' in yamlFile['Properties']:
        Schedule = yamlFile['Properties']['Schedule']
    Classifiers = yamlFile['Properties']['Classifiers']
    TablePrefix = ''
    if 'TablePrefix' in yamlFile['Properties']:
        TablePrefix = yamlFile['Properties']['TablePrefix']
    SchemaChangePolicy = yamlFile['Properties']['SchemaChangePolicy']
    RecrawlPolicy = yamlFile['Properties']['RecrawlPolicy']
    LineageConfiguration = yamlFile['Properties']['LineageConfiguration']
    LakeFormationConfiguration = yamlFile['Properties']['LakeFormationConfiguration']
    Configuration = ''
    if 'Configuration' in yamlFile['Properties']:
        Configuration = yamlFile['Properties']['Configuration']
    CrawlerSecurityConfiguration = ''
    if 'CrawlerSecurityConfiguration' in yamlFile['Properties']:
        CrawlerSecurityConfiguration = yamlFile['Properties']['CrawlerSecurityConfiguration']
    Targets = yamlFile['Properties']['Targets']
    new_crawler = True
    try:
        glue_client.get_crawler(Name=crawler_name)
        new_crawler = False
    except ClientError as e:
        pass
    if new_crawler:
        response = glue_client.create_crawler(Name=Name,Role=Role,DatabaseName=DatabaseName,Description=Description,Targets=Targets,Schedule=Schedule,Classifiers=Classifiers,TablePrefix=TablePrefix,\
        SchemaChangePolicy=SchemaChangePolicy,RecrawlPolicy=RecrawlPolicy,LineageConfiguration=LineageConfiguration,LakeFormationConfiguration=LakeFormationConfiguration,Configuration=Configuration,\
        CrawlerSecurityConfiguration=CrawlerSecurityConfiguration)
    else:
        response = glue_client.update_crawler(Name=Name,Role=Role,DatabaseName=DatabaseName,Description=Description,Targets=Targets,Schedule=Schedule,Classifiers=Classifiers,TablePrefix=TablePrefix,\
        SchemaChangePolicy=SchemaChangePolicy,RecrawlPolicy=RecrawlPolicy,LineageConfiguration=LineageConfiguration,LakeFormationConfiguration=LakeFormationConfiguration,Configuration=Configuration,\
        CrawlerSecurityConfiguration=CrawlerSecurityConfiguration)
    logger.info("Create or update crawler " + crawler_name)

def deploy_database(yamlFile):
    glue_client = boto3.client('glue')
    Name = yamlFile['Properties']['Name']
    DatabaseInput = yamlFile['Properties']
    new_db = True
    try:
        glue_client.get_database(Name=Name)
        new_db = False
    except:
        pass
    if new_db:
        response = glue_client.create_database(DatabaseInput=DatabaseInput)
    else:
        response = glue_client.update_database(Name=Name,DatabaseInput=DatabaseInput)
    logger.info("Create or update Glue database " + Name)

def deploy_table(yamlFile):
    glue_client = boto3.client('glue')
    DatabaseName=yamlFile['DatabaseName']
    TableInput = yamlFile['TableInput']
    Name = yamlFile['TableInput']['Name']
    new_table = True
    try:
        glue_client.get_table(DatabaseName=DatabaseName, Name=Name)
        new_table = False
    except:
        pass
    if new_table:
        response = glue_client.create_table(DatabaseName=DatabaseName, TableInput=TableInput)
    else:
        response = glue_client.update_table(DatabaseName=DatabaseName, TableInput=TableInput)
    logger.info("Create or update Glue table " + Name)

repName = 'Deployment'
rel = args['release']
ccFolderName = 'Release' + str(rel)
#env = args['destination']

#gets the dev account and creates a client to access dev code commit
RoleSessionName=""
if Account == "555366363216":
    RoleSessionName="ProdGlue"
    assumed_role_object=sts_client.assume_role(RoleArn="arn:aws:iam::685276224587:role/Glue_Full_Perms",RoleSessionName=RoleSessionName)
    credentials=assumed_role_object['Credentials']
    cclient = boto3.client('codecommit',region_name='us-east-1',
    aws_access_key_id=credentials['AccessKeyId'],
    aws_secret_access_key=credentials['SecretAccessKey'],
    aws_session_token=credentials['SessionToken'])
else:
    RoleSessionName="PreProdGlue"
    cclient = boto3.client('codecommit',region_name='us-east-1')

# create s3 client
s3client = boto3.client('s3')

# check if Release4 folder exist
fresponse = {} # Initialize an empty Dictionary
try:
    fresponse = cclient.get_folder(
    repositoryName = repName,
    folderPath = ccFolderName
    )
except:
    print(traceback.format_exc())
    raise

subFolders = fresponse['subFolders'] # <class 'list'>
#files = fresponse['files'] # <class 'list'>

#do we have subfolders?
if len(subFolders) > 0:
    for directory in subFolders:
        # Process Lambda Layers first
        yresponse = cclient.get_file(repositoryName = repName, filePath = "{0}/{1}/{1}.yaml".format(ccFolderName, directory["relativePath"]))
        yamlFile = yaml.safe_load(yresponse["fileContent"].decode('utf-8'))
        resource_type = yamlFile["Type"]
        if resource_type == "AWS::Lambda::LayerVersion":
            Layer_arn = yamlFile["Properties"]["LayerName"]
            Name = (Layer_arn.split(":"))[-1]
            logger.info(F"Found AWS::Lambda::LayerVersion {Name}")
            deploy_lambda_layer (release=rel, git_client=cclient, layer_name=Name, yamlFile=yamlFile, repName=repName)

    # done with Lambda Layer deployment, start Lambda deployment
    for directory in subFolders:
        # Process Lambda Layers first
        yresponse = cclient.get_file(repositoryName = repName, filePath = "{0}/{1}/{1}.yaml".format(ccFolderName, directory["relativePath"]))
        yamlFile = yaml.safe_load(yresponse["fileContent"].decode('utf-8'))
        resource_type = yamlFile["Type"]
        if resource_type == "AWS::Lambda::Function":
            FunctionArn = yamlFile["Properties"]["FunctionArn"]
            Name = (FunctionArn.split(":"))[6]
            logger.info(F"Found AWS::Lambda::Function {Name}")
            deploy_lambda (release=rel, git_client=cclient, lambda_name=Name, yamlFile=yamlFile, repName=repName)

    #os._exit(0)
    # now process glue jobs
    for directory in subFolders:
        # it it Glue job? Load bml file
        yresponse = cclient.get_file(repositoryName = repName, filePath = "{0}/{1}/{1}.yaml".format(ccFolderName, directory["relativePath"]))
        txtFile = yresponse["fileContent"].decode('utf-8')
        yamlFile = yaml.safe_load(txtFile)
        resource_type = yamlFile["Type"]
        if resource_type == "AWS::Glue::Job":
            Name = yamlFile["Properties"]["Name"]
            logger.info(F"Found AWS::Glue::Job {Name}")
            deploy_job (release=rel, git_client=cclient, job_name=directory["relativePath"], yamlFile=yamlFile, s3client=s3client, repName=repName)

    # process Glue databases before Glue tables and Crawlers
    for directory in subFolders:
        yresponse = cclient.get_file(repositoryName = repName, filePath = "{0}/{1}/{1}.yaml".format(ccFolderName, directory["relativePath"]))
        txtFile = yresponse["fileContent"].decode('utf-8')
        yamlFile = yaml.safe_load(txtFile)
        resource_type = yamlFile["Type"]
        if resource_type == "AWS::Glue::Database":
            Name = yamlFile["Properties"]["Name"]
            logger.info(F"Found AWS::Glue::Database {Name}")
            deploy_database(yamlFile)

    # now process everything else
    for directory in subFolders:
        yresponse = cclient.get_file(repositoryName = repName, filePath = "{0}/{1}/{1}.yaml".format(ccFolderName, directory["relativePath"]))
        txtFile = yresponse["fileContent"].decode('utf-8')
        yamlFile = yaml.safe_load(txtFile)
        resource_type = yamlFile["Type"]
        if resource_type == "AWS::Glue::Workflow":
            Name = yamlFile["Name"]
            logger.info(F"Found AWS::Glue::Workflow {Name}")
            deploy_workflow(Name, yamlFile)
        elif resource_type == "AWS::SecretsManager::Secret":
            Name = yamlFile["Properties"]["Name"]
            logger.info(F"Found AWS::SecretsManager::Secret {Name}")
            deploy_secret(Name, yamlFile)
        elif resource_type == "AWS::SSM::Parameter":
            Name = yamlFile["Properties"]["Name"]
            logger.info(F"Found AWS::SSM::Parameter {Name}")
            deploy_param(Name, yamlFile)
        elif resource_type == "AWS::Glue::Crawler":
            Name = yamlFile["Properties"]["Name"]
            logger.info(F"Found AWS::Glue::Crawler {Name}")
            deploy_crawler(Name, yamlFile)
        elif resource_type ==  "AWS::Glue::Table":
            Name = yamlFile["TableInput"]["Name"]
            logger.info(F"Found AWS::Glue::Table {Name}")
            deploy_table(yamlFile)

# deploy (config) files if exists
try:
    file_response = cclient.get_file(repositoryName=repName, filePath="{0}/{1}".format(ccFolderName, "files.txt"))
    txtFile = file_response["fileContent"].decode('utf-8')
    for file in txtFile.splitlines():
        absolutePath = file
        file_name = os.path.basename(absolutePath)
        file_response = cclient.get_file(repositoryName=repName, filePath="{0}/{1}".format(ccFolderName, file_name))
        bucket, key = absolutePath.replace("s3://", "").split("/", 1)
        response = s3client.put_object(Body=file_response["fileContent"], Bucket=bucket, Key=key)
        logger.info(F'Deploy {file_name} to s3, ETag =' + response["ETag"])
except:
    logger.info("files.txt not found, no files to deploy")
    pass

logger.info("end")
job.commit()