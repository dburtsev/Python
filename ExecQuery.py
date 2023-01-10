#-------------------------------------------------------------------------------
# Name:        module1
# Purpose:	   Work with Redshift from Lambda/Glue
#
# Author:      dburtsev
#
# Created:     04/08/2022
# Copyright:   (c) dburtsev 2022
# Licence:     <your licence>
#-------------------------------------------------------------------------------
import boto3

def main():
    pass

if __name__ == '__main__':
    main()

def ExecuteNonQuery(Boto3Client,ClusterIdentifier,Database,DbUser,Sql):
    #boto3.set_stream_logger('')
    rows = 0
    Response1 = Boto3Client.execute_statement(ClusterIdentifier=ClusterIdentifier,Database=Database,DbUser=DbUser,Sql=Sql)
    Response2 = Boto3Client.describe_statement(Id=Response1['Id'])
    while Response2['Status'] in ['PICKED', 'STARTED', 'SUBMITTED']:
        Response2 = Boto3Client.describe_statement(Id=Response1['Id'])
    if Response2['Status'] != 'FINISHED':
        print("Expect FINISHED got " + Response2['Status'])
        raise ValueError("Expect FINISHED got " + Response2['Status'] + ' ' + Response2['Error'])
    rows = int(Response2["ResultRows"])
    return rows

def ExecuteScalar(Boto3Client,ClusterIdentifier,Database,DbUser,Sql):
    Response1 = Boto3Client.execute_statement(ClusterIdentifier=ClusterIdentifier,Database=Database,DbUser=DbUser,Sql=Sql)
    Response2 = Boto3Client.describe_statement(Id=Response1['Id'])
    while Response2['Status'] in ['PICKED', 'STARTED', 'SUBMITTED']:
        Response2 = Boto3Client.describe_statement(Id=Response1['Id'])
    if Response2['Status'] != 'FINISHED':
        print("Expect FINISHED got " + Response2['Status'])
        raise ValueError("Expect FINISHED got " + Response2['Status'] + ' ' + Response2['Error'])
    if Response2['HasResultSet'] == False:
        return None
    else:
        # now get record from query
        Response3 = Boto3Client.get_statement_result(Id=Response1['Id'])
        rows, meta = Response3["Records"], Response3["ColumnMetadata"]
        if len(rows) == 0:
            return None
        else:
            return list(rows[0][0].values())[0]

# You’re limited to retrieving only 100 MB of data with the Data API.
def ExecuteReader(Boto3Client,ClusterIdentifier,Database,DbUser,Sql):
    Response1 = Boto3Client.execute_statement(ClusterIdentifier=ClusterIdentifier,Database=Database,DbUser=DbUser,Sql=Sql)
    Response2 = Boto3Client.describe_statement(Id=Response1['Id'])
    while Response2['Status'] in ['PICKED', 'STARTED', 'SUBMITTED']:
        Response2 = Boto3Client.describe_statement(Id=Response1['Id'])
    if Response2['Status'] != 'FINISHED':
        print("Expect FINISHED got " + Response2['Status'])
        raise ValueError("Expect FINISHED got " + Response2['Status'] + ' ' + Response2['Error'])
    if Response2['HasResultSet'] == False:
        return None
    else:
        # now get record from query
        Response3 = Boto3Client.get_statement_result(Id=Response1['Id'])
        #TotalNumRows = int(Response3["TotalNumRows"])
        #print(TotalNumRows)
        return tuple(Response3["Records"])


# Not working. Bug in boto3 always return 0 rows.
def ExecuteCopy(Boto3Client,ClusterIdentifier,Database,DbUser,Sql):
    #boto3.set_stream_logger('')
    rows = 0
    Response1 = Boto3Client.execute_statement(ClusterIdentifier=ClusterIdentifier,Database=Database,DbUser=DbUser,Sql=Sql)
    time.sleep(3)
    Response2 = Boto3Client.describe_statement(Id=Response1['Id'])
    while Response2['Status'] in ['PICKED', 'STARTED', 'SUBMITTED']:
        Response2 = Boto3Client.describe_statement(Id=Response1['Id'])
    if Response2['Status'] != 'FINISHED':
        print("Expect FINISHED got " + Response2['Status'])
        raise ValueError("Expect FINISHED got " + Response2['Status'] + ' ' + Response2['Error'])
    rows = int(Response2["ResultRows"])
    return rows


#print(boto3.__version__)
sql_client = boto3.client('redshift-data', region_name = 'xyz')
sql = "INSERT INTO dev.test10 (strng)  VALUES ('abc2'),('qwe2');"
ClusterIdentifier = 'qwe'
Database = 'asd'
DbUser = 'zxc'

i = ExecuteNonQuery(sql_client,ClusterIdentifier,Database,DbUser,sql)
print(i)

sql = "SELECT 1 AS clmn, 4 AS clmn2 UNION ALL SELECT 2, 5"
x = ExecuteScalar(sql_client,ClusterIdentifier,Database,DbUser,sql)
print(x)

sql = "SELECT CAST('t' AS BOOLEAN) AS clmn1, CAST(2.2 AS FLOAT4) AS clmn2, CAST(2.3 AS FLOAT) AS clmn3 UNION ALL SELECT CAST('f' AS BOOLEAN), 3.0, 4.5;"
tbl = ExecuteReader(sql_client,ClusterIdentifier,Database,DbUser,sql)
print(tbl)


