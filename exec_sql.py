#-------------------------------------------------------------------------------
# Name:        exec_sql
# Purpose:     execute Redshift SQL command
#
# Author:      dburtsev
#
# Created:     04/08/2022
#-------------------------------------------------------------------------------
# Example
#sql_client = boto3.client('redshift-data', region_name = 'us-east-1')
#sql = "INSERT INTO dev.test10 (strng)  VALUES ('abc2'),('qwe2');"
#ClusterIdentifier = 'redmocs'
#Database = 'mocsdw'
#DbUser = 'xyz'
#i = ExecuteNonQuery(sql_client,ClusterIdentifier,Database,DbUser,SQL)
# sql = "SELECT CAST('t' AS BOOLEAN) AS clmn1, CAST(2.2 AS FLOAT4) AS clmn2, CAST(2.3 AS FLOAT) AS clmn3 UNION ALL SELECT CAST('f' AS BOOLEAN), 3.0::FLOAT4, 4.5::FLOAT;"
# tbl = ExecuteReader(sql_client,ClusterIdentifier,Database,DbUser,sql)
#for row in tbl:
    #column_1 = list(row[0].values())[0]
    #column_2 = list(row[1].values())[0]
    #column_3 = list(row[2].values())[0]
    #print(F"{column_1} | {column_2} | {column_3}")
import time

# Executes a SQL statement against the connection and returns the number of rows affected.
def ExecuteNonQuery(Boto3Client,ClusterIdentifier,Database,DbUser,Sql):
    rows = 0
    Response1 = Boto3Client.execute_statement(ClusterIdentifier=ClusterIdentifier,Database=Database,DbUser=DbUser,Sql=Sql)
    Response2 = Boto3Client.describe_statement(Id=Response1['Id'])
    while Response2['Status'] in ['PICKED', 'STARTED', 'SUBMITTED']:
        time.sleep(2)  # Wait before rechecking
        Response2 = Boto3Client.describe_statement(Id=Response1['Id'])
    if Response2['Status'] != 'FINISHED':
        print("Expect FINISHED got " + Response2['Status'])
        raise ValueError("Expect FINISHED got " + Response2['Status'] + ' ' + Response2['Error'])
    rows = int(Response2["ResultRows"])
    return rows

# Executes the query, and returns the fir#st column of the first row in the result set returned by the query. Additional columns or rows are ignored.
def ExecuteScalar(Boto3Client,ClusterIdentifier,Database,DbUser,Sql):
    Response1 = Boto3Client.execute_statement(ClusterIdentifier=ClusterIdentifier,Database=Database,DbUser=DbUser,Sql=Sql)
    Response2 = Boto3Client.describe_statement(Id=Response1['Id'])
    while Response2['Status'] in ['PICKED', 'STARTED', 'SUBMITTED']:
        time.sleep(2)  # Wait before rechecking
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

# Return output from SQL query as tuple
# You’re limited to retrieving only 100 MB of data with the Data API.
def ExecuteReader(Boto3Client,ClusterIdentifier,Database,DbUser,Sql):
    Response1 = Boto3Client.execute_statement(ClusterIdentifier=ClusterIdentifier,Database=Database,DbUser=DbUser,Sql=Sql)
    Response2 = Boto3Client.describe_statement(Id=Response1['Id'])
    while Response2['Status'] in ['PICKED', 'STARTED', 'SUBMITTED']:
        time.sleep(2)  # Wait before rechecking
        Response2 = Boto3Client.describe_statement(Id=Response1['Id'])
    if Response2['Status'] != 'FINISHED':
        print("Expect FINISHED got " + Response2['Status'])
        raise ValueError("Expect FINISHED got " + Response2['Status'] + ' ' + Response2['Error'])
    if Response2['HasResultSet'] == False:
        return None
    else:
        # now get record from query
        Response3 = Boto3Client.get_statement_result(Id=Response1['Id'])
        return tuple(Response3["Records"])

# Runs one or more SQL statements, which can be data manipulation language (DML) or data definition language (DDL).
def ExecuteBatch(Boto3Client,ClusterIdentifier,Database,DbUser,Sql):
    # Execute batch statements
    Response1 = Boto3Client.execute_statement(ClusterIdentifier=ClusterIdentifier,Database=Database,DbUser=DbUser,Sql=Sql)
    Response2 = Boto3Client.describe_statement(Id=Response1['Id'])
    while Response2['Status'] in ['PICKED', 'STARTED', 'SUBMITTED']:
        time.sleep(2)  # Wait before rechecking

        Response2 = Boto3Client.describe_statement(Id=Response1['Id'])
    if Response2['Status'] != 'FINISHED':
        print("Expect FINISHED got " + Response2['Status'])
        raise ValueError("Expect FINISHED got " + Response2['Status'] + ' ' + Response2['Error'])

# Runs one or more SQL statements, which can be data manipulation language (DML) or data definition language (DDL).
# as one transaction
def ExecuteBatchTran(Boto3Client,ClusterIdentifier,Database,DbUser,Sql):
    # Execute batch statements
    Response1 = Boto3Client.batch_execute_statement(
        ClusterIdentifier=ClusterIdentifier,
        Database=Database,
        DbUser=DbUser,
        Sqls=Sql)
    Response2 = Boto3Client.describe_statement(Id=Response1['Id'])
    while Response2['Status'] in ['PICKED', 'STARTED', 'SUBMITTED']:
        time.sleep(2)  # Wait before rechecking

        Response2 = Boto3Client.describe_statement(Id=Response1['Id'])
    if Response2['Status'] != 'FINISHED':
        print("Expect FINISHED got " + Response2['Status'])
        raise ValueError("Expect FINISHED got " + Response2['Status'] + ' ' + Response2['Error'])
