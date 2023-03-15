import sys, os
import boto3
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
print("start")
table_name = 't_ctr_contract'
schema_name = 'dev'
rows_cnt = 0
sqlserver_jdbc_url = ""
redshift_jdbc_url = ""
jks_file = ""
pem_file = ""
script_name = args["JOB_NAME"]

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

logger = glueContext.get_logger()
logger.info('Getting details for connection ')
glue_client = boto3.client('glue')
getjob=glue_client.get_job(JobName=args["JOB_NAME"])
jar_file = os.path.basename(getjob['Job']['DefaultArguments']['--extra-jars'])

e_files = getjob['Job']['DefaultArguments']['--extra-files']
if e_files != "":
    e_files_list = e_files.split(",")
    for e_file in e_files_list:
        file_name = os.path.basename(e_file)
        for dir_name in sys.path:
            candidate = os.path.join(dir_name, file_name)
            if os.path.isfile(candidate):
                if candidate.endswith('.jks'):
                    jks_file = candidate
                    break
					
conn_list = getjob['Job']['Connections']['Connections']
if len(conn_list) < 2:
    raise Exception("Must have at least two connections")
    
for conn_name in conn_list:
    df = glueContext.extract_jdbc_conf(conn_name)
    url = df.get('fullUrl')
    if url.startswith('jdbc:redshift'):
        redshift_jdbc_url = url + ';user=' + df.get('user') + ';password=' + df.get('password') 
    elif url.startswith('jdbc:sqlserver'):
        pem_file = df.get('customJDBCCert')
        sqlserver_jdbc_url = url + ';user=' + df.get('user') + ';password=' + df.get('password') + ';hostNameInCertificate=' + df.get('customJDBCCertString') + ';enforceSSL=' + df.get('enforceSSL') + F';customJDBCCert={pem_file};trustServerCertificate=false'
    else:
        raise Exception("Unknown database " + url)
    
logger.info('run java_import')
from py4j.java_gateway import java_import
java_import(sc._gateway.jvm,"java.sql.Connection")
java_import(sc._gateway.jvm,"java.sql.DatabaseMetaData")
java_import(sc._gateway.jvm,"java.sql.DriverManager")
java_import(sc._gateway.jvm,"java.sql.SQLException")

spark.sparkContext.addPyFile(jar_file)

# get Java Interface Connection
aws_conn = sc._gateway.jvm.DriverManager.getConnection(redshift_jdbc_url)
aws_stmt = aws_conn.createStatement()

conn = sc._gateway.jvm.DriverManager.getConnection(sqlserver_jdbc_url)
conn.setAutoCommit(True)
stmt = conn.createStatement()

logger.info('SQLServerBulkCopy')
bulkCopy = spark.sparkContext._jvm.com.microsoft.sqlserver.jdbc.SQLServerBulkCopy(sqlserver_jdbc_url)

batch_id = 0
sql_txt = "INSERT INTO ods.etl_batch (src) VALUES ('{0}')".format(script_name)
logger.info(sql_txt)
aws_stmt.executeUpdate(sql_txt)
sql_txt = "SELECT MAX(etl_bch_id) AS newid FROM ods.etl_batch WHERE src = '{0}';".format(script_name)
rs = aws_stmt.executeQuery(sql_txt)
while (rs.next()):
    batch_id = rs.getInt('newid')
    
logger.info("batch_id is {0}".format(batch_id))
    
tmp_str = 'Connected to ' + aws_conn.getMetaData().getDatabaseProductName() + ' ' + aws_conn.getMetaData().getDatabaseProductVersion() + ', JDBC Driver ' + str(aws_conn.getMetaData().getJDBCMajorVersion()) + '.' + str(aws_conn.getMetaData().getJDBCMinorVersion()) + ' version ' + aws_conn.getMetaData().getDriverVersion()
logger.info(tmp_str)
sql_txt = "INSERT INTO ods.etl_audit (etl_bch_id,cpo_nm,sta,msg) VALUES({0},'{1}','{2}','{3}')".format(batch_id,script_name,'SUCCESS',tmp_str)
aws_stmt.executeUpdate(sql_txt)
tmp_str = 'Connected to ' + conn.getMetaData().getDatabaseProductName() + ' ' + conn.getMetaData().getDatabaseProductVersion() + ', JDBC Driver ' + str(conn.getMetaData().getJDBCMajorVersion()) + '.' + str(conn.getMetaData().getJDBCMinorVersion()) + ' version ' + conn.getMetaData().getDriverVersion()
logger.info(tmp_str)
sql_txt = "INSERT INTO ods.etl_audit (etl_bch_id,cpo_nm,sta,msg) VALUES({0},'{1}','{2}','{3}')".format(batch_id,script_name,'SUCCESS',tmp_str)
aws_stmt.executeUpdate(sql_txt)

sql_txt = "SELECT ti.tbl_rows AS clmn FROM SVV_TABLE_INFO ti WHERE ti.schema = '{0}' AND ti.table = '{1}';".format(schema_name,table_name)    
logger.info(sql_txt)
rs = aws_stmt.executeQuery(sql_txt)
while (rs.next()):
	rows_cnt = rs.getInt('clmn')
	
# expected Redshift table structure
logger.info("expected Redshift table structure")
sql_txt = """
SET NOCOUNT ON;
SET QUOTED_IDENTIFIER ON;
SELECT 'CREATE TABLE {0}.{2} (' + STRING_AGG(CAST(LOWER(c.COLUMN_NAME) as VARCHAR(MAX)) + ' ' +
CASE(c.DATA_TYPE)
WHEN 'bit' THEN 'BOOLEAN'
WHEN 'date' THEN 'DATE' 
WHEN 'time' THEN 'TIME' 
WHEN 'datetime' THEN 'TIMESTAMP' 
WHEN 'datetime2' THEN 'TIMESTAMP' 
WHEN 'smalldatetime' THEN 'TIMESTAMP' 
WHEN 'float' THEN 'FLOAT'
WHEN 'nchar' THEN 'CHAR(' + CAST(c.CHARACTER_MAXIMUM_LENGTH AS VARCHAR) + ')' 
WHEN 'char' THEN 'CHAR(' + CAST(c.CHARACTER_MAXIMUM_LENGTH AS VARCHAR) + ')' 
WHEN 'decimal' THEN ('DECIMAL(' + CAST((COALESCE(c.NUMERIC_PRECISION, 0)) AS VARCHAR) + ',' + CAST((COALESCE(c.NUMERIC_SCALE, 0)) AS VARCHAR) + ')') 
WHEN 'numeric' THEN ('NUMERIC(' + CAST((COALESCE(c.NUMERIC_PRECISION, 0)) AS VARCHAR) + ',' + CAST((COALESCE(c.NUMERIC_SCALE, 0)) AS VARCHAR) + ')') 
WHEN 'nvarchar' THEN 'VARCHAR(' + CASE WHEN c.CHARACTER_MAXIMUM_LENGTH = -1 THEN '65535' ELSE CAST(c.CHARACTER_MAXIMUM_LENGTH AS VARCHAR) END + ')' 
WHEN 'varchar' THEN 'VARCHAR(' + CASE WHEN c.CHARACTER_MAXIMUM_LENGTH = -1 THEN '65535' ELSE CAST(c.CHARACTER_MAXIMUM_LENGTH AS VARCHAR) END + ')' 
WHEN 'real' THEN 'REAL' 
WHEN 'tinyint' THEN 'SMALLINT' 
WHEN 'smallint' THEN 'SMALLINT' 
WHEN 'int' THEN 'INT'
WHEN 'bigint' THEN 'INT8'
WHEN 'uniqueidentifier' THEN 'CHAR(36)' 
WHEN 'xml' THEN 'VARCHAR(65535)' 
WHEN 'varbinary' THEN 'CHAR(6)'
WHEN 'binary' THEN 'CHAR(6)'
WHEN 'timestamp' THEN 'CHAR(18)'
WHEN 'hierarchyid' THEN 'VARCHAR(4000)'
ELSE c.DATA_TYPE END + 
CASE WHEN c.IS_NULLABLE = 'NO' THEN ' NOT NULL ' ELSE '' END 
,',') + ')'
FROM INFORMATION_SCHEMA.COLUMNS c
WHERE c.TABLE_SCHEMA = '{1}' AND c.TABLE_NAME ='{2}';
""".format(schema_name,'dbo',table_name)
rs = stmt.executeQuery(sql_txt)
while (rs.next()):
    sql_txt=rs.getString(1)
logger.info(sql_txt)
	
# drop temp table if exist
sql_txt = "IF OBJECT_ID('tempdb..##{0}') IS NOT NULL DROP TABLE ##{0}l;".format(table_name)
logger.info(sql_txt)
try:
    stmt.executeUpdate(sql_txt)
except:
    pass

# create temp table	
sql_txt = """
SET NOCOUNT ON;
SET QUOTED_IDENTIFIER ON;
DECLARE @TBL_NAME VARCHAR(128) = '{0}';
SELECT 'CREATE TABLE ##' + @TBL_NAME + ' (' +
STUFF((SELECT ',' + c.COLUMN_NAME + ' ' + c.DATA_TYPE +
CASE WHEN c.DATA_TYPE = 'decimal' THEN ' (' + CAST(c.NUMERIC_PRECISION AS VARCHAR) + ',' + CAST(c.NUMERIC_SCALE AS VARCHAR) + ') ' 
WHEN c.DATA_TYPE IN ('char','varchar','nvarchar') THEN ' (' + CAST(c.CHARACTER_MAXIMUM_LENGTH AS VARCHAR) + ') '
ELSE '' END
FROM INFORMATION_SCHEMA.COLUMNS c 
LEFT OUTER JOIN sys.indexes i ON i.object_id = OBJECT_ID((c.TABLE_SCHEMA + '.' + c.TABLE_NAME), N'U') AND i.is_primary_key = 1
LEFT OUTER JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id 
AND COLUMNPROPERTY(OBJECT_ID((c.TABLE_SCHEMA + '.' + c.TABLE_NAME), N'U'), c.COLUMN_NAME, 'ColumnId') = ic.column_id
WHERE c.TABLE_NAME = @TBL_NAME AND c.TABLE_SCHEMA = 'dbo' AND ic.index_column_id IS NOT NULL
ORDER BY c.ORDINAL_POSITION
FOR XML PATH (''), TYPE).value('.', 'VARCHAR(4000)'),1,1,'')
+ ' NOT NULL, chcksum int NOT NULL);'
""".format(table_name)
logger.info(sql_txt)
rs = stmt.executeQuery(sql_txt)
while (rs.next()):
    sql_txt=rs.getString(1)
logger.info(sql_txt)
stmt.executeUpdate(sql_txt)

sql_txt = "INSERT INTO ods.etl_audit (etl_bch_id,cpo_nm,sta,msg) VALUES({0},'{1}','{2}','{3}')".format(batch_id,script_name,'SUCCESS',('CREATE ##' + table_name))
aws_stmt.executeUpdate(sql_txt)

# get select statement from Redshift
sql_txt = """
SET NOCOUNT ON;
SET QUOTED_IDENTIFIER ON;
DECLARE @TBL_NAME VARCHAR(128) = '{1}';
SELECT 'SELECT ' + 
STUFF((SELECT ',' + c.COLUMN_NAME 
FROM INFORMATION_SCHEMA.COLUMNS c 
LEFT OUTER JOIN sys.indexes i ON i.object_id = OBJECT_ID((c.TABLE_SCHEMA + '.' + c.TABLE_NAME), N'U') AND i.is_primary_key = 1
LEFT OUTER JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id 
AND COLUMNPROPERTY(OBJECT_ID((c.TABLE_SCHEMA + '.' + c.TABLE_NAME), N'U'), c.COLUMN_NAME, 'ColumnId') = ic.column_id
WHERE c.TABLE_NAME = @TBL_NAME AND c.TABLE_SCHEMA = 'dbo' AND ic.index_column_id IS NOT NULL
ORDER BY c.ORDINAL_POSITION
FOR XML PATH (''), TYPE).value('.', 'VARCHAR(4000)'),1,1,'') + ',chcksum FROM {0}.{1};'
""".format(schema_name,table_name)
logger.info(sql_txt)
rs = stmt.executeQuery(sql_txt)
while (rs.next()):
    sql_txt=rs.getString(1)

logger.info(sql_txt)    
rsSourceData = aws_stmt.executeQuery(sql_txt)  

#logger.info('SQLServerBulkCopyOptions')
#copyOptions = spark.sparkContext._jvm.com.microsoft.sqlserver.jdbc.SQLServerBulkCopyOptions

bulkCopy.setDestinationTableName('##' + table_name)

#if rows_cnt > 0 :
#    copyOptions.setBatchSize(rows_cnt)
#    bulkCopy.setBulkCopyOptions(copyOptions)

bulkCopy.writeToServer(rsSourceData);
sql_txt = "INSERT INTO ods.etl_audit (etl_bch_id,cpo_nm,sta,msg) VALUES({0},'{1}','{2}','{3}')".format(batch_id,script_name,'SUCCESS',('LOAD ' + str(rows_cnt) + ' rows into temp table'))
aws_stmt.executeUpdate(sql_txt) 

sql_txt = """
SET NOCOUNT ON;
SET QUOTED_IDENTIFIER ON;
DECLARE @TBL_NAME VARCHAR(128) = '{0}';
SELECT CAST('SELECT ' AS VARCHAR(MAX)) + STUFF((SELECT 
CASE WHEN DATA_TYPE IN('varchar', 'nvarchar') AND (CHARACTER_MAXIMUM_LENGTH = -1 OR CHARACTER_MAXIMUM_LENGTH > 65000) THEN ',LEFT(t.' + c.COLUMN_NAME + ',65000) AS '+ c.COLUMN_NAME
WHEN DATA_TYPE = 'uniqueidentifier' THEN ',CAST(t.' + c.COLUMN_NAME + ' AS CHAR(36)) AS '+ c.COLUMN_NAME
WHEN DATA_TYPE = 'hierarchyid' THEN ',CAST(t.' + c.COLUMN_NAME + ' AS VARCHAR(4000)) AS '+ c.COLUMN_NAME
WHEN DATA_TYPE = 'xml' THEN ',LEFT(CAST(ISNULL(CAST(t.' + c.COLUMN_NAME + ' AS NVARCHAR(MAX)),'''') AS VARCHAR(MAX)),6500) AS '+ c.COLUMN_NAME
ELSE ',t.' + c.COLUMN_NAME
END 
FROM INFORMATION_SCHEMA.COLUMNS c 
WHERE c.TABLE_NAME = @TBL_NAME AND c.TABLE_SCHEMA = 'dbo' 
ORDER BY c.ORDINAL_POSITION
FOR XML PATH (''), TYPE).value('.', 'VARCHAR(MAX)'),1,1,'') 
+ ',t.chcksum FROM (SELECT ' +  
STUFF((SELECT ',' + c.COLUMN_NAME
FROM INFORMATION_SCHEMA.COLUMNS c 
WHERE c.TABLE_NAME = @TBL_NAME AND c.TABLE_SCHEMA = 'dbo' 
ORDER BY c.ORDINAL_POSITION
FOR XML PATH (''), TYPE).value('.', 'VARCHAR(MAX)'),1,1,'') + 
+ ', BINARY_CHECKSUM (' +
STUFF((SELECT ',' + c.COLUMN_NAME
FROM INFORMATION_SCHEMA.COLUMNS c 
LEFT OUTER JOIN sys.indexes i ON i.object_id = OBJECT_ID((c.TABLE_SCHEMA + '.' + c.TABLE_NAME), N'U') AND i.is_primary_key = 1
LEFT OUTER JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id 
AND COLUMNPROPERTY(OBJECT_ID((c.TABLE_SCHEMA + '.' + c.TABLE_NAME), N'U'), c.COLUMN_NAME, 'ColumnId') = ic.column_id
WHERE c.TABLE_NAME = @TBL_NAME AND c.TABLE_SCHEMA = 'dbo' AND ic.index_column_id IS NULL
ORDER BY c.ORDINAL_POSITION
FOR XML PATH (''), TYPE).value('.', 'VARCHAR(MAX)'),1,1,'')
+ ') AS chcksum FROM dbo.' + @TBL_NAME + ' ) AS t LEFT OUTER JOIN ##' + @TBL_NAME + ' AS tmp ON ' + 
STUFF((SELECT ' AND t.' + c.COLUMN_NAME + ' = tmp.' + c.COLUMN_NAME
FROM INFORMATION_SCHEMA.COLUMNS c 
LEFT OUTER JOIN sys.indexes i ON i.object_id = OBJECT_ID((c.TABLE_SCHEMA + '.' + c.TABLE_NAME), N'U') AND i.is_primary_key = 1
LEFT OUTER JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id 
AND COLUMNPROPERTY(OBJECT_ID((c.TABLE_SCHEMA + '.' + c.TABLE_NAME), N'U'), c.COLUMN_NAME, 'ColumnId') = ic.column_id
WHERE c.TABLE_NAME = @TBL_NAME AND c.TABLE_SCHEMA = 'dbo' AND ic.index_column_id IS NOT NULL
ORDER BY c.ORDINAL_POSITION
FOR XML PATH (''), TYPE).value('.', 'VARCHAR(MAX)'),1,4,'') + ' AND t.chcksum = tmp.chcksum WHERE tmp.chcksum IS NULL'
""".format(table_name)
logger.info(sql_txt)
rs = stmt.executeQuery(sql_txt)
while (rs.next()):
    sql_txt=rs.getString(1)
logger.info(sql_txt)    
df = spark.read.format("jdbc").option("url", sqlserver_jdbc_url).option("query", sql_txt).load()
# extracting number of rows from the Dataframe
row = df.count()
logger.info('There are ' + str(row) + ' rows') 
   
# extracting number of columns from the Dataframe
col = len(df.columns)
logger.info('There are ' + str(col) + ' columns') 

sql_txt = "INSERT INTO ods.etl_audit (etl_bch_id,cpo_nm,sta,msg) VALUES({0},'{1}','{2}','{3}')".format(batch_id,script_name,'SUCCESS',('There are ' + str(row) + ' rows and ' + str(col) + ' columns in Dataframe'))
aws_stmt.executeUpdate(sql_txt)

logger.info('truncate table ' + table_name + '_diff')
sql_txt = "TRUNCATE TABLE dev.{0}_diff".format(table_name)
aws_stmt.executeUpdate(sql_txt)

# Convert Spark DataFrame to DynamicFrame
dynamic_frame = DynamicFrame.fromDF(df, glueContext, "dynamic_frame")
logger.info('glueContext.write_dynamic_frame')
glueContext.write_dynamic_frame.from_jdbc_conf(
    frame=dynamic_frame,
    catalog_connection="mocsdw",
    connection_options={
        "dbtable": "dev.t_ctr_contract_diff",
        "database": "mocsdw"
    },
    redshift_tmp_dir=args["TempDir"],
    transformation_ctx="dynamic_frame"
)

sql_txt = "INSERT INTO ods.etl_audit (etl_bch_id,cpo_nm,sta,msg,row_cnt) VALUES({0},'{1}','{2}','{3}',{4})".format(batch_id,script_name,'SUCCESS',('glueContext.write_dynamic_frame to ' + table_name + '_diff'),row)
aws_stmt.executeUpdate(sql_txt)

sql_txt = """
SET NOCOUNT ON;
SET QUOTED_IDENTIFIER ON;
DECLARE @TBL_NAME VARCHAR(128) = '{0}';
SELECT CAST('UPDATE ' AS VARCHAR(MAX)) + 'dev.' + @TBL_NAME + ' SET ' + STUFF((SELECT 
 ', ' + c.COLUMN_NAME + ' = diff.' + c.COLUMN_NAME  
FROM INFORMATION_SCHEMA.COLUMNS c 
LEFT OUTER JOIN sys.indexes i ON i.object_id = OBJECT_ID((c.TABLE_SCHEMA + '.' + c.TABLE_NAME), N'U') AND i.is_primary_key = 1
LEFT OUTER JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id 
AND COLUMNPROPERTY(OBJECT_ID((c.TABLE_SCHEMA + '.' + c.TABLE_NAME), N'U'), c.COLUMN_NAME, 'ColumnId') = ic.column_id
WHERE c.TABLE_NAME = @TBL_NAME AND c.TABLE_SCHEMA = 'dbo' AND ic.index_column_id IS NULL
ORDER BY c.ORDINAL_POSITION
FOR XML PATH (''), TYPE).value('.', 'VARCHAR(MAX)'),1,1,'') + ' FROM dev.' + @TBL_NAME + '_diff AS diff WHERE ' +
STUFF((SELECT ' AND dev.' + @TBL_NAME + '.' + c.COLUMN_NAME + ' = diff.' + c.COLUMN_NAME
FROM INFORMATION_SCHEMA.COLUMNS c 
LEFT OUTER JOIN sys.indexes i ON i.object_id = OBJECT_ID((c.TABLE_SCHEMA + '.' + c.TABLE_NAME), N'U') AND i.is_primary_key = 1
LEFT OUTER JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id 
AND COLUMNPROPERTY(OBJECT_ID((c.TABLE_SCHEMA + '.' + c.TABLE_NAME), N'U'), c.COLUMN_NAME, 'ColumnId') = ic.column_id
WHERE c.TABLE_NAME = @TBL_NAME AND c.TABLE_SCHEMA = 'dbo' AND ic.index_column_id IS NOT NULL
ORDER BY c.ORDINAL_POSITION
FOR XML PATH (''), TYPE).value('.', 'VARCHAR(MAX)'),1,4,'') + ' AND dev.' + @TBL_NAME + '.chcksum != diff.chcksum'
""".format(table_name)
logger.info(sql_txt)
rs = stmt.executeQuery(sql_txt)
while (rs.next()):
    sql_txt=rs.getString(1)
logger.info(sql_txt)
rows_cnt = aws_stmt.executeUpdate(sql_txt)
logger.info('updated {0} rows'.format(rows_cnt))
sql_txt = "INSERT INTO ods.etl_audit (etl_bch_id,cpo_nm,sta,msg,row_cnt) VALUES({0},'{1}','{2}','{3}',{4})".format(batch_id,script_name,'SUCCESS',('update ' + table_name),rows_cnt)
aws_stmt.executeUpdate(sql_txt)

sql_txt = """
SET NOCOUNT ON;
SET QUOTED_IDENTIFIER ON;
DECLARE @TBL_NAME VARCHAR(128) = '{0}';
SELECT CAST('INSERT INTO ' AS VARCHAR(MAX)) + 'dev.' + @TBL_NAME + ' ( ' + STUFF((SELECT 
 ', ' + c.COLUMN_NAME  
FROM INFORMATION_SCHEMA.COLUMNS c 
WHERE c.TABLE_NAME = @TBL_NAME AND c.TABLE_SCHEMA = 'dbo'
ORDER BY c.ORDINAL_POSITION
FOR XML PATH (''), TYPE).value('.', 'VARCHAR(MAX)'),1,1,'') + ',chcksum ) SELECT ' +
STUFF((SELECT 
 ', ' + c.COLUMN_NAME  
FROM INFORMATION_SCHEMA.COLUMNS c 
WHERE c.TABLE_NAME = @TBL_NAME AND c.TABLE_SCHEMA = 'dbo'
ORDER BY c.ORDINAL_POSITION
FOR XML PATH (''), TYPE).value('.', 'VARCHAR(MAX)'),1,1,'') + ',chcksum FROM dev.' + @TBL_NAME + '_diff AS tmp WHERE NOT EXISTS (SELECT 1 FROM dev.' + @TBL_NAME + ' AS t WHERE ' +
STUFF((SELECT ' AND t.' + c.COLUMN_NAME + ' = tmp.' + c.COLUMN_NAME
FROM INFORMATION_SCHEMA.COLUMNS c 
LEFT OUTER JOIN sys.indexes i ON i.object_id = OBJECT_ID((c.TABLE_SCHEMA + '.' + c.TABLE_NAME), N'U') AND i.is_primary_key = 1
LEFT OUTER JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id 
AND COLUMNPROPERTY(OBJECT_ID((c.TABLE_SCHEMA + '.' + c.TABLE_NAME), N'U'), c.COLUMN_NAME, 'ColumnId') = ic.column_id
WHERE c.TABLE_NAME = @TBL_NAME AND c.TABLE_SCHEMA = 'dbo' AND ic.index_column_id IS NOT NULL
ORDER BY c.ORDINAL_POSITION
FOR XML PATH (''), TYPE).value('.', 'VARCHAR(MAX)'),1,4,'') + ')'
""".format(table_name)

logger.info(sql_txt)
rs = stmt.executeQuery(sql_txt)
while (rs.next()):
    sql_txt=rs.getString(1)
logger.info(sql_txt)   
rows_cnt = aws_stmt.executeUpdate(sql_txt)
logger.info('inserted {0} rows'.format(rows_cnt))

sql_txt = "INSERT INTO ods.etl_audit (etl_bch_id,cpo_nm,sta,msg,row_cnt) VALUES({0},'{1}','{2}','{3}',{4})".format(batch_id,script_name,'SUCCESS',('insert into ' + table_name),rows_cnt)
aws_stmt.executeUpdate(sql_txt)

sql_txt = "INSERT INTO ods.etl_batch (pnt_id, src) VALUES ({0}, 'END');".format(batch_id)
logger.info(sql_txt)
aws_stmt.executeUpdate(sql_txt)
print("end")
job.commit()
# 