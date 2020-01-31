# -*- coding: utf-8 -*-
"""
# Configuring Redshift cluster
"""

import pandas as pd
import boto3
import json
import configparser
import psycopg2
from time import time
import matplotlib.pyplot as plt
import pandas as pd

config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))

config.sections()

KEY = config.get('AWS', 'KEY')
SECRET = config.get('AWS', 'SECRET')

DWH_CLUSTER_TYPE = config.get("DWH","DWH_CLUSTER_TYPE")
DWH_NUM_NODES = config.get("DWH","DWH_NUM_NODES")
DWH_NODE_TYPE = config.get("DWH","DWH_NODE_TYPE")

DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")
DWH_DB = config.get("DWH","DWH_DB")
DWH_DB_USER= config.get("DWH","DWH_DB_USER")
DWH_DB_PASSWORD = config.get("DWH","DWH_DB_PASSWORD")
DWH_PORT = config.get("DWH","DWH_PORT")

DWH_IAM_ROLE_NAME = config.get("DWH", "DWH_IAM_ROLE_NAME")

# Create EC2, S3, IAM and Redshift clients

# EC2
ec2 = boto3.resource('ec2',
                     region_name = "us-east-1",
                     aws_access_key_id=KEY,
                     aws_secret_access_key=SECRET
                    )

# S3
s3 = boto3.resource('s3',
                     region_name = "us-east-1",
                     aws_access_key_id=KEY,
                     aws_secret_access_key=SECRET
                    )

# IAM
iam = boto3.client('iam',
                     region_name = "us-east-1",
                     aws_access_key_id=KEY,
                     aws_secret_access_key=SECRET
                    )

# redshift
redshift = boto3.client('redshift',
                     region_name = "us-east-1",
                     aws_access_key_id=KEY,
                     aws_secret_access_key=SECRET
                    )

# Resolve linuxacademy issue by deleting and recreating the default VPC

# Create an IAM role for redshift to access the bucket
try:
  dwhRole = iam.create_role(
      Path = '/',
      RoleName = DWH_IAM_ROLE_NAME,
      # The trust relationship policy document that grants an entity 
      # permission to assume the role.
      AssumeRolePolicyDocument=json.dumps(
            {'Statement': [{'Action': 'sts:AssumeRole',
               'Effect': 'Allow',
               'Principal': {'Service': 'redshift.amazonaws.com'}}],
             'Version': '2012-10-17'}),
      Description = "Allows Redshift to call AWS services"
  )
except Exception as e:
  print(e)
  dwhRole = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)

# Attach a policy
iam.attach_role_policy(
    RoleName = DWH_IAM_ROLE_NAME,
    PolicyArn = "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
)['ResponseMetadata']['HTTPStatusCode']

# Get the role ARN
roleArn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']

print(roleArn)

# Create the Redshift cluster
# https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/redshift.html#Redshift.Client.create_cluster
# cluster should be in the same region as the default VPC

try:
  response = redshift.create_cluster(
    DBName=DWH_DB,
    ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
    ClusterType=DWH_CLUSTER_TYPE,
    NumberOfNodes=int(DWH_NUM_NODES),
    NodeType=DWH_NODE_TYPE,
    MasterUsername=DWH_DB_USER,
    MasterUserPassword=DWH_DB_PASSWORD,
    IamRoles=[roleArn]
)
except Exception as e:
  print(e)

clusterprops = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]

keys = ['ClusterIdentifier', 'NodeType', 'ClusterStatus', 'Endpoint']
print('Cluster Properties \n')
for key in keys:
  print(key+':', clusterprops[key])

DWH_ENDPOINT = clusterprops['Endpoint']['Address']
DWH_ROLE_ARN = clusterprops['IamRoles'][0]['IamRoleArn']

# Setup access to cluster endpoint

try:
  vpc = ec2.Vpc(id=clusterprops['VpcId'])
  defaultSg = list(vpc.security_groups.all())[0]
  print(defaultSg)
  defaultSg.authorize_ingress(
      GroupName=defaultSg.group_name,
      CidrIp='0.0.0.0/0',
      IpProtocol='TCP',
      FromPort=int(DWH_PORT),
      ToPort=int(DWH_PORT)
  )
except Exception as e:
  print(e)

# Connect to cluster

# Using psycopg2

# conn = psycopg2.connect(f'host={DWH_ENDPOINT} dbname={DWH_DB} user={DWH_DB_USER} password={DWH_DB_PASSWORD} port={DWH_PORT}')

# cur = conn.cursor()

# conn.set_session(autocommit=True)

# Commented out IPython magic to ensure Python compatibility.
# In notebook
# %load_ext sql

conn_string="postgresql://{}:{}@{}:{}/{}".format(DWH_DB_USER, DWH_DB_PASSWORD, DWH_ENDPOINT, DWH_PORT, DWH_DB)
print(conn_string)

# Commented out IPython magic to ensure Python compatibility.
# %sql $conn_string

# Commented out IPython magic to ensure Python compatibility.
# # Compare distribution strategies
# # Setup 4 dim tables and 1 fact table (lineorder)
# # https://docs.aws.amazon.com/redshift/latest/dg/t_Distributing_data.html
# 
# %%sql
# CREATE SCHEMA IF NOT EXISTS nodist;
# SET search_path TO nodist;
# 
# DROP TABLE IF EXISTS part;
# DROP TABLE IF EXISTS supplier;
# DROP TABLE IF EXISTS customer;
# DROP TABLE IF EXISTS dwdate;
# DROP TABLE IF EXISTS lineorder;
# 
# CREATE TABLE part 
# (
#   p_partkey     INTEGER NOT NULL,
#   p_name        VARCHAR(22) NOT NULL,
#   p_mfgr        VARCHAR(6) NOT NULL,
#   p_category    VARCHAR(7) NOT NULL,
#   p_brand1      VARCHAR(9) NOT NULL,
#   p_color       VARCHAR(11) NOT NULL,
#   p_type        VARCHAR(25) NOT NULL,
#   p_size        INTEGER NOT NULL,
#   p_container   VARCHAR(10) NOT NULL
# );
# 
# CREATE TABLE supplier 
# (
#   s_suppkey   INTEGER NOT NULL,
#   s_name      VARCHAR(25) NOT NULL,
#   s_address   VARCHAR(25) NOT NULL,
#   s_city      VARCHAR(10) NOT NULL,
#   s_nation    VARCHAR(15) NOT NULL,
#   s_region    VARCHAR(12) NOT NULL,
#   s_phone     VARCHAR(15) NOT NULL
# );
# 
# CREATE TABLE customer 
# (
#   c_custkey      INTEGER NOT NULL,
#   c_name         VARCHAR(25) NOT NULL,
#   c_address      VARCHAR(25) NOT NULL,
#   c_city         VARCHAR(10) NOT NULL,
#   c_nation       VARCHAR(15) NOT NULL,
#   c_region       VARCHAR(12) NOT NULL,
#   c_phone        VARCHAR(15) NOT NULL,
#   c_mktsegment   VARCHAR(10) NOT NULL
# );
# 
# CREATE TABLE dwdate 
# (
#   d_datekey            INTEGER NOT NULL,
#   d_date               VARCHAR(19) NOT NULL,
#   d_dayofweek          VARCHAR(10) NOT NULL,
#   d_month              VARCHAR(10) NOT NULL,
#   d_year               INTEGER NOT NULL,
#   d_yearmonthnum       INTEGER NOT NULL,
#   d_yearmonth          VARCHAR(8) NOT NULL,
#   d_daynuminweek       INTEGER NOT NULL,
#   d_daynuminmonth      INTEGER NOT NULL,
#   d_daynuminyear       INTEGER NOT NULL,
#   d_monthnuminyear     INTEGER NOT NULL,
#   d_weeknuminyear      INTEGER NOT NULL,
#   d_sellingseason      VARCHAR(13) NOT NULL,
#   d_lastdayinweekfl    VARCHAR(1) NOT NULL,
#   d_lastdayinmonthfl   VARCHAR(1) NOT NULL,
#   d_holidayfl          VARCHAR(1) NOT NULL,
#   d_weekdayfl          VARCHAR(1) NOT NULL
# );
# 
# CREATE TABLE lineorder 
# (
#   lo_orderkey          INTEGER NOT NULL,
#   lo_linenumber        INTEGER NOT NULL,
#   lo_custkey           INTEGER NOT NULL,
#   lo_partkey           INTEGER NOT NULL,
#   lo_suppkey           INTEGER NOT NULL,
#   lo_orderdate         INTEGER NOT NULL,
#   lo_orderpriority     VARCHAR(15) NOT NULL,
#   lo_shippriority      VARCHAR(1) NOT NULL,
#   lo_quantity          INTEGER NOT NULL,
#   lo_extendedprice     INTEGER NOT NULL,
#   lo_ordertotalprice   INTEGER NOT NULL,
#   lo_discount          INTEGER NOT NULL,
#   lo_revenue           INTEGER NOT NULL,
#   lo_supplycost        INTEGER NOT NULL,
#   lo_tax               INTEGER NOT NULL,
#   lo_commitdate        INTEGER NOT NULL,
#   lo_shipmode          VARCHAR(10) NOT NULL
# );

# Commented out IPython magic to ensure Python compatibility.
# # Same setup in different schema using KEY distribution
# # strategies
# 
# %%sql
# 
# CREATE SCHEMA IF NOT EXISTS dist;
# SET search_path TO dist;
# 
# DROP TABLE IF EXISTS part;
# DROP TABLE IF EXISTS supplier;
# DROP TABLE IF EXISTS customer;
# DROP TABLE IF EXISTS dwdate;
# DROP TABLE IF EXISTS lineorder;
# 
# CREATE TABLE part 
# (
#   p_partkey     INTEGER NOT NULL sortkey distkey,
#   p_name        VARCHAR(22) NOT NULL,
#   p_mfgr        VARCHAR(6) NOT NULL,
#   p_category    VARCHAR(7) NOT NULL,
#   p_brand1      VARCHAR(9) NOT NULL,
#   p_color       VARCHAR(11) NOT NULL,
#   p_type        VARCHAR(25) NOT NULL,
#   p_size        INTEGER NOT NULL,
#   p_container   VARCHAR(10) NOT NULL
# );
# 
# CREATE TABLE supplier 
# (
#   s_suppkey   INTEGER NOT NULL sortkey,
#   s_name      VARCHAR(25) NOT NULL,
#   s_address   VARCHAR(25) NOT NULL,
#   s_city      VARCHAR(10) NOT NULL,
#   s_nation    VARCHAR(15) NOT NULL,
#   s_region    VARCHAR(12) NOT NULL,
#   s_phone     VARCHAR(15) NOT NULL
# );
# 
# CREATE TABLE customer 
# (
#   c_custkey      INTEGER NOT NULL sortkey,
#   c_name         VARCHAR(25) NOT NULL,
#   c_address      VARCHAR(25) NOT NULL,
#   c_city         VARCHAR(10) NOT NULL,
#   c_nation       VARCHAR(15) NOT NULL,
#   c_region       VARCHAR(12) NOT NULL,
#   c_phone        VARCHAR(15) NOT NULL,
#   c_mktsegment   VARCHAR(10) NOT NULL
# );
# 
# CREATE TABLE dwdate 
# (
#   d_datekey            INTEGER NOT NULL sortkey,
#   d_date               VARCHAR(19) NOT NULL,
#   d_dayofweek          VARCHAR(10) NOT NULL,
#   d_month              VARCHAR(10) NOT NULL,
#   d_year               INTEGER NOT NULL,
#   d_yearmonthnum       INTEGER NOT NULL,
#   d_yearmonth          VARCHAR(8) NOT NULL,
#   d_daynuminweek       INTEGER NOT NULL,
#   d_daynuminmonth      INTEGER NOT NULL,
#   d_daynuminyear       INTEGER NOT NULL,
#   d_monthnuminyear     INTEGER NOT NULL,
#   d_weeknuminyear      INTEGER NOT NULL,
#   d_sellingseason      VARCHAR(13) NOT NULL,
#   d_lastdayinweekfl    VARCHAR(1) NOT NULL,
#   d_lastdayinmonthfl   VARCHAR(1) NOT NULL,
#   d_holidayfl          VARCHAR(1) NOT NULL,
#   d_weekdayfl          VARCHAR(1) NOT NULL
# );
# 
# CREATE TABLE lineorder 
# (
#   lo_orderkey          INTEGER NOT NULL,
#   lo_linenumber        INTEGER NOT NULL,
#   lo_custkey           INTEGER NOT NULL,
#   lo_partkey           INTEGER NOT NULL distkey,
#   lo_suppkey           INTEGER NOT NULL,
#   lo_orderdate         INTEGER NOT NULL sortkey,
#   lo_orderpriority     VARCHAR(15) NOT NULL,
#   lo_shippriority      VARCHAR(1) NOT NULL,
#   lo_quantity          INTEGER NOT NULL,
#   lo_extendedprice     INTEGER NOT NULL,
#   lo_ordertotalprice   INTEGER NOT NULL,
#   lo_discount          INTEGER NOT NULL,
#   lo_revenue           INTEGER NOT NULL,
#   lo_supplycost        INTEGER NOT NULL,
#   lo_tax               INTEGER NOT NULL,
#   lo_commitdate        INTEGER NOT NULL,
#   lo_shipmode          VARCHAR(10) NOT NULL
# );

# Commented out IPython magic to ensure Python compatibility.
def loadTables(schema, tables):
    loadTimes = []
    SQL_SET_SCHEMA = "SET search_path TO {};".format(schema)
#     %sql $SQL_SET_SCHEMA
    
    for table in tables:
        SQL_COPY= """
                  COPY {} FROM 's3://awssampledbuswest2/ssbgz/{}' 
                  CREDENTIALS 'aws_iam_role={}'
                  gzip REGION 'us-west-2';
                  """.format(table, table, DWH_ROLE_ARN)

        print("======= LOADING TABLE: ** {} ** IN SCHEMA ==> {} =======".format(table, schema))
        print(SQL_COPY)

        t0 = time()
#         %sql $SQL_COPY
        loadTime = time()-t0
        loadTimes.append(loadTime)

        print("=== DONE IN: {0:.2f} sec\n".format(loadTime))
    return pd.DataFrame({"table":tables, "loadtime_"+schema:loadTimes}).set_index('table')

tables = ["customer","dwdate","supplier", "part", "lineorder"]

# Insertion into each schema
nodistStats = loadTables("nodist", tables)
distStats = loadTables("dist", tables)

# Delete cluster
redshift.delete_cluster(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER, SkipFinalClusterSnapshot=True)

# Detach role policy and delete role
iam.detach_role_policy(RoleName=DWH_IAM_ROLE_NAME, PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
iam.delete_role(RoleName=DWH_IAM_ROLE_NAME)