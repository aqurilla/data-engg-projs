# -*- coding: utf-8 -*-
"""
# Setting up IAM and Redshift cluster
"""

import pandas as pd
import boto3
import json
import configparser
import psycopg2

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

# Delete cluster
redshift.delete_cluster(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER, SkipFinalClusterSnapshot=True)

# Detach role policy and delete role
iam.detach_role_policy(RoleName=DWH_IAM_ROLE_NAME, PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
iam.delete_role(RoleName=DWH_IAM_ROLE_NAME)