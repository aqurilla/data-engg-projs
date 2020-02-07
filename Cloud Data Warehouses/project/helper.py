import pandas as pd
import boto3
import json
import configparser
import psycopg2
import time


def config_parser():
    '''
    Parses config file
    '''
    
    global CFG_FILENAME, KEY, SECRET, REGION_NAME, DWH_CLUSTER_TYPE, \
        DWH_NUM_NODES, DWH_NODE_TYPE, DWH_CLUSTER_IDENTIFIER, DWH_DB, \
        DWH_DB_USER, DWH_DB_PASSWORD, DWH_PORT, DWH_IAM_ROLE_NAME
    
    CFG_FILENAME = "dwh.cfg"
    
    config = configparser.ConfigParser()
    config.read_file(open(CFG_FILENAME))
    
    # Read config options from file
    KEY = config.get('AWS', 'KEY')
    SECRET = config.get('AWS', 'SECRET')
    REGION_NAME = config.get('AWS', 'REGION_NAME')

    DWH_CLUSTER_TYPE = config.get("DWH","DWH_CLUSTER_TYPE")
    DWH_NUM_NODES = config.get("DWH","DWH_NUM_NODES")
    DWH_NODE_TYPE = config.get("DWH","DWH_NODE_TYPE")

    DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")
    DWH_DB = config.get("DWH","DWH_DB")
    DWH_DB_USER= config.get("DWH","DWH_DB_USER")
    DWH_DB_PASSWORD = config.get("DWH","DWH_DB_PASSWORD")
    DWH_PORT = config.get("DWH","DWH_PORT")

    DWH_IAM_ROLE_NAME = config.get("DWH", "DWH_IAM_ROLE_NAME")
    

def create_client(service):
    '''
    Returns AWS client
    '''
    global KEY, SECRET, REGION_NAME
    
    client = boto3.client(service,
                         region_name = REGION_NAME,
                         aws_access_key_id=KEY,
                         aws_secret_access_key=SECRET
                        )
    
    return client


def create_resource(service):
    '''
    Returns AWS resource
    '''
    global KEY, SECRET, REGION_NAME
    
    resource = boto3.resource(service,
                         region_name = REGION_NAME,
                         aws_access_key_id=KEY,
                         aws_secret_access_key=SECRET
                        )
    
    return resource


def create_iam_role(iam):
    '''
    Creates IAM role and returns role ARN
    '''
    
    global DWH_IAM_ROLE_NAME
    
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
    )
    
    # Get the role ARN
    roleArn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']
    
    return roleArn


def create_cluster(redshift, roleArn):
    '''
    Create the Redshift cluster
    '''
    
    global DWH_DB, DWH_CLUSTER_IDENTIFIER, DWH_CLUSTER_TYPE, \
        DWH_NUM_NODES, DWH_NODE_TYPE, DWH_DB_USER, \
        DWH_DB_PASSWORD
    
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
        return response['ResponseMetadata']['HTTPStatusCode']==200
    except Exception as e:
        print(e)

        
def check_cluster_status(redshift):
    
    global DWH_CLUSTER_IDENTIFIER, DWH_ENDPOINT, DWH_ROLE_ARN
    
    clusterprops = redshift.describe_clusters(
        ClusterIdentifier=DWH_CLUSTER_IDENTIFIER
    )['Clusters'][0]
    
    if clusterprops['ClusterStatus'].lower() == 'available':
        print("Cluster is now available")
        
        # Update config file
        update_config(clusterprops)
        
        return True
    else:
        print("Cluster still provisioning...")
    
    return False


def update_config(clusterprops):
    '''
    Update role ARN and cluster endpoint 
    in config file
    '''
    
    global DWH_ENDPOINT, DWH_ROLE_ARN
    
    config = configparser.ConfigParser()
    config.read_file(open(CFG_FILENAME))
    
    DWH_ENDPOINT = clusterprops['Endpoint']['Address']
    DWH_ROLE_ARN = clusterprops['IamRoles'][0]['IamRoleArn']
    
    config['IAM_ROLE']['ARN'] = DWH_ROLE_ARN
    config['DWH']['DWH_ENDPOINT'] = DWH_ENDPOINT

    with open('dwh.cfg', 'w') as configfile:
        config.write(configfile)
        
    print('Updated dwh.cfg')

    
def enable_redshift_access(ec2, redshift):
    '''
    Enable access to Redshift
    '''
    global DWH_PORT, DWH_CLUSTER_IDENTIFIER
    
    clusterprops = redshift.describe_clusters(
        ClusterIdentifier=DWH_CLUSTER_IDENTIFIER
    )['Clusters'][0]
    
    try:
        vpc = ec2.Vpc(id=clusterprops['VpcId'])
        defaultSg = list(vpc.security_groups.all())[0]
        defaultSg.authorize_ingress(
          GroupName=defaultSg.group_name,
          CidrIp='0.0.0.0/0',
          IpProtocol='TCP',
          FromPort=int(DWH_PORT),
          ToPort=int(DWH_PORT)
        )
    except Exception as e:
        print(e)
    
def connect_redshift():
    '''
    Connect to Redshift
    '''
    global DWH_ENDPOINT, DWH_DB, DWH_DB_USER, \
    DWH_DB_PASSWORD, DWH_PORT
    
    conn = psycopg2.connect(f'host={DWH_ENDPOINT} \
                            dbname={DWH_DB} \
                            user={DWH_DB_USER} \
                            password={DWH_DB_PASSWORD} \
                            port={DWH_PORT}')

    cur = conn.cursor()

    conn.set_session(autocommit=True)
    
    print('Connected')