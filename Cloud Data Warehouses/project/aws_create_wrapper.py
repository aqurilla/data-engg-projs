from helper import *    

def main():
    
    # Parse the config file
    print('* Parsing config file')
    config_parser()
    
    # Create EC2, S3, IAM and Redshift resources/clients
    print('* Creating AWS resources')
    ec2 = create_resource('ec2')
    s3 = create_resource('s3')
    iam = create_client('iam')
    redshift = create_client('redshift')
    
    # Create role to allow Redshift to access S3
    print('* Creating role')
    roleArn = create_iam_role(iam)
    
    # Create redshift cluster
    print('* Creating Redshift cluster')
    create_cluster(redshift, roleArn)
    
    # Check cluster availability status
    # Takes ~5 min
    print('* Checking cluster availability')
    while not check_cluster_status(redshift):
        time.sleep(60)
    
    print('* Enabling Redshift access')
    enable_redshift_access(ec2, redshift)
    
    print('* Connecting to Redshift')
    connect_redshift()
    
    
if __name__ == "__main__":
    main()