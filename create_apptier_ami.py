import boto3

ec2 = boto3.client('ec2')

instance_id = "i-0ee0bfa7674671b7d"

response = ec2.create_image(
    InstanceId=instance_id,
    Name='app-tier-cse546-1s-and-0s',  # Provide a name for the new AMI
    Description='Modified app-tier AMI for CSE-546 Project-1 by [Ones and Zeros]',  # Provide a description
    NoReboot=True  # Specify whether the instance should be rebooted when creating the AMI
)

ami_id = response['ImageId']
print(f"AMI ID: {ami_id}")