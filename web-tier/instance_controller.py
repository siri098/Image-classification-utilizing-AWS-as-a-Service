from datetime import datetime

import boto3
import pytz

utc = pytz.UTC

RUN_SCRIPT = """#cloud-boothook
#!/bin/bash
cd /home/ubuntu/app-tier && sudo --user ubuntu python3 consumer.py"""

class InstanceController:
    AMI = "ami-0083df0691841575e"
    INSTANCE_TYPE = "t2.micro"
    IAM_INSTANCE_PROFILE = "ec2-admin"
    MAX_COUNT = 20
    
    def __init__(self) -> None:
        self.ec2 = boto3.resource('ec2')
        self.setUp()

    def setUp(self):
        self._generate_instance_names()
        self._get_instances()
        self.scale_to_count(1)

    def _generate_instance_names(self, count: int = MAX_COUNT) -> list:
        self.instances = {f"app-instance-{i}": None for i in range(1, count+1)}
    
    def _get_instance_ec2(self, name) -> object:
        instances = list(self.ec2.instances.filter(
            Filters=[{'Name': 'tag:Name', 'Values': [name]}, 
                     {'Name': 'instance-state-name', 'Values': ['running']}]))
        if len(instances) == 0:
            return None
        return instances[0]
    
    def _get_instances(self) -> list:
        for name in self.instances:
            self.instances[name] = self._get_instance_ec2(name)
    
    def _get_first_available_instance_name(self) -> str:
        live = self.get_live_instances()
        for name in self.instances:
            if name not in live:
                return name
        return None

    def _get_latest_instance_name(self) -> str:
        live = self.get_live_instances()
        latest = None
        launch = datetime.min.replace(tzinfo=utc)
        for name in live:
            instance = self.instances[name]
            if instance is not None and instance.state['Name'] == 'pending':
                return name
            if launch is None and instance is not None:
                launch = instance.launch_time
                latest = name
                continue
            if instance is not None and instance.launch_time > launch:
                launch = instance.launch_time
                latest = name
        return latest
    
    def create_instance(self):
        name = self._get_first_available_instance_name()
        if name is None:
            print("MAXIMUM NUMBER OF INSTANCES REACHED")
            return
        print(f"Creating instance {name}")
        self.instances[name] = self.ec2.create_instances(
            ImageId=self.AMI,
            InstanceType=self.INSTANCE_TYPE,
            MinCount=1,
            MaxCount=1,
            IamInstanceProfile={
                'Name': self.IAM_INSTANCE_PROFILE
            },
            InstanceInitiatedShutdownBehavior='terminate',
            UserData=RUN_SCRIPT,
            TagSpecifications=[
                {
                    'ResourceType': 'instance',
                    'Tags': [
                        {
                            'Key': 'Name',
                            'Value': name
                        },
                    ]
                },
            ]
        )[0]

    def terminate_instance(self):
        name = self._get_latest_instance_name()
        print(f"Terminating instance {name}")
        self.instances[name].terminate()
        self.instances[name] = None

    def get_live_instances(self) -> list:
        res = []
        for name, instance in self.instances.items():
            if instance is not None and instance.state['Name'] in ['pending', 'running']:
                res.append(name)
            else:
                self.instances[name] = None
        return res

    def scale_to_count(self, count: int):
        if count < 1 or count > self.MAX_COUNT:
            print(f"Invalid count {count}. Must be between 1 and {self.MAX_COUNT}")
            return
        while True:
            current_count = sum([1 for instance in self.instances.values() if instance is not None])
            print(f"Instance count: {current_count}")
            if current_count < count:
                try:
                    self.create_instance()
                except Exception as e:
                    break
            elif current_count > count:
                self.terminate_instance()
            else:
                break
