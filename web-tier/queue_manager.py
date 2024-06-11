import base64
import os

import boto3
from botocore.exceptions import ClientError
from gevent import joinall, sleep, spawn
from instance_controller import InstanceController
from response_handler import ResponseHandler

REQUEST_QUEUE_NAME = 'cse546-1-requests'
RESPONSE_QUEUE_NAME = 'cse546-1-responses'
IMAGES_BUCKET_NAME = 'cse546-1-images'
LABELS_BUCKET_NAME = 'cse546-1-labels'
REGION = 'us-east-1'

os.environ['AWS_DEFAULT_REGION'] = REGION

class SQSQueueManager:
    def __init__(self) -> None:
        self.sqs = boto3.client('sqs')
        self.s3 = boto3.client('s3')
        self.setup()
        self.running = True
        self.consumer_greenlet = spawn(self.consume)
        self.response_handler = ResponseHandler()
        self.instance_controller = InstanceController()
        self.monitor_greenlet = spawn(self.monitor)

    def __del__(self) -> None:
        self.running = False
        joinall([self.consumer_greenlet, self.monitor_greenlet])

    def setup(self) -> None:
        '''
            This sets up the request queue and response queue if they do not exist.
        '''
        try:
            self.request_queue_url = self._setup_queue(REQUEST_QUEUE_NAME)
            self.response_queue_url = self._setup_queue(RESPONSE_QUEUE_NAME)
            self._setup_s3_bucket(IMAGES_BUCKET_NAME)
            self._setup_s3_bucket(LABELS_BUCKET_NAME)
        except Exception as e:
            print("Error setting up Request and Response queues and S3 buckets.")
            raise e
        
    def _setup_queue(self, queue_name: str) -> None:
        try:
            resp = self.sqs.get_queue_url(QueueName=queue_name)
            print(f"Queue {queue_name} already exists. Skipping creation.")
        except ClientError as e:
            if e.response['Error']['Code'] == 'AWS.SimpleQueueService.NonExistentQueue':
                print(f"Creating queue {queue_name}")
                resp = self.sqs.create_queue(
                    QueueName=queue_name
                )
                print(f"Queue {queue_name} created successfully.")
            else:
                raise e
        return resp['QueueUrl']
    
    def _setup_s3_bucket(self, bucket_name: str) -> None:
        try:
            resp = self.s3.head_bucket(Bucket=bucket_name)
            print(f"Bucket {bucket_name} already exists. Skipping creation.")
        except ClientError as e:
            print(e.response['Error']['Code'])
            if e.response['Error']['Code'] in ['404', '403']:
                print(f"Creating bucket {bucket_name}")
                self.s3.create_bucket(
                    Bucket=bucket_name
                )
                print(f"Bucket {bucket_name} created successfully.")
    
    def handle_request(self, image, filename) -> None:
        image_body = base64.b64encode(image.read()).decode('utf-8')
        resp = self.sqs.send_message(
            QueueUrl=self.request_queue_url,
            MessageBody=image_body,
            MessageAttributes={
                'filename': {
                    'StringValue': filename,
                    'DataType': 'String'
                }
            }
        )
        self.response_handler.add_response(resp['MessageId'])
        return resp['MessageId']
    
    def get_result(self, messageId: str) -> dict:
        result = self.response_handler.get_response(messageId)
        if result is None:
            return None
        print("Result:", result)
        return result.split(',')[1].strip()
    
    def consume(self, waittime=1) -> None:
        while self.running:
            resp = self.sqs.receive_message(
                QueueUrl=self.response_queue_url,
                MessageAttributeNames=['OriginMessage'],
                MaxNumberOfMessages=10,
                WaitTimeSeconds=20
            )
            messages = resp.get('Messages', [])
            if len(messages) == 0:
                sleep(waittime)
            for message in messages:
                origin_message_id = message['MessageAttributes']['OriginMessage']['StringValue']
                print(f"Received response for {origin_message_id}: {message['Body']}")
                self.response_handler.add_response(origin_message_id, message['Body'])
                self.sqs.delete_message(
                    QueueUrl=self.response_queue_url,
                    ReceiptHandle=message['ReceiptHandle']
                )
    
    def _get_request_queue_size(self) -> int:
        resp = self.sqs.get_queue_attributes(
            QueueUrl=self.request_queue_url,
            AttributeNames=['ApproximateNumberOfMessages']
        )
        return int(resp['Attributes']['ApproximateNumberOfMessages'])

    def monitor(self):
        while self.running:
            current_queue_size = self._get_request_queue_size()
            print(f"Current queue size: {current_queue_size}")
            if current_queue_size == 0:
                self.instance_controller.scale_to_count(1)
            elif current_queue_size < InstanceController.MAX_COUNT:
                self.instance_controller.scale_to_count(current_queue_size)
            else:
                self.instance_controller.scale_to_count(InstanceController.MAX_COUNT)
            sleep(10)
        