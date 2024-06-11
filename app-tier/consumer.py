import base64
import os
import subprocess

import boto3
from botocore.exceptions import ClientError
from config import (IMAGES_BUCKET_NAME, LABELS_BUCKET_NAME, REGION,
                    REQUEST_QUEUE_NAME, RESPONSE_QUEUE_NAME)

os.environ['AWS_DEFAULT_REGION'] = REGION

class Consumer:
    def __init__(self):
        self.sqs = boto3.client('sqs')
        self.s3 = boto3.client('s3')
        self.setup()

    def setup(self):
        self.request_queue_url = self._get_queue_url(REQUEST_QUEUE_NAME)
        print("Request Queue", self.request_queue_url)
        self.response_queue_url = self._get_queue_url(RESPONSE_QUEUE_NAME)
        print("Response Queue", self.response_queue_url)
    
    def _get_queue_url(self, queue_name):
        try:
            resp = self.sqs.get_queue_url(QueueName=queue_name)
            return resp['QueueUrl']
        except ClientError as e:
            if e.response['Error']['Code'] == 'AWS.SimpleQueueService.NonExistentQueue':
                print(f"Queue {queue_name} not found. Is app tier running?")
            raise e

    def _put_img_s3(self, image, filename):
        resp = self.s3.put_object(
            Body=image,
            Bucket=IMAGES_BUCKET_NAME,
            Key=filename
        )
    
    def _put_result_s3(self, result):
        image_name = result.split(',')[0]
        resp = self.s3.put_object(
            Body=result,
            Bucket=LABELS_BUCKET_NAME,
            Key=image_name
        )
    
    def _process_image(self, image, filename):
        filepath = f'data/{filename}'
        image_name = filename.split('.')[0]
        with open(filepath, 'wb') as f:
            f.write(image)
        command = ['python3', 'image_classification.py', filepath]
        try:
            result = subprocess.run(command, capture_output=True)

            # Print the return code
            print(f"Return Code: {result.returncode}")
            if result.returncode != 0:
                raise Exception("Error processing image")
            output = result.stdout.decode('utf-8')
            label = output.split(',')[1].strip()
            print("Output:", label)
            return f"{image_name},{label}"
        except Exception as e:
            print("Error processing image")
            raise e
    
    def consume(self):
        while True:
            msgs = self.sqs.receive_message(
                QueueUrl = self.request_queue_url,
                AttributeNames = ['All'],
                MessageAttributeNames = ['All'],
                MaxNumberOfMessages = 1,
                WaitTimeSeconds = 10
            )
            msgs = msgs.get('Messages', [])
            for msg in msgs:
                print(f"Processing message {msg['MessageId']}")
                try:
                    body = msg['Body']
                    image = base64.b64decode(body)
                    filename = msg['MessageAttributes']['filename']['StringValue']
                    self._put_img_s3(image, filename)
                    result = self._process_image(image, filename)
                    self._put_result_s3(result)
                    self.produce(result, msg['MessageId'])
                except Exception as e:
                    print("Error processing message")
                    print(e)
                    continue
                finally:
                    print(f"Processing Finished. Deleting message {msg['MessageId']}")
                    self.sqs.delete_message(
                        QueueUrl = self.request_queue_url,
                        ReceiptHandle = msg['ReceiptHandle']
                    )
    
    def produce(self, result, messageId):
        self.sqs.send_message(
            QueueUrl = self.response_queue_url,
            MessageBody = result,
            MessageAttributes = {
                'OriginMessage': {
                    'StringValue': messageId,
                    'DataType': 'String'
                }
            }
        )

if __name__ == '__main__':
    consumer = Consumer()
    consumer.consume()