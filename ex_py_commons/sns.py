import boto3

class Publish:
    def __init__(self, topic_name):
        self.client =  boto3.client('sns', region_name='eu-west-1')
        response = self.client.create_topic(Name=topic_name)
        self.topic_arn = response['TopicArn']

    def publish(self, message_body):
        self.client.publish(TopicArn=self.topic_arn,
                            Message=message_body)
