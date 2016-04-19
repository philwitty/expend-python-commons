from .session import boto_session


class Publish:
    def __init__(self, session, topic_name):
        self.client = session.client('sns', region_name='eu-west-1')
        response = self.client.create_topic(Name=topic_name)
        self.topic_arn = response['TopicArn']

    def publish(self, message_body):
        self.client.publish(TopicArn=self.topic_arn,
                            Message=message_body)


class AsyncPublish:
    def __init__(self, client, topic_arn):
        self.client = client
        self.topic_arn = topic_arn

    @staticmethod
    async def create(session, topic_name):
        client = session.create_client('sns', region_name='eu-west-1')
        topic = await client.create_topic(Name=topic_name)
        return AsyncPublish(client, topic['TopicArn'])

    async def publish(self, message_body):
        await self.client.publish(TopicArn=self.topic_arn,
                                  Message=message_body)


class EndpointPush:
    def __init__(self, application_arn, aws_session=boto_session()):
        self.client = aws_session.client('sns', region_name='eu-west-1')
        self.application = application_arn

    def push_to_endpoint(self, endpoint_token, message_body):
        response = self.client.create_platform_endpoint(
            PlatformApplicationArn=self.application,
            Token=endpoint_token,
        )
        self.client.publish(TargetArn=response['EndpointArn'],
                            Message=message_body)

    def delete_endpoint(self, endpoint_token):
        response = self.client.create_platform_endpoint(
            PlatformApplicationArn=self.application,
            Token=endpoint_token,
        )
        self.client.delete_endpoint(
            EndpointArn=response['EndpointArn']
        )
