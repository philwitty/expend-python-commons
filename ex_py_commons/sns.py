from ex_py_commons import session


class Publish:
    def __init__(self, topic_name, aws_session=None):
        if aws_session is None:
            aws_session = session.boto_session()
        self.client = session.client('sns', region_name='eu-west-1')
        response = self.client.create_topic(Name=topic_name)
        self.topic_arn = response['TopicArn']

    def publish(self, message_body):
        self.client.publish(TopicArn=self.topic_arn,
                            Message=message_body)


class EndpointPush:
    def __init__(self, application_arn, aws_session=None):
        if aws_session is None:
            aws_session = session.boto_session()
        self.client = session.client('sns', region_name='eu-west-1')
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
