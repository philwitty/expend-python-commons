class Queue:
    def __init__(self, session, queue_name):
        client = session.resource('sqs', region_name='eu-west-1')
        self.queue = client.create_queue(QueueName=queue_name)

    def receive_messages(self, num_messages=1):
        return self.queue.receive_messages(MaxNumberOfMessages=num_messages,
                                           WaitTimeSeconds=20)

    def send_message(self, message_body):
        self.queue.send_message(MessageBody=message_body)

    def delete_message(self, message_handle):
        self.queue.delete_messages(
            Entries=[
                {
                    'Id': 'MessageToDelete',
                    'ReceiptHandle': message_handle
                },
            ]
        )


class AsyncQueue:
    def __init__(self, client, queue_url):
        self.client = client
        self.queue_url = queue_url

    @staticmethod
    async def create(session, queue_name):
        client = session.create_client('sqs', region_name='eu-west-1')
        queue_url = \
            await client.create_queue(QueueName=queue_name)['QueueUrl']
        return AsyncQueue(client, queue_url)

    async def receive_messages(self, num_messages=1):
        messages = \
            await self.client.receive_message(QueueUrl=self.queue_url,
                                              MaxNumberOfMessages=num_messages,
                                              WaitTimeSeconds=20)
        return messages.get('Messages', [])

    async def send_message(self, message_body):
        await self.client.send_message(QueueUrl=self.queue_url,
                                       MessageBody=message_body)

    async def delete_message(self, message_handle):
        await self.client.delete_message(QueueUrl=self.queue_url,
                                         ReceiptHandle=message_handle)
