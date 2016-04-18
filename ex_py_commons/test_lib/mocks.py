from ex_py_commons.sqs import AsyncQueue
from ex_py_commons.sns import AsyncPublish


class TestHandler():
    async def handle_request(self, message):
        if message['Body'] == 'fail':
            raise Exception
        return {}

    async def build_action_input(self, message_dict):
        pass

    async def perform_action(self, action_input):
        pass

    async def parse_action_response(self, response):
        return response

    async def handle_response(self, response_dict):
        pass


class AsyncRequestsMock(AsyncQueue):

    def __init__(self, messages):
        self.messages = []
        self.send_messages(messages)

    def send_messages(self, messages):
        self.messages.extend([
            {'ReceiptHandle': i, 'Body': message}
            for i, message in enumerate(messages)
        ])

    async def receive_messages(self, num_messages=10):
        return list(self.messages)

    async def delete_message(self, handle):
        self.messages = [
            m for m in self.messages
            if m['ReceiptHandle'] != handle
        ]


class AsyncResponsesMock(AsyncPublish):

    def __init__(self, messages):
        self.messages = messages

    async def publish(self, message_body):
        self.messages.append(message_body)
