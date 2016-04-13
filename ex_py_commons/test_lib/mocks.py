from ex_py_commons.sqs import AsyncQueue


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


class RequestsMock(AsyncQueue):

    def __init__(self, messages):
        self.messages = [
            {'ReceiptHandle': i, 'Body': message}
            for i, message in enumerate(messages)
        ]

    async def receive_messages(self, num_messages=10):
        return list(self.messages)

    async def delete_message(self, handle):
        [m for m in self.messages if m['ReceiptHandle'] != handle]
