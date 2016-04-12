from unittest.mock import Mock, patch
from ex_py_commons.sqs import AsyncQueue
from ex_py_commons.sqs_service import Service
import asynctest
from functools import partial


class TestService(asynctest.TestCase):
    def test_service_single_request(self):
        # given
        requests = Mock(wraps=RequestsMock(['']))
        create_mock = create_requests_mock(requests)
        session = Mock()
        handlers = []
        with patch.object(AsyncQueue, 'create', new=create_mock):
            service = Service.run(self.loop, session, 'test-queue',
                                  handler_cls=partial(
                                    create_handler, handlers
                                  ),
                                  run_forever=False)
            # when
            self.loop.run_until_complete(service)
        # then
        self.assertEqual(requests.receive_messages.call_count, 1)
        self.assertEqual(requests.delete_message.call_count, 1)
        self.ensure_handlers_called(handlers)

    def test_service_multiple_request(self):
        # given
        requests = Mock(wraps=RequestsMock(['', '']))
        create_mock = create_requests_mock(requests)
        session = Mock()
        handlers = []
        with patch.object(AsyncQueue, 'create', new=create_mock):
            service = Service.run(self.loop, session, 'test-queue',
                                  handler_cls=partial(
                                    create_handler, handlers
                                  ),
                                  run_forever=False)
            # when
            self.loop.run_until_complete(service)
        # then
        self.assertEqual(requests.receive_messages.call_count, 1)
        self.assertEqual(requests.delete_message.call_count, 2)
        self.ensure_handlers_called(handlers)

    def test_service_failure_request(self):
        # given
        requests = Mock(wraps=RequestsMock(['fail', '']))
        create_mock = create_requests_mock(requests)
        session = Mock()
        handlers = []
        with patch.object(AsyncQueue, 'create', new=create_mock):
            service = Service.run(self.loop, session, 'test-queue',
                                  handler_cls=partial(
                                    create_handler, handlers
                                  ),
                                  run_forever=False)
            # when
            self.loop.run_until_complete(service)
        # then
        self.assertEqual(requests.receive_messages.call_count, 1)
        self.assertEqual(requests.delete_message.call_count, 1)
        self.ensure_handlers_called(handlers[1:])

    def test_service_multiple_failure_request(self):
        # given
        requests = Mock(wraps=RequestsMock(['fail', 'fail']))
        create_mock = create_requests_mock(requests)
        session = Mock()
        handlers = []
        with patch.object(AsyncQueue, 'create', new=create_mock):
            service = Service.run(self.loop, session, 'test-queue',
                                  handler_cls=partial(
                                    create_handler, handlers
                                  ),
                                  run_forever=False)
            # when
            self.loop.run_until_complete(service)
        # then
        self.assertEqual(requests.receive_messages.call_count, 1)
        self.assertEqual(requests.delete_message.call_count, 0)

    def ensure_handlers_called(self, handlers):
        for handler in handlers:
            self.assertEqual(handler.handle_request.call_count, 1)
            self.assertEqual(handler.build_action_input.call_count, 1)
            self.assertEqual(handler.perform_action.call_count, 1)
            self.assertEqual(handler.parse_action_response.call_count, 1)
            self.assertEqual(handler.handle_response.call_count, 1)


def create_requests_mock(requests):
    async def create(*args, **kwargs):
        return requests
    return create


def create_handler(handlers):
    handler = Mock(wraps=TestHandler())
    handlers.append(handler)
    return handler


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
