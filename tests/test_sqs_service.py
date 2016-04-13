from unittest.mock import Mock, patch
from ex_py_commons.sqs import AsyncQueue
from ex_py_commons.sqs_service import Service
import asynctest
from functools import partial
from ex_py_commons.test_lib.mocks import RequestsMock, TestHandler


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
