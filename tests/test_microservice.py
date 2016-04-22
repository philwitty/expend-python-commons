from unittest.mock import Mock, patch
from ex_py_commons.sqs import AsyncQueue
from ex_py_commons.sns import AsyncPublish
from ex_py_commons.microservice import Microservice
import asynctest
from functools import partial
from ex_py_commons.test_lib.mocks import AsyncRequestsMock, TestHandler, \
                                         AsyncResponsesMock
import json


class TestService(asynctest.TestCase):
    def test_service_single_request(self):
        # given
        requests = Mock(wraps=AsyncRequestsMock([message('')]))
        requests_mock = async_mock(requests)
        responses = Mock(wraps=AsyncResponsesMock())
        responses_mock = async_mock(responses)
        session = Mock()
        handlers = []
        with patch.object(AsyncQueue, 'create',
                          new=requests_mock), \
            patch.object(AsyncPublish, 'create',
                         new=responses_mock):
            service = Microservice.run(self.loop, session, 'test-queue',
                                       handler_cls=partial(
                                           create_handler, handlers
                                       ),
                                       aws_session=session,
                                       run_forever=False)
            # when
            self.loop.run_until_complete(service)
        # then
        self.assertEqual(requests.receive_messages.call_count, 1)
        self.assertEqual(requests.delete_message.call_count, 1)
        self.assertEqual(responses.publish.call_count, 1)
        self.ensure_handlers_called(handlers)

    def test_service_multiple_request(self):
        # given
        requests = Mock(wraps=AsyncRequestsMock([message(''), message('')]))
        requests_mock = async_mock(requests)
        responses = Mock(wraps=AsyncResponsesMock())
        responses_mock = async_mock(responses)
        session = Mock()
        handlers = []
        with patch.object(AsyncQueue, 'create',
                          new=requests_mock), \
            patch.object(AsyncPublish, 'create',
                         new=responses_mock):
            service = Microservice.run(self.loop, session, 'test-queue',
                                       handler_cls=partial(
                                         create_handler, handlers
                                       ),
                                       aws_session=session,
                                       run_forever=False)
            # when
            self.loop.run_until_complete(service)
        # then
        self.assertEqual(requests.receive_messages.call_count, 1)
        self.assertEqual(requests.delete_message.call_count, 2)
        self.assertEqual(responses.publish.call_count, 2)
        self.ensure_handlers_called(handlers)

    def test_service_failure_request(self):
        # given
        requests = Mock(wraps=AsyncRequestsMock([message('fail'),
                                                 message('')]))
        requests_mock = async_mock(requests)
        responses = Mock(wraps=AsyncResponsesMock())
        responses_mock = async_mock(responses)
        session = Mock()
        handlers = []
        with patch.object(AsyncQueue, 'create',
                          new=requests_mock), \
            patch.object(AsyncPublish, 'create',
                         new=responses_mock):
            service = Microservice.run(self.loop, session, 'test-queue',
                                       handler_cls=partial(
                                         create_handler, handlers
                                       ),
                                       aws_session=session,
                                       run_forever=False)
            # when
            self.loop.run_until_complete(service)
        # then
        self.assertEqual(requests.receive_messages.call_count, 1)
        self.assertEqual(requests.delete_message.call_count, 1)
        self.assertEqual(responses.publish.call_count, 1)
        self.ensure_handlers_called(handlers[1:])

    def test_service_multiple_failure_request(self):
        # given
        requests = Mock(wraps=AsyncRequestsMock([message('fail'),
                                                 message('fail')]))
        requests_mock = async_mock(requests)
        responses = Mock(wraps=AsyncResponsesMock())
        responses_mock = async_mock(responses)
        session = Mock()
        handlers = []
        with patch.object(AsyncQueue, 'create',
                          new=requests_mock), \
            patch.object(AsyncPublish, 'create',
                         new=responses_mock):
            service = Microservice.run(self.loop, session, 'test-queue',
                                       handler_cls=partial(
                                         create_handler, handlers
                                       ),
                                       aws_session=session,
                                       run_forever=False)
            # when
            self.loop.run_until_complete(service)
        # then
        self.assertEqual(requests.receive_messages.call_count, 1)
        self.assertEqual(requests.delete_message.call_count, 0)
        self.assertEqual(responses.publish.call_count, 0)

    def ensure_handlers_called(self, handlers):
        for handler in handlers:
            self.assertEqual(handler.handle_request.call_count, 1)


def async_mock(mock):
    async def create(*args, **kwargs):
        return mock
    return create


def message(request):
    return json.dumps({
        'workflowExecutionUid': 'uid',
        'request': request,
        'responseTopic': 'test-response-topic',
    })


def create_handler(handlers):
    handler = Mock(wraps=TestHandler())
    handlers.append(handler)
    return handler
