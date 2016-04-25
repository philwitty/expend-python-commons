from ex_py_commons.queue_reader import AsyncQueueReader
from ex_py_commons.sns import AsyncPublish
from ex_py_commons.lib.asyncio import log_exceptions_from_tasks
import asyncio
import json


def run_microservices(loop, session, microservices):
    tasks = [loop.create_task(microservice) for microservice in microservices]
    if tasks:
        loop.run_until_complete(asyncio.wait(tasks))
    log_exceptions_from_tasks(tasks, __name__)


class Microservice(AsyncQueueReader):
    def __init__(self, loop, requests, **kwargs):
        self._requests = requests
        self._loop = loop
        self._handler_cls = kwargs['handler_cls']
        self._session = kwargs['aws_session']

    async def _process_message(self, message):
        message_body = json.loads(message['Body'])
        request = message_body['request']
        handler = self._handler_cls()
        response = await handler.handle_request(request)
        message_body['response'] = response
        await self._publish_response(message_body)
        await self._requests.delete_message(message['ReceiptHandle'])

    async def _publish_response(self, message):
        topic = message['responseTopic']
        sns_publish = await AsyncPublish.create(self._session, topic)
        await sns_publish.publish(json.dumps(message))
