from ex_py_commons.queue_reader import AsyncQueueReader
from ex_py_commons.lib.asyncio import log_exceptions_from_tasks
import asyncio


def run_services(loop, session, services):
    tasks = [
        loop.create_task(
            Service.run(loop, session, queue, handler_cls=handler)
        )
        for (queue, handler) in services
    ]
    if tasks:
        loop.run_until_complete(asyncio.wait(tasks))
    log_exceptions_from_tasks(tasks, __name__)


class Service(AsyncQueueReader):
    def __init__(self, loop, requests, **kwargs):
        self._requests = requests
        self._loop = loop
        self._handler_cls = kwargs['handler_cls']

    async def _process_message(self, message):
        handler = self._handler_cls()
        message_dict = await handler.handle_request(message)
        action_input = await handler.build_action_input(message_dict)
        response = await handler.perform_action(action_input)
        response_dict = await handler.parse_action_response(response)
        await handler.handle_response(response_dict)
        await self._requests.delete_message(message['ReceiptHandle'])
