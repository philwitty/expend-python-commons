import logging
from ex_py_commons.sqs import AsyncQueue
import asyncio

logger = logging.getLogger(__name__)


def run_services(loop, session, services):
    tasks = [
        loop.create_task(Service.run(loop, session, queue, handler))
        for (queue, handler) in services
    ]
    if tasks:
        loop.run_until_complete(asyncio.wait(tasks))
    log_exceptions_from_tasks(tasks)


class Service:
    def __init__(self, loop, requests, handler_cls):
        self._requests = requests
        self._handler_cls = handler_cls
        self._loop = loop

    @staticmethod
    async def run(loop, session, queue_name, handler_cls, run_forever=True):
        logger.info('Creating queue: {}'.format(queue_name))
        requests = await AsyncQueue.create(session, queue_name)
        logger.info('Created queue: {}'.format(queue_name))
        service = Service(loop, requests, handler_cls)
        while True:
            try:
                await service._receive_messages()
            except:
                error_msg = 'Exception in service for {}'.format(queue_name)
                logger.exception(error_msg)
            if not run_forever:
                break

    async def _process_message(self, message):
        handler = self._handler_cls()
        message_dict = await handler.handle_request(message)
        action_input = await handler.build_action_input(message_dict)
        response = await handler.perform_action(action_input)
        response_dict = await handler.parse_action_response(response)
        await handler.handle_response(response_dict)
        await self._requests.delete_message(message['ReceiptHandle'])

    async def _receive_messages(self):
        messages = await self._requests.receive_messages(num_messages=10)
        tasks = [
            self._loop.create_task(self._process_message(message))
            for message in messages
        ]
        if tasks:
            await asyncio.wait(tasks)
        log_exceptions_from_tasks(tasks)


def log_exceptions_from_tasks(tasks):
    for task in tasks:
        e = task.exception()
        if e is not None:
            try:
                raise e
            except Exception:
                logger.exception("Exception in Task")
