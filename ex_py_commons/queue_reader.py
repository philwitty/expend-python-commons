import logging
from ex_py_commons.sqs import AsyncQueue
import asyncio
from ex_py_commons.lib.asyncio import log_exceptions_from_tasks

logger = logging.getLogger(__name__)


class AsyncQueueReader:
    def __init__(self, loop, requests):
        self._requests = requests
        self._loop = loop

    @classmethod
    async def run(cls, loop, session, queue_name, **kwargs):
        logger.info('Creating queue: {}'.format(queue_name))
        requests = await AsyncQueue.create(session, queue_name)
        logger.info('Created queue: {}'.format(queue_name))
        service = cls(loop, requests, **kwargs)
        run_forever = kwargs.get('run_forever', True)
        while True:
            try:
                await service._receive_messages()
            except:
                error_msg = 'Exception receiving message for {}'.format(
                    queue_name
                )
                logger.exception(error_msg)
            if not run_forever:
                break

    async def _process_message(self, message):
        await self._requests.delete_message(message['ReceiptHandle'])

    async def _receive_messages(self):
        messages = await self._requests.receive_messages(num_messages=10)
        tasks = [
            self._loop.create_task(self._process_message(message))
            for message in messages
        ]
        if tasks:
            await asyncio.wait(tasks)
        log_exceptions_from_tasks(tasks, __name__)
