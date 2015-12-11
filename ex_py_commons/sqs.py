from ex_py_commons import session


class Queue:
    def __init__(self, queue_name, aws_session=None):
        if session is None:
            aws_session = session.boto_session()
        sqs = aws_session.resource('sqs', region_name='eu-west-1')
        self.queue = sqs.get_queue_by_name(QueueName=queue_name)

    def receive_message(self):
        """
        Will wait for up to 20 seconds to receive message (Max limit)
        Returns tuple of (message_handle, message_body)
        """
        messages = self.queue.receive_messages(MaxNumberOfMessages=1,
                                               WaitTimeSeconds=20)
        if len(messages) != 1:
            return None
        else:
            message = messages[0]
            return (message.receipt_handle, message.body)

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
