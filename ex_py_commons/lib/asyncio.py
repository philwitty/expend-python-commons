from logging import getLogger


def log_exceptions_from_tasks(tasks, logger_name):
    logger = getLogger(logger_name)
    for task in tasks:
        e = task.exception()
        if e is not None:
            try:
                raise e
            except Exception:
                logger.exception("Caught exception in task")
