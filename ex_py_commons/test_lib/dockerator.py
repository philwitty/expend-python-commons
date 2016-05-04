from functools import wraps
from docker import Client
import psycopg2
import os
from requests.exceptions import HTTPError
from subprocess import check_output
from docker.utils import kwargs_from_env
from ex_py_commons.session import boto_session
from botocore.exceptions import ClientError

client = Client(**kwargs_from_env())


def dockerator(image,
               command=None,
               ports=[],
               wait_for=None):
    def _my_decorator(func):
        @wraps(func)
        def _decorator(*args, **kwargs):
            try:
                client.pull(image)
            except HTTPError as e:
                if e.response.status_code == 404:
                    print(('Couldn\'t pull {}, using local'
                           ' image (if available).').format(image))
                else:
                    raise
            default_bindings = {}
            for port in ports:
                default_bindings[port] = port
            create_config_kwargs = {
                'port_bindings': default_bindings
            }
            # If running inside a docker container then run inside the
            # container rather than exposing ports
            if os.path.exists('/.dockerenv'):
                container_id = check_output(
                    ('cat /proc/self/cgroup | grep \'docker\' '
                     '| sed \'s/^.*\///\' | tail -n1'),
                    shell=True
                ).decode().strip()
                create_config_kwargs = {
                    'network_mode': 'container:' + container_id
                }
                create_container_kwargs = {
                    'image': image,
                    'command': command,
                }
            else:
                create_config_kwargs = {
                    'port_bindings': default_bindings
                }
                create_container_kwargs = {
                    'image': image,
                    'command': command,
                    'ports': ports,
                }

            default_host_config = client.create_host_config(
                **create_config_kwargs
            )
            create_container_kwargs['host_config'] = default_host_config
            # Create the container
            container = client.create_container(**create_container_kwargs)
            # Start the container
            client.start(container=container)
            # Our container is running, execute the function
            try:
                # If a wait_for callable was passed in execute it,
                # it should wait until the necessary service is running
                if wait_for:
                    wait_for()
                ret = func(*args, **kwargs)
            finally:
                client.stop(container)
                client.wait(container)
                client.remove_container(container)
            return ret
        return _decorator
    return _my_decorator


def wait_for_postgresql(host='dev.docker', user='postgres'):
    def wait_for():
        while True:
            try:
                psycopg2.connect(host=host, user=user)
                return
            except psycopg2.OperationalError as e:
                acceptable_errors = [
                    'could not connect to server',
                    'server closed the connection unexpectedly',
                    'the database system is starting up'
                ]
                if not any(msg in str(e) for msg in acceptable_errors):
                    raise
    return wait_for


def wait_for_dynamodb(host='dev.docker', session=boto_session()):
    def wait_for():
        dynamodb = session.client(
            'dynamodb', endpoint_url='http://' + host + ':8000'
        )
        while True:
            try:
                dynamodb.list_tables()
                return
            except ClientError:
                pass
    return wait_for
