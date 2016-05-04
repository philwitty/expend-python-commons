from ex_py_commons.test_lib.dockerator import dockerator, wait_for_dynamodb, \
                                              wait_for_postgresql
from unittest import TestCase
import psycopg2
from ex_py_commons.session import boto_session


class TestDockerator(TestCase):
    @dockerator(image='postgres:9.5',
                ports=[5432],
                wait_for=wait_for_postgresql())
    def test_dockerator_postgresql(self):
        conn = psycopg2.connect(host='dev.docker', user='postgres')
        cursor = conn.cursor()
        cursor.execute('SELECT 1')

    @dockerator(image='curoo/aws-dynamodb-local:latest',
                ports=[8000],
                wait_for=wait_for_dynamodb())
    def test_dockerator_dyanamodb(self):
        session = boto_session()
        client = session.client('dynamodb',
                                endpoint_url='http://dev.docker:8000')
        client.create_table(
            AttributeDefinitions=[
                {
                    'AttributeName': 'example_key',
                    'AttributeType': 'S'
                },
            ],
            TableName='example_table',
            KeySchema=[
                {
                    'AttributeName': 'example_key',
                    'KeyType': 'HASH'
                }
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 5,
                'WriteCapacityUnits': 5
            }
        )
