from boto3.session import Session
from botocore.client import Config
from urllib.parse import urlparse


def concatenate_files_from_urls(urls, **kwargs):
    session = kwargs.get('aws_session')
    return b''.join(
        [read_file_from_url(url, aws_session=session) for url in urls])


def read_file_from_url(url, **kwargs):
    session = kwargs.get('aws_session', Session())
    res = urlparse(url)

    if res.scheme == 's3':
        bucket = res.netloc
        key = res.path[1:]

        s3 = session.client('s3', config=Config(signature_version='s3v4'))
        response = s3.get_object(
            Bucket=bucket,
            Key=key,
        )
        return response['Body'].read()
    elif res.scheme == 'file':
        path = res.netloc + res.path
        with open(path, 'rb') as f:
            contents = f.read()
        return contents
