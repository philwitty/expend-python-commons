import boto3
from botocore.session import Session
from botocore.credentials import RefreshableCredentials, CredentialProvider


def assume_role(role_arn):
    sts = boto3.client('sts')
    credentials = sts.assume_role(
        RoleArn=role_arn,
        RoleSessionName='RefreshableBotoSession'
    )['Credentials']
    return {
        'access_key':  credentials['AccessKeyId'],
        'secret_key':  credentials['SecretAccessKey'],
        'token':       credentials['SessionToken'],
        'expiry_time': str(credentials['Expiration']),
    }


class RoleARNCredentialProvider(CredentialProvider):
    METHOD = 'sts-role'

    def __init__(self, role_arn):
        self._role_arn = role_arn

    def load(self):
        metadata = assume_role(self._role_arn)
        creds = RefreshableCredentials.create_from_metadata(
            metadata,
            method=self.METHOD,
            refresh_using=lambda: assume_role(self._role_arn),
        )
        return creds


def boto_session(role_arn=None):
    if role_arn is None:
        return boto3.session.Session()
    else:
        credential_provider = RoleARNCredentialProvider(role_arn)
        botocore_session = Session()
        cred_resolver = botocore_session.get_component('credential_provider')
        cred_resolver.insert_before('env', credential_provider)
        return boto3.session.Session(botocore_session=botocore_session)
