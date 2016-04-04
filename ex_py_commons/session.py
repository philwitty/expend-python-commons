from botocore.credentials import RefreshableCredentials, CredentialProvider
import botocore
import boto3


def async_boto_session(loop, role_arn=None):
    import aiobotocore
    session = aiobotocore.get_session(loop=loop)
    if role_arn:
        _configure_credential_provider(session, role_arn)
    return session


def boto_session(role_arn=None):
    if role_arn:
        session = botocore.session.Session()
        _configure_credential_provider(session, role_arn)
        return boto3.session.Session(botocore_session=session)
    return boto3.session.Session()


def _assume_role(role_arn):
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


def _configure_credential_provider(session, role_arn):
    credential_provider = RoleARNCredentialProvider(role_arn)
    cred_resolver = session.get_component('credential_provider')
    cred_resolver.insert_before('env', credential_provider)


class RoleARNCredentialProvider(CredentialProvider):
    METHOD = 'sts-role'

    def __init__(self, role_arn):
        self._role_arn = role_arn

    def load(self):
        metadata = _assume_role(self._role_arn)
        creds = RefreshableCredentials.create_from_metadata(
            metadata,
            method=self.METHOD,
            refresh_using=lambda: _assume_role(self._role_arn),
        )
        return creds
