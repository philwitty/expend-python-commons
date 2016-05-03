from ex_pb import ex_crypto_pb_pb2
from uuid import uuid4
import socket
import ssl
import struct


class CryptoService:

    def __init__(self, crypto_config):
        self.socket = CryptoSocket(crypto_config)

    def hmac(self, key_ref, data):

        crypto_key = ex_crypto_pb_pb2.CryptoKey()
        crypto_key.key_type = ex_crypto_pb_pb2.CryptoKey.AWS_KMS
        crypto_key.reference = key_ref

        crypto_request = ex_crypto_pb_pb2.CryptoRequest()
        crypto_request.request_type = ex_crypto_pb_pb2.CryptoRequest.HMAC
        crypto_request.key.CopyFrom(crypto_key)
        crypto_request.data = data

        return self._make_crypto_request(crypto_request)

    def _make_crypto_request(self, crypto_request):
        header = ex_crypto_pb_pb2.MessageHeader()
        header.request_id = str(uuid4())

        request = ex_crypto_pb_pb2.Request()
        request.header.CopyFrom(header)
        request.crypto_request.CopyFrom(crypto_request)

        response = self._make_request(request)
        if not response.HasField('crypto_response'):
            status_code = response.response_status.code
            status_message = response.response_status.message
            raise CryptoException(status_code, status_message)
        return response.crypto_response.data

    def _make_request(self, request_message):
        with self.socket:
            serialized_req = request_message.SerializeToString()
            self.socket.write(serialized_req)
            response_data = self.socket.read()
        response = ex_crypto_pb_pb2.Response()
        response.ParseFromString(response_data)
        return response


class CryptoSocket:

    def __init__(self, crypto_config):
        self.host = crypto_config['host']
        self.port = crypto_config['port']
        self.ssl_context = \
            ssl.create_default_context(cafile=crypto_config['cafile'])
        with open(crypto_config['keypassfile'], 'rt') as keypassfile:
            self.ssl_context.load_cert_chain(crypto_config['certfile'],
                                             keyfile=crypto_config['keyfile'],
                                             password=keypassfile.read())

    def __enter__(self):
        self._connect()

    def __exit__(self, exc_type, exc_value, traceback):
        self.sock.close()

    def _connect(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock = self.ssl_context.wrap_socket(self.sock,
                                                 server_hostname=self.host)
        self.sock.connect((self.host, self.port))

    def write(self, data):
        # Write length in 4 bytes followed by data
        header = struct.pack('!I', len(data))
        self.sock.sendall(header + data)

    def read(self):
        # Read length then get required amount of bytes
        data = self.sock.recv(4)
        data_len = struct.unpack("!I", data)[0]
        return self.sock.recv(data_len)


class CryptoException(Exception):
    def __init__(self, status_code, status_message):
        message = ('crypto service error status_code=%s status_message=%s'
                   % (status_code, status_message))
        super().__init__(message)
        self.status_code = status_code
        self.status_message = status_message
