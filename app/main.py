import socket  # noqa: F401
import threading
import sys

from app.header_request import HeaderRequest, UnknownApiKeyResponse
from app.kafka_response import KafkaResponse
from .kafka_encode import KafkaEncode
from . import kafka_handlers, kafka_parser


def accept_client(client: socket.socket):
    while True:
        data = client.recv(1024)
        if len(data) == 0:
            break
        try:
            response_bytes = kafka_response(data)
        except Exception as e:
            print("exeception: ", e, file=sys.stderr)
            response_bytes = b""
            raise e
            break

        log = f"input: {data}\noutput: {response_bytes}\n"
        print(log)
        client.sendall(response_bytes)

    print("closing socket")
    client.close()


def kafka_response(data: bytes) -> bytes:
    header, body_bytes = kafka_parser.parse_header_request(data)

    return kafka_build_response(header, body_bytes)


def kafka_build_response(header: HeaderRequest, body_bytes: bytes) -> bytes:
    response_body = kafka_body_response(header, body_bytes)
    return KafkaResponse(header, response_body).encode()


def kafka_body_response(header: HeaderRequest, body_bytes: bytes) -> KafkaEncode:
    handlers = kafka_handlers.get_handles()
    handler = handlers.get(header.api_key)
    if handler is None:
        return UnknownApiKeyResponse(header.api_key)

    body_parser, callback = handler
    request = body_parser(body_bytes)
    response = callback(header, request)

    return response


def main():
    server = socket.create_server(("localhost", 9092), reuse_port=True)

    while True:
        client_socket, _ = server.accept()  # wait for client
        thread = threading.Thread(target=accept_client, args=(client_socket,))
        thread.start()


if __name__ == "__main__":
    main()
