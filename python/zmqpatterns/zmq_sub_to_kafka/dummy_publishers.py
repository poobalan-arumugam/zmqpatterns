import sys
import zmq
import logging
import time

logger = logging.getLogger(__file__)


def run():
    context = zmq.Context()
    socket = context.socket(zmq.PUB)

    addresses = [b"tcp://127.0.0.1:9000",
                 b"tcp://127.0.0.1:9001",
                 b"tcp://127.0.0.1:9002",
                 b"tcp://127.0.0.1:9003",
                 b"tcp://127.0.0.1:9004",]

    for address in addresses:
        socket.bind(address)

    counter = 0
    while True:
        value = str(counter).encode("utf8")
        socket.send_multipart([value])
        counter += 1
        time.sleep(1)
        print(".", end="")
        sys.stdout.flush()


if __name__ == "__main__":
    run()
