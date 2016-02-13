import sys
import zmq
import logging
import time

logger = logging.getLogger(__file__)


def run():
    context = zmq.Context()
    socket = context.socket(zmq.ROUTER)
    socket.bind("tcp://127.0.0.1:10001")

    while True:
        frames = socket.recv_multipart()
        print(frames)
        sys.stdout.flush()


if __name__ == "__main__":
    run()
