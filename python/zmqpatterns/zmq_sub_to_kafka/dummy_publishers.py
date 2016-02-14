import sys
import zmq
import logging
import binascii
import msgpack
import time

logger = logging.getLogger(__file__)


def run2():
    context = zmq.Context()
    socket = context.socket(zmq.PUB)

    addresses = [b"tcp://127.0.0.1:9000",
                 b"tcp://127.0.0.1:9001",
                 b"tcp://127.0.0.1:9002",
                 b"tcp://127.0.0.1:9003",
                 b"tcp://127.0.0.1:9004",]

    for address in addresses:
        socket.bind(address)

    filenames = ["../data1.log", "../data2.log"]

    for filename in filenames:
        with open(filename, "rU") as hFile:
            for line in hFile:
                line = line.strip()
                if line == "":
                    continue
                line = binascii.unhexlify(line)
                frames = msgpack.unpackb(line)

                socket.send_multipart(frames)
                time.sleep(0.01)
                print(".", end="")
                sys.stdout.flush()


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
