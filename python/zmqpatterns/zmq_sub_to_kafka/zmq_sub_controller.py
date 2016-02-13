import sys
import os
import argparse
import zmq
import uuid
import logging

logger = logging.getLogger(__file__)

def get_options():
    parser = argparse.ArgumentParser()
    parser.add_argument('--server',
                        help='zmq_sub server to connect to')

    subparsers = parser.add_subparsers(dest='main')
    parser_main = subparsers.add_parser('main',
                                        description="commands to main controller")
    parser_subscribers = subparsers.add_parser('subscriber',
                                               description="subscribers: add and more")


    subparsers_subscribers = parser_subscribers.add_subparsers(dest="subscribers")

    parser_subscribers_add = subparsers_subscribers.add_parser('add',
                                                               description="add subscriber(s)")
    parser_subscribers_add.add_argument('--address',
                                        help='add a remote zmq subscriber address')
    parser_subscribers_add.add_argument('--name',
                                        help='provide a name of the target publisher')


    parser_subscribers_remove = subparsers_subscribers.add_parser('remove',
                                                                  description='remove subscriber(s)')
    parser_subscribers_remove.add_argument('--address',
                                           help='add a remote zmq subscriber address')
    parser_subscribers_remove.add_argument('--name',
                                           help='provide a name of the target publisher')

    if len(sys.argv) < 2:
        sys.argv.append('--help')

    parsed = parser.parse_args()

    server_address = parsed.server

    if parsed.main == "main":
        print("MAIN")
    elif parsed.main == "subscriber":
        address = name = None
        if parsed.subscribers in ["add", "remove"]:
            address = parsed.address
            name = parsed.name
            if name is None:
                print("Please supply --name")
                return
            if address is None:
                print("Please supply --address")
                return

        if parsed.subscribers == "add":
            print("subscribe to: ", address, " as name:", name)
            process_subscriber_add(server_address=server_address,
                                   address=address,
                                   name=name)
        elif parsed.subscribers == "remove":
            print("unsubscribe from: ", address, " as name:", name)
            process_subscriber_remove(server_address=server_address,
                                      address=address,
                                      name=name)
            pass


class Connection(object):
    def __init__(self, server_address, timeout=10000):
        self.server_address = server_address
        self.timeout = timeout

        self.zmq_context = zmq.Context()
        self.socket = self.zmq_context.socket(zmq.DEALER)
        self.socket.connect(self.server_address)

        self.poller = zmq.Poller()
        self.poller.register(self.socket, zmq.POLLIN)

        self.echo_and_wait_for_reply()

    def make_request_id(self):
        return str(uuid.uuid4()).encode("utf8")

    def echo_and_wait_for_reply(self):
        request_id = self.make_request_id()
        self.send_and_recv_multipart_for_request_id(request_id,
                                                    [b"cmd",
                                                     b"echo-request"])

    def send_multipart(self, frames):
        return self.socket.send_multipart(frames)

    def recv_multipart_or_default(self, default_result=None, timeout=None):
        if timeout is None:
            timeout = self.timeout
        for socket, options in self.poller.poll(timeout=timeout):
            frames = socket.recv_multipart()
            logger.debug("recv: %s", frames)
            return frames
        if default_result is None:
            default_result = []
        logger.debug("recv timeout - sending default: %s", default_result)
        return default_result

    def send_and_recv_multipart_for_request_id(self, request_id, frames):
        request = [request_id] + frames
        while True:
            self.send_multipart(request)
            frames = self.recv_multipart_or_default(timeout=1000)
            if len(frames) < 3:
                logger.warn("GOT garbage frame: %s", frames)
                continue
            if frames[0] == request_id:
                return frames
            logger.debug("Ignoring frame: %s - expecting %s", frames, request_id)

        pass



def process_subscriber_add(server_address, address, name):
    print(server_address, name, address)

    connection = Connection(server_address)

    connection.send_and_recv_multipart_for_request_id(
        connection.make_request_id(),
        [b"cmd",
         b"subscribe",
         name.encode("utf8"),
         address.encode("utf8")])


def process_subscriber_remove(server_address, address, name):
    print(server_address, name, address)

    connection = Connection(server_address)

    connection.send_and_recv_multipart_for_request_id(
        connection.make_request_id(),
        [b"cmd",
         b"unsubscribe",
         name.encode("utf8"),
         address.encode("utf8")])


def main():
    logging.basicConfig(level=logging.DEBUG)
    get_options()


if __name__ == "__main__":
    main()
