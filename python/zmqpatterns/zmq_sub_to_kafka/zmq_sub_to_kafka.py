import sys
import os
import zmq
import pykafka
import uuid
import argparse
import logging
import threading
import datetime
import pykafka
import msgpack

logger = logging.getLogger(__file__)


def make_unique_id():
    value = str(uuid.uuid4())
    value = value.encode("utf8")
    return value


class ZmqSubscriber(object):
    def __init__(self, identity, zmq_context, peer_controller_address,
                 zmq_subscriber_address, kafka_zmq_address):
        self._identity = identity
        self.zmq_context = zmq_context
        self.peer_controller_address = peer_controller_address
        self.zmq_subscriber_address = zmq_subscriber_address
        self.kafka_zmq_address = kafka_zmq_address
        self.kafka_send_id = make_unique_id()
        self._continue_running = None

    def identity(self):
        return self._identity

    def run(self):
        logger.debug("starting: %s", self.identity())
        assert self._continue_running is None, (self, self._continue_running)
        self._continue_running = True

        controller_socket = self.zmq_context.socket(zmq.DEALER)
        controller_socket.connect(self.peer_controller_address)

        sub_socket = self.zmq_context.socket(zmq.SUB)
        sub_socket.setsockopt(zmq.SUBSCRIBE, b"")
        sub_socket.connect(self.zmq_subscriber_address)

        kafka_zmq_socket = self.zmq_context.socket(zmq.DEALER)
        kafka_zmq_socket.connect(self.kafka_zmq_address)

        poller = zmq.Poller()
        poller.register(sub_socket, zmq.POLLIN)
        poller.register(controller_socket, zmq.POLLIN)

        controller_socket.send_multipart([b"started"])

        while self.continue_running():
            for socket, option in poller.poll(timeout=1000):
                frames = socket.recv_multipart()
                if socket == sub_socket:
                    self.forward_to_kafka(sub_socket, kafka_zmq_socket, frames)
                elif socket == controller_socket:
                    identity = frames[0]
                    self.handle_controller(identity, frames[1:], controller_socket)
                else:
                    logger.warn("Unknown socket: %s %s", socket, frames)
        pass

        self._continue_running = None

        logger.warn("exitting %s", self.identity())

        controller_socket.close()
        sub_socket.close()
        kafka_zmq_socket.close()

        logger.warn("exit %s", self.identity())

    def continue_running(self):
        return self._continue_running

    def request_quit(self):
        self._continue_running = False


    def forward_to_kafka(self, sub_socket, kafka_zmq_socket, frames):
        logger.warn("SUB: %s %s", self._identity, frames)
        time_value = msgpack.packb(datetime.datetime.now().timetuple())
        header = [self._identity, self.kafka_send_id, time_value]
        kafka_zmq_socket.send_multipart(header, flags=zmq.SNDMORE)
        kafka_zmq_socket.send_multipart(frames)
        pass

    def handle_controller(self, identity, frames, controller_socket):
        if len(frames) < 3:
            return

        request_id = frames[0]
        msgtype = frames[1]
        cmd = frames[1]
        if msgtype == b"cmd":
            if cmd == b"quit":
                self.request_quit()
                logger.debug("quit requested: %s", self.identity())


class ZmqPrimaryController(object):
    def __init__(self, zmq_context,
                 controller_primary_address,
                 kafka_zmq_address,
                 kafka_connection_details):
        self.zmq_context = zmq_context
        self.controller_primary_address = controller_primary_address
        self.kafka_zmq_address = kafka_zmq_address
        self.kafka_connection_details = kafka_connection_details
        self._continue_running = None
        self._subscriptions = {}

    def run(self):
        logger.debug("starting primary controller")
        assert self._continue_running is None, (self, self._continue_running)
        self._continue_running = True

        controller_socket = self.zmq_context.socket(zmq.ROUTER)
        controller_socket.bind(self.controller_primary_address)

        poller = zmq.Poller()
        poller.register(controller_socket, zmq.POLLIN)

        while self.continue_running():
            for socket, option in poller.poll(timeout=1000):
                frames = socket.recv_multipart()
                logger.warn("%s %s", socket, frames)
                # all sockets to here are ROUTERS
                identity = frames[0]
                frames = frames[1:]
                if socket == controller_socket:
                    self.handle_controller(identity, frames, controller_socket, poller)
                else:
                    self.handle_subscriber_controller(identity, frames, socket)
        pass

        self._continue_running = None

    def continue_running(self):
        return self._continue_running

    def request_quit(self):
        self._continue_running = False


    def handle_subscriber_controller(self, frames, socket, poller):
        logger.warn("from sub controller: %s", frames)
        if len(frames) < 3:
            return None

        # frames[0] here (note identity already removed) must be request_id
        request_id = frames[0]
        msgtype = frames[1]
        cmd = frames[2]
        if msgtype == b"cmd":
            if cmd == b"quit":
                self.request_quit()
                logger.debug("quit requested: %s", self.identity())
            elif cmd == b"echo-request":
                socket.send_multipart([identity,
                                       request_id,
                                       b"cmd-response",
                                       b"echo-response"])

    def handle_controller(self, identity, frames, incoming_socket, poller):
        logger.warn("from primary controller: %s", frames)
        if len(frames) < 3:
            return

        # frames[0] here (note identity already removed) must be request_id
        request_id = frames[0]
        msgtype = frames[1]
        cmd = frames[2]
        if msgtype == b"cmd":
            if cmd == b"subscribe":
                self.handle_subscribe_request(identity,
                                              incoming_socket,
                                              request_id, msgtype,
                                              cmd, frames,
                                              poller)
            elif cmd == b"unsubscribe":
                self.handle_unsubscribe_request(identity,
                                                incoming_socket,
                                                request_id, msgtype,
                                                cmd, frames,
                                                poller)
            elif cmd == b"quit":
                self.request_quit()
                logger.debug("quit requested: %s", self.identity())
            elif cmd == b"echo-request":
                incoming_socket.send_multipart(
                    [identity,
                     request_id,
                     b"cmd-response",
                     b"echo-response"])

        pass

    def handle_subscribe_request(self, identity,
                                 incoming_socket,
                                 request_id,
                                 msgtype, cmd, frames,
                                 poller):
        if len(frames) < 5:
            return None

        name = frames[3]
        address = frames[4]
        socket = error = None
        try:
            socket, error = self.start_subscriber(name, address)
        except Exception as ex:
            error = str(ex)
        if error:
            logger.error("%s %s", frames, error)
            incoming_socket.send_multipart(
                [identity, request_id,
                 b"cmd-response",
                 b"subscribe-error",
                 error.encode("utf8")])
        elif socket:
            incoming_socket.send_multipart(
                [identity, request_id,
                 b"cmd-response",
                 b"subscribed",
                 name])
            poller.register(socket, zmq.POLLIN)

    def handle_unsubscribe_request(self, identity,
                                   incoming_socket,
                                   request_id,
                                   msgtype, cmd, frames,
                                   poller):
        if len(frames) < 5:
            return None

        name = frames[3]
        address = frames[4]
        socket, error = self.stop_subscriber(name, address)
        if error:
            logger.error("%s %s", frames, error)
            incoming_socket.send_multipart(
                [identity, request_id,
                 b"cmd-response",
                 b"unsubscribe-error",
                 error.encode("utf8")])
        elif socket:
            incoming_socket.send_multipart(
                [identity, request_id,
                 b"cmd-response",
                 b"unsubscribed",
                 name])
            poller.unregister(socket)


    def start_subscriber(self, name, address):
        if name in self._subscriptions:
            return None, "Name already in use"

        peer_address = b"inproc://controller/" + name
        socket = self.zmq_context.socket(zmq.ROUTER)
        socket.bind(peer_address)


        subscriber = ZmqSubscriber(identity=name,
                                   zmq_context=self.zmq_context,
                                   peer_controller_address=peer_address,
                                   zmq_subscriber_address=address,
                                   kafka_zmq_address=self.kafka_zmq_address)
        thread = threading.Thread(target=subscriber.run)
        self._subscriptions[name] = (thread, subscriber, socket)
        thread.setDaemon(True)
        thread.start()

        return socket, None


    def stop_subscriber(self, name, address):
        if name not in self._subscriptions:
            return None, "Name not in use"

        thread, subscriber, socket = self._subscriptions[name]
        subscriber.request_quit()
        del self._subscriptions[name]
        return socket, None


def run_controller(primary_controller_address,
                   kafka_connection_details):
    zmq_context = zmq.Context()

    kafka_zmq_address = kafka_connection_details # hack

    primary_controller = ZmqPrimaryController(zmq_context,
                                              primary_controller_address,
                                              kafka_zmq_address,
                                              kafka_connection_details)
    primary_controller.run()


def main():
    logging.basicConfig(level=logging.DEBUG)

    parser = argparse.ArgumentParser()
    parser.add_argument('--controller',
                        help='zmq_sub primary controller address')
    parser.add_argument('--config',
                        help='configuration filename')
    parser.add_argument('--kafka_address',
                        help='kafka address')

    parsed = parser.parse_args()

    primary_controller_address = parsed.controller
    config_filename = parsed.config
    kafka_address = parsed.kafka_address

    kafka_connection_details = kafka_address

    run_controller(primary_controller_address,
                   kafka_connection_details)

if __name__ == "__main__":
    main()
