import sys
import os
import argparse
import zmq
import uuid
import logging
import msgpack
import datetime
import queue
import pykafka
import threading

try:
    from . import simple_kafka_to_zmq as ktoz
    from . import simple_zmq_to_kafka as ztok
except:
    import simple_kafka_to_zmq as ktoz
    import simple_zmq_to_kafka as ztok


logger = logging.getLogger(__file__)

def get_options():
    parser = argparse.ArgumentParser()
    parser.add_argument('--kafka_server',
                        help='kafka server to connect to')
    parser.add_argument('--kafka_dispatch_topic',
                        help='kafka topic to dispatch to')
    parser.add_argument('--kafka_consume_topic',
                        help='kafka topic to consume from')
    parser.add_argument('--zknodes',
                        help='zookeeper nodes')
    parser.add_argument('--kafka_consumer_group',
                        help='kafka consumer group')
    parser.add_argument('--zmq_address',
                        help='receive and dispatch data using a zmq socket on this address')

    parsed = parser.parse_args()

    if parsed.kafka_server is None:
        raise Exception("Please specify --kafka_server")
    if parsed.kafka_dispatch_topic is None:
        raise Exception("Please specify --kafka_dispatch_topic")
    if parsed.kafka_consume_topic is None:
        raise Exception("Please specify --kafka_consume_topic")
    if parsed.kafka_consumer_group is None:
        raise Exception("Please specify --kafka_consumer_group")
    if parsed.zknodes is None:
        raise Exception("Please specify --zknodes")
    if parsed.zmq_address is None:
        raise Exception("Please specify --zmq_address")

    return parsed


class ContinueRunning(object):
    def __init__(self):
        self._continue_running = True
    def request_quit(self):
        self._continue_running = False
    def __call__(self):
        return self._continue_running

def zmq_to_and_from_kafka(kafka_server_hosts,
                          kafka_dispatch_topic_name,
                          kafka_consume_topic_name,
                          kafka_consumer_group, zknodes,
                          zmq_address,
                          continue_running=None):

    context = zmq.Context()
    main_socket = context.socket(zmq.ROUTER)
    main_socket.bind(zmq_address)


    zmq_dispatch_address = "inproc://dispatch"
    zmq_consume_address = "inproc://consume"

    def run1():
        ztok.from_zmq_to_kafka(zmq_listener_address=zmq_dispatch_address,
                               kafka_server_hosts=kafka_server_hosts,
                               kafka_topic_name=kafka_dispatch_topic_name,
                               continue_running=continue_running,
                               zmq_context=context)

    thread1 = threading.Thread(target=run1)
    thread1.setDaemon(True)
    thread1.start()

    dispatch_socket = context.socket(zmq.DEALER)
    dispatch_socket.connect(zmq_dispatch_address)

    consume_socket = context.socket(zmq.ROUTER)
    consume_socket.bind(zmq_consume_address)


    def run2():
        ktoz.from_kafka_to_zmq(kafka_server_hosts=kafka_server_hosts,
                               kafka_topic_name=kafka_consume_topic_name,
                               kafka_consumer_group=kafka_consumer_group,
                               zknodes=zknodes,
                               zmq_dispatch_address=zmq_consume_address,
                               continue_running=continue_running,
                               zmq_context=context)

    thread2 = threading.Thread(target=run2)
    thread2.setDaemon(True)
    thread2.start()

    poller = zmq.Poller()
    poller.register(main_socket, zmq.POLLIN)
    poller.register(consume_socket, zmq.POLLIN)

    while continue_running():
        for socket, event in poller.poll(timeout=1000):
            frames = socket.recv_multipart()
            if socket == main_socket:
                dispatch_socket.send_multipart(frames)
                logger.debug("main to dispatch: %s", frames)
            elif socket == consume_socket:
                logger.debug("consume to main: %s", frames)
                try:
                    xframes = msgpack.unpackb(frames[-1])
                    logger.debug("xframes: %s", xframes)
                    main_socket.send_multipart(xframes[1:])
                except Exception as ex:
                    logger.error("ERROR: %s : %s", ex, frames)
            else:
                logger.error("unknown socket: %s - %s", socket, frames)



def run():
    options = get_options()

    continue_running = ContinueRunning()

    zmq_to_and_from_kafka(
        kafka_server_hosts=options.kafka_server,
        kafka_dispatch_topic_name=options.kafka_dispatch_topic,
        kafka_consume_topic_name=options.kafka_consume_topic,
        kafka_consumer_group=options.kafka_consumer_group,
        zknodes=options.zknodes,
        zmq_address=options.zmq_address,
        continue_running=continue_running)



if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    run()
