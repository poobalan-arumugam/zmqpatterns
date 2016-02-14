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

logger = logging.getLogger(__file__)

def get_options():
    parser = argparse.ArgumentParser()
    parser.add_argument('--kafka_server',
                        help='kafka server to connect to')
    parser.add_argument('--kafka_topic',
                        help='kafka topic to consume from')
    parser.add_argument('--zknodes',
                        help='zookeeper nodes')
    parser.add_argument('--kafka_consumer_group',
                        help='kafka consumer group')
    parser.add_argument('--zmq_dispatch_address',
                        help='dispatch data to zmq socket on this address')

    parsed = parser.parse_args()

    if parsed.kafka_server is None:
        raise Exception("Please specify --kafka_server")
    if parsed.kafka_topic is None:
        raise Exception("Please specify --kafka_topic")
    if parsed.kafka_consumer_group is None:
        raise Exception("Please specify --kafka_consumer_group")
    if parsed.zknodes is None:
        raise Exception("Please specify --zknodes")
    if parsed.zmq_dispatch_address is None:
        raise Exception("Please specify --zmq_dispatch_address")

    return parsed


def from_kafka_to_zmq(kafka_server_hosts, kafka_topic_name,
                      kafka_consumer_group, zknodes,
                      zmq_dispatch_address,
                      continue_running=None,
                      zmq_context=None):
    kafka_client = pykafka.KafkaClient(hosts=kafka_server_hosts)
    topic = kafka_client.topics[kafka_topic_name.encode("utf8")]

    context = zmq_context
    if context is None:
        context = zmq.Context()
    dispatch_socket = context.socket(zmq.DEALER)
    dispatch_socket.connect(zmq_dispatch_address)

    balanced_consumer = topic.get_balanced_consumer(
        consumer_group=kafka_consumer_group.encode("utf8"),
        auto_commit_enable=True,
        zookeeper_connect=zknodes
        )

    for message in balanced_consumer:
        if message is not None:
            offset_bytes = msgpack.packb(message.offset)
            dispatch_socket.send_multipart([offset_bytes, message.value])
        elif continue_running:
            if not continue_running():
                break


def run():
    options = get_options()

    from_kafka_to_zmq(kafka_server_hosts=options.kafka_server,
                      kafka_topic_name=options.kafka_topic,
                      kafka_consumer_group=options.kafka_consumer_group,
                      zknodes=options.zknodes,
                      zmq_dispatch_address=options.zmq_dispatch_address)



if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    run()
