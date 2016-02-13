import sys
import os
import argparse
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
                        help='kafka topic to publish to')
    parser.add_argument('--zknodes',
                        help='zookeeper nodes')
    parser.add_argument('--consumer_group',
                        help='consumer group')

    parsed = parser.parse_args()

    if parsed.kafka_server is None:
        raise Exception("Please specify --kafka_server")

    return parsed


def run():
    options = get_options()

    kafka_client = pykafka.KafkaClient(hosts=options.kafka_server)
    topic = kafka_client.topics[options.kafka_topic.encode("utf8")]

    balanced_consumer = topic.get_balanced_consumer(
        consumer_group=options.consumer_group.encode("utf8"),
        auto_commit_enable=True,
        zookeeper_connect=options.zknodes
        )

    for message in balanced_consumer:
        if message is not None:
            print(message.offset, message.value)



if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    run()
