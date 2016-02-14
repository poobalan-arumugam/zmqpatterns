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

def run():
    kafka_server_hosts = "127.0.0.1:9092"
    kafka_request_topic_name = "abc-request"
    kafka_response_topic_name = "abc-response"
    kafka_consumer_group = "dummy-test1"
    zknodes = "127.0.0.1:2181"

    kafka_client = pykafka.KafkaClient(hosts=kafka_server_hosts)
    request_topic = kafka_client.topics[kafka_request_topic_name.encode("utf8")]
    response_topic = kafka_client.topics[kafka_response_topic_name.encode("utf8")]

    balanced_consumer = request_topic.get_balanced_consumer(
        consumer_group=kafka_consumer_group.encode("utf8"),
        auto_commit_enable=True,
        zookeeper_connect=zknodes
        )

    with response_topic.get_sync_producer() as kafka_producer:
        for message in balanced_consumer:
            if message is not None:
                offset_bytes = msgpack.packb(message.offset)
                print(message.offset, message.value)
                data = msgpack.unpackb(message.value)
                data[-1] = data[-1] + b"-Response"
                bvalue = msgpack.packb(data)
                kafka_producer.produce(bvalue)
                print("response: ", bvalue)
    



if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    run()
