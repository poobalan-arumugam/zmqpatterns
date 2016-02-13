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
                        help='kafka topic to publish to')

    parsed = parser.parse_args()

    if parsed.kafka_server is None:
        raise Exception("Please specify --kafka_server")

    return parsed


def commit(kafka_producer, mcollected):
    while True:
        try:
            msg, exc = kafka_producer.get_delivery_report(block=False)
            bframes, index = mcollected[msg.partition_key]
            collected = mcollected["LIST"]
            if exc is not None:
                logger.warn("Failed to deliver msg %s: %s", msg.partition_key, repr(exc))
                kafka_producer.produce(bframes, partition_key=msg.partition_key)
            else:
                collected = mcollected["LIST"]
                if collected[index]:
                    assert collected[index][0] == msg.partition_key, (collected[index], msg)
                collected[index] = None
                del mcollected[msg.partition_key]
                logger.warn("%s - %s", msg.partition_key, len(mcollected))
                if len(mcollected) == 1:
                    mcollected["LIST"] = []
                    logger.warn("Zeroed")
        except queue.Empty:
            break


def run():
    options = get_options()

    kafka_client = pykafka.KafkaClient(hosts=options.kafka_server)
    topic = kafka_client.topics[options.kafka_topic.encode("utf8")]

    context = zmq.Context()
    recv_socket = context.socket(zmq.ROUTER)
    recv_socket.bind("tcp://127.0.0.1:10001")

    poller = zmq.Poller()
    poller.register(recv_socket, zmq.POLLIN)

    with topic.get_producer(delivery_reports=True) as kafka_producer:
        last_updated = datetime.datetime.now()
        mcollected = {}
        mcollected["LIST"] = []
        counter = 0
        while True:
            collected = mcollected["LIST"]
            recved = False
            for socket, option in poller.poll(timeout=1000):
                recved = True
                frames = socket.recv_multipart()
                bframes = msgpack.packb(frames)

                partition_key = str(counter).encode("utf8")
                kafka_producer.produce(bframes, partition_key=partition_key)
                counter += 1

                collected.append((partition_key, bframes))
                mcollected[partition_key] = (bframes, len(collected) - 1)

                print(partition_key, frames)


                if len(mcollected) > 1000:
                    commit(kafka_producer, mcollected)
                    pass

                if len(mcollected) > 10:
                    current_timestamp = datetime.datetime.now()
                    delta = current_timestamp - last_updated
                    if delta.total_seconds() > 5:
                        commit(kafka_producer, mcollected)
                        last_updated = current_timestamp
                        pass


            if not recved:
                if len(mcollected) > 0:
                    commit(kafka_producer, mcollected)
                pass



if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    run()
