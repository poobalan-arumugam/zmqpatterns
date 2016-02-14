python simple_zmq_to_from_kafka.py \
    --kafka_server 127.0.0.1:9092 \
    --kafka_dispatch_topic abc-request \
    --kafka_consume_topic abc-response \
    --kafka_consumer_group testgroup1 \
    --zknodes 127.0.0.1:2181 \
    --zmq_address tcp://127.0.0.1:10003
