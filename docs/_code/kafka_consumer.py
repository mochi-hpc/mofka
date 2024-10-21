import sys
import json
import pymargo.core
from pymargo.core import Engine
from mochi.mofka.client import KafkaDriver


def consume(config_file: str, topic_name: str):
    driver = KafkaDriver(config_file)
    topic = service.open_topic(topic_name)
    consumer = topic.consumer(name="myconsumer")

    for i in range(0, 100):
        event = consumer.pull().wait()
        print(event.metadata)
        if (i+1) % 10:
            event.acknowledge()


if __name__ == "__main__":

    if len(sys.argv) != 3:
        print(f"Usage: {sys.argv[0]} <config-file> <topic>")
    config_file = sys.argv[1]
    topic_name = sys.argv[2]

    consume(config_file, topic_name)
