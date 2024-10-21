import sys
import pymargo.core
from pymargo.core import Engine
from mochi.mofka.client import KafkaDriver


def produce(config_file: str, topic_name: str):

    driver = KafkaDriver(config_file, engine)
    topic = driver.open_topic(topic_name)
    producer = topic.producer()

    for i in range(0, 100):
        producer.push(
            metadata={"x": i*42, "name": "bob"},
            data=bytes())

    producer.flush()


if __name__ == "__main__":

    if len(sys.argv) != 3:
        print(f"Usage: {sys.argv[0]} <config-file> <topic>")
    config_file = sys.argv[1]
    topic_name = sys.argv[2]

    produce(config_file, topic_name)
