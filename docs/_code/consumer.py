import sys
import json
import pymargo.core
from pymargo.core import Engine
from mochi.mofka.client import MofkaDriver


def consume(group_file: str, topic_name: str):
    driver = MofkaDriver(group_file, use_progress_thread=True)
    topic = driver.open_topic(topic_name)
    consumer = topic.consumer(name="myconsumer")

    for i in range(0, 100):
        event = consumer.pull().wait()
        print(event.metadata)
        if (i+1) % 10:
            event.acknowledge()


if __name__ == "__main__":

    if len(sys.argv) != 4:
        print(f"Usage: {sys.argv[0]} <groupfile> <topic>")
    group_file = sys.argv[1]
    topic_name = sys.argv[2]
    consume(group_file, topic_name)
