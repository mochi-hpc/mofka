import sys
import json
import pymargo.core
from pymargo.core import Engine
from mochi.mofka.client import Client


def consume(engine: Engine, group_file: str, topic_name: str):
    client = Client(engine)
    service = client.connect(group_file)
    topic = service.open_topic(topic_name)
    consumer = topic.consumer(name="myconsumer")

    for i in range(0, 100):
        event = consumer.pull().wait()
        print(event.metadata)
        if (i+1) % 10:
            event.acknowledge()


if __name__ == "__main__":

    if len(sys.argv) != 4:
        print(f"Usage: {sys.argv[0]} <protocol> <groupfile> <topic>")
    protocol   = sys.argv[1]
    group_file = sys.argv[2]
    topic_name = sys.argv[3]

    with Engine(protocol, pymargo.core.server) as engine:
        consume(engine, group_file, topic_name)
        engine.finalize()
