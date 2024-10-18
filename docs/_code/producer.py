import sys
import pymargo.core
from pymargo.core import Engine
from mochi.mofka.client import MofkaDriver


def produce(engine: Engine, group_file: str, topic_name: str):
    driver = MofkaDriver(group_file, engine)
    topic = driver.open_topic(topic_name)
    producer = topic.producer()

    for i in range(0, 100):
        producer.push(
            metadata={"x": i*42, "name": "bob"},
            data=bytes())

    producer.flush()


if __name__ == "__main__":

    if len(sys.argv) != 4:
        print(f"Usage: {sys.argv[0]} <protocol> <groupfile> <topic>")
    protocol   = sys.argv[1]
    group_file = sys.argv[2]
    topic_name = sys.argv[3]

    with Engine(protocol, pymargo.core.server) as engine:
        produce(engine, group_file, topic_name)
        engine.finalize()
