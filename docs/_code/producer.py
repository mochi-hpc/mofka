import sys
import pymargo.core
from pymargo.core import Engine
from diaspora_stream.api import Driver


def produce(group_file: str, topic_name: str):
    options = {
        "group_file": group_file,
        "margo": {
            "use_progress_thread": True
        }
    }
    driver = Driver.new("mofka", options)
    topic = driver.open_topic(topic_name)
    producer = topic.producer()

    for i in range(0, 100):
        future = producer.push(
            metadata={"x": i*42, "name": "bob"},
            data=bytes())
        # event_id = future.wait()

    producer.flush()


if __name__ == "__main__":

    if len(sys.argv) != 3:
        print(f"Usage: {sys.argv[0]} <groupfile> <topic>")
    group_file = sys.argv[1]
    topic_name = sys.argv[2]
    produce(group_file, topic_name)
