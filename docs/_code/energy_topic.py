import sys
import pymargo.core
from pymargo.core import Engine
from mochi.mofka.client import MofkaDriver


def main(group_file: str):
    driver = MofkaDriver(group_file, use_progress_thread=True)

    # START CREATE TOPIC
    from mochi.mofka.client import Validator, PartitionSelector, Serializer

    validator = Validator.from_metadata(
        "energy_validator:libenergy_validator.so", {"energy_max": 100})
    selector = PartitionSelector.from_metadata(
        "energy_partition_selector:libenergy_partition_selector.so", {"energy_max": 100})
    serializer = Serializer.from_metadata(
        "energy_serializer:libenergy_serializer.so", {"energy_max": 100})

    driver.create_topic(
        topic_name="collisions",
        validator=validator,
        selector=selector,
        serializer=serializer)
    # END CREATE TOPIC

    # START ADD PARTITION
    # add an in-memory partition
    driver.add_memory_partition(
        topic_name="collisions",
        server_rank=0)
    # add a default partition (all arguments specified)
    driver.add_default_partition(
        topic_name="collisions",
        server_rank=0,
        metadata_provider="my_metadata_provider@local",
        data_provider="my_data_provider@local",
        partition_config={},
        pool_name="__primary__")
    # add a default partition (discover providers automatically)
    driver.add_default_partition(
        topic_name="collisions",
        server_rank=0)
    # END ADD PARTITION

    # START ADD PROVIDERS
    metadata_provider = driver.add_metadata_provider(
        server_rank=0,
        database_type="log",
        database_config={
            "path": "/tmp/mofka-log",
            "create_if_missing": True
        })
    data_provider = driver.add_data_provider(
        server_rank=0,
        target_type="abtio",
        target_config={
            "path": "/tmp/mofka-data",
            "create_if_missing": True
        })
    driver.add_default_partition(
        topic_name="collisions",
        server_rank=0,
        metadata_provider=metadata_provider,
        data_provider=data_provider)
    # END ADD PROVIDERS

    # START PRODUCER
    from mochi.mofka.client import ThreadPool, AdaptiveBatchSize, Ordering

    topic = driver.open_topic("collisions")
    thread_pool = ThreadPool(4)
    batch_size = AdaptiveBatchSize # or an integer > 0
    ordering = Ordering.Strict # or Ordering.Loose

    producer = topic.producer(
        name="app1",
        batch_size=batch_size,
        thread_pool=thread_pool,
        ordering=ordering)
    # END PRODUCER

    # START EVENT
    # data can be any object adhering to the buffer protocol
    data1 = b"abcd"
    data2 = bytearray("efgh", encoding="ascii")
    data3 = memoryview(data1)
    # or a list of such objects
    data4 = [data1, data2, data3]

    metadata1 = """{"energy": 42}""" # use a string
    metadata2 = {"energy": 42} # or use a dictionary
    # END EVENT

    # START PRODUCE EVENT
    future = producer.push(metadata=metadata1, data=data1)
    future.completed # returns True if the future has completed
    event_id = future.wait()

    producer.flush()
    # END PRODUCE EVENT

    # START CONSUMER
    from mochi.mofka.client import ThreadPool, AdaptiveBatchSize, DataDescriptor

    batch_size = AdaptiveBatchSize
    thread_pool = ThreadPool(0)

    def data_selector(metadata, descriptor):
        if metadata["energy"] > 20:
            return descriptor
        else:
            return None

    def data_broker(metadata, descriptor):
        # note that we return a *list* of objects satisfying the buffer protocol
        return [ bytearray(descriptor.size) ]

    consumer = topic.consumer(
        name="app2",
        thread_pool=thread_pool,
        batch_size=batch_size,
        data_selector=data_selector,
        data_broker=data_broker)
    # END CONSUMER

    # START CONSUME EVENTS
    future = consumer.pull()
    future.completed # returns true if the future has completed

    event    = future.wait()
    data     = event.data
    metadata = event.metadata
    event_id = event.event_id

    event.acknowledge()
    # END CONSUME EVENTS

if __name__ == "__main__":

    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} <groupfile>")
    group_file = sys.argv[1]
    main(group_file)
