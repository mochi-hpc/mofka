import os
import sys
from time import sleep
import traceback
import mochi.mofka.client as mofka
from concurrent.futures import ProcessPoolExecutor

group_file = "mofka.flock.json"

class MofkaSingleton:

    _instance = None
    _pid = 0

    def __new__(cls):
        print(f"Creating new MofkaSingleton, previous was {cls._instance} with pid {cls._pid}")
        if cls._instance is None or os.getpid() != cls._pid:
            cls._instance = super(MofkaSingleton, cls).__new__(cls)
            cls._pid = os.getpid()
            cls._instance.driver = mofka.MofkaDriver(group_file=group_file, use_progress_thread=True)
        return cls._instance

def consume():

    try:
        topic_name = "my_topic"
        s = MofkaSingleton()
        topic = s.driver.open_topic(topic_name)
        consumer = topic.consumer(
            name="my_consumer",
            thread_pool=mofka.ThreadPool(0),
            batch_size=mofka.AdaptiveBatchSize,
            data_broker=mofka.ByteArrayAllocator,
            data_selector=mofka.FullDataSelector)
        print("Consumer created", flush=True)

        event_count = 0
        while event_count < 20:
            event = consumer.pull().wait()
            print(f"Received event ID {event.event_id} with metadata {event.metadata}", flush=True)
            event_count += 1
    except:
        print(traceback.format_exc(), flush=True)

    return "Consumer done!"

def produce():

    try:
        print("Creating MofkaSingleton")
        topic_name = "my_topic"
        s = MofkaSingleton()
        print(f"Opening topic {topic_name}")
        topic = s.driver.open_topic(topic_name)
        producer = topic.producer(
                batch_size=mofka.AdaptiveBatchSize,
                thread_pool=mofka.ThreadPool(1),
                ordering=mofka.Ordering.Strict)
        print("Producer created", flush=True)

        for i in range(10):
            message = f"\"test_event_{i}\""
            print(f"Submitting {message}", flush=True)
            producer.push(message)

        sleep(1)

        for i in range(10):
            message = f"\"test_event_{i+10}\""
            print(f"Submitting {message}", flush=True)
            producer.push(message)

        producer.flush()
        print("Producer completed", flush=True)
    except:
        print(traceback.format_exc(), flush=True)

    return "Producer done!"

with ProcessPoolExecutor(max_workers=2) as executor:
    future_p = executor.submit(produce)
    future_c = executor.submit(consume)
    print(future_p.result(), flush=True)
    print(future_c.result(), flush=True)
