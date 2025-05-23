import os
import sys
import unittest
import json
import string
import random
import subprocess
import tempfile
import time
import random
import uuid

wd = os.getcwd()

from pymargo.core import Engine
from mochi.bedrock.server import Server as BedrockServer
import mochi.mofka.client as mofka


def my_data_selector(metadata, descriptor):
    return descriptor

def my_data_broker(metadata, descriptor):
    data = bytearray(descriptor.size)
    return [data]


class TestConsumer(unittest.TestCase):

    def setUp(self):
        # Create and connect a create
        with open("kafka.json", "w+") as f:
            f.write(json.dumps({"bootstrap.servers": "localhost:9092"}))
        self.service = mofka.KafkaDriver("kafka.json")

        # create data and metadata
        self.metadata = dict()
        letters = string.ascii_letters
        key_len = random.randint(8, 16)
        val_len = random.randint(8, 16)
        key = ''.join(random.choice(letters) for i in range(key_len))
        val = ''.join(random.choice(letters) for i in range(val_len))
        self.metadata[key] = val
        self.data = ''.join(random.choice(letters) for i in range(random.randint(1024, 2048))).encode('ascii')

        # create a topic
        topic_name = "my_topic_" + str(uuid.uuid4())[0:8]
        self.service.create_topic(topic_name)
        import time
        time.sleep(2) # sometimes asking for the topic immediately yield "topic does not exist"
        self.topic = self.service.open_topic(topic_name)

        # Create a producer
        batchsize = mofka.AdaptiveBatchSize
        thread_pool = mofka.ThreadPool(0)
        ordering = mofka.Ordering.Strict
        self.producer = self.topic.producer(batch_size=batchsize, thread_pool=thread_pool, ordering=ordering)

        # Push a single event
        f = self.producer.push(self.metadata, self.data)
        f.wait()
        self.producer.flush()

        # Create a consumer
        self.consumer = self.topic.consumer(
                name="my_consumer",
                batch_size=1,
                data_broker=my_data_broker,
                data_selector=my_data_selector)

    def tearDown(self):
        del self.service
        del self.topic
        del self.consumer
        del self.producer
        os.remove("kafka.json")

    def test_get_name(self):
        """Test consumer get name"""
        name = self.consumer.name
        self.assertEqual(name, "my_consumer")

    def test_get_batchsize(self):
        """Test get consumer batch_size"""
        b = self.consumer.batch_size
        # TODO

    def test_get_data_broker(self):
        """Test get data broker"""
        db = self.consumer.data_broker

    def test_get_topic(self):
        """Test get topic"""
        topic = self.consumer.topic

    def test_get_data_selector(self):
        """Test get data selector"""
        dataselector = self.consumer.data_selector

    def test_get_thread_pool(self):
        """Test get threadpool"""
        pool = self.consumer.thread_pool

    def test_pull(self):
        """Test pull event data"""
        f = self.consumer.pull()
        event = f.wait()
        data = event.data
        self.assertEqual(len(data), 1)
        self.assertEqual(data[0], self.data)


if __name__ == '__main__':
    unittest.main()
