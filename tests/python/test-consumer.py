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

wd = os.getcwd()

from pymargo.core import Engine
from mochi.bedrock.server import Server as BedrockServer
import pymofka_client as mofka


def my_data_selector(metadata, descriptor):
    return descriptor

def my_data_broker(metadata, descriptor):
    data = bytearray(descriptor.size)
    return [data]


class TestConsumer(unittest.TestCase):

    def setUp(self):
        # Create and connect a create
        bedrock_config_file = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "config.json")
        with open(bedrock_config_file) as f:
            self.bedrock_server = BedrockServer("na+sm", config=f.read())
        self.mid = self.bedrock_server.margo.mid
        self.client = mofka.Client(mid=self.mid)
        self.service = self.client.connect("mofka.json")

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
        topic_name = "my_topic"
        self.service.create_topic(topic_name)
        self.service.add_memory_partition(topic_name, 0)
        self.topic = self.service.open_topic(topic_name)

        # Create a producer
        self.producer = self.topic.producer(ordering=mofka.Ordering.Strict)

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
        del self.mid
        del self.service
        del self.client
        del self.topic
        del self.consumer
        del self.producer
        self.bedrock_server.finalize()

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
