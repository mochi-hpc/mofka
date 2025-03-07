import os
import sys
import unittest
import json
import string
import random

from mochi.bedrock.server import Server as BedrockServer
import mochi.mofka.client as mofka



class TestTopicHandle(unittest.TestCase):

    def setUp(self):
        bedrock_config_file = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "config.json")
        with open(bedrock_config_file) as f:
            self.bedrock_server = BedrockServer("na+sm", config=f.read())
        self.service = mofka.MofkaDriver("mofka.json", self.bedrock_server.margo.mid)

        name = "my_topic"
        validator = mofka.Validator.from_metadata("my_validator:libmy_validator.so")
        selector = mofka.PartitionSelector.from_metadata("my_partition_selector:libmy_partition_selector.so")
        serializer = mofka.Serializer.from_metadata("my_serializer:libmy_serializer.so")
        self.service.create_topic(name, validator, selector, serializer)
        self.topic = self.service.open_topic(name)

    def tearDown(self):
        del self.service
        del self.topic
        self.bedrock_server.finalize()
        del self.bedrock_server

    def test_create_producer(self):
        """Test create a producer associated with a topic"""
        name = "myproducer"
        batchsize = random.randint(1,8)
        thread_pool = mofka.ThreadPool(random.randint(1,8))
        ordering = mofka.Ordering.Strict
        producer = self.topic.producer(name, batchsize, thread_pool, ordering)

    def test_create_producer_default(self):
        """Test create data producer with default params"""
        name = "myproducer"
        producer = self.topic.producer(name)

    def test_create_consumer_default(self):
        """Test create data consumer with default params"""
        name = "myconsumer"
        consumer = self.topic.consumer(name)

    def test_create_consumer(self):
        """Test Create a consumer associated with a topic"""
        import my_broker_selector
        name = "myconsumer"
        batchsize = random.randint(1,8)
        consumer = self.topic.consumer(
                name=name, batch_size=batchsize,
                data_broker=my_broker_selector.broker,
                data_selector=my_broker_selector.selector,
                targets=list(range(len(self.topic.partitions))))


if __name__ == '__main__':
    unittest.main()
