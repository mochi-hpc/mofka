import os
import sys
import unittest
import json
import string
import random
import time
import random

from mochi.bedrock.server import Server as BedrockServer
import mochi.mofka.client as mofka


class TestProducer(unittest.TestCase):

    def setUp(self):
        # Create and connect a create
        bedrock_config_file = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "config.json")
        with open(bedrock_config_file) as f:
            self.bedrock_server = BedrockServer("na+sm", config=f.read())
        self.service = mofka.MofkaDriver("mofka.json", self.bedrock_server.margo.mid)

        # create data and metadata
        self.metadata = dict()
        letters = string.ascii_letters
        key_len = random.randint(8, 64)
        val_len = random.randint(16, 128)
        key = ''.join(random.choice(letters) for i in range(key_len))
        val = ''.join(random.choice(letters) for i in range(val_len))
        self.str_data = ''.join(random.choice(letters) for i in range(random.randint(1024, 2048)))
        self.metadata[key] = val

        # create a topic
        name = "my_topic"
        validator = mofka.Validator.from_metadata("my_validator:libmy_validator.so")
        selector = mofka.PartitionSelector.from_metadata("my_partition_selector:libmy_partition_selector.so")
        serializer = mofka.Serializer.from_metadata("my_serializer:libmy_serializer.so")
        self.service.create_topic(name, validator, selector, serializer)
        self.service.add_memory_partition(name, 0)

        # Create a producer
        self.topic = self.service.open_topic(name)
        batchsize = mofka.AdaptiveBatchSize
        thread_pool = mofka.ThreadPool(0)
        ordering = mofka.Ordering.Strict
        self.producer = self.topic.producer(name, batchsize, thread_pool, ordering)

    def tearDown(self):
        del self.metadata
        del self.str_data
        del self.service
        del self.topic
        del self.producer
        self.bedrock_server.finalize()
        del self.bedrock_server

    def test_push_flush_dict_buffer(self):
        data = self.str_data.encode('ascii')
        f = self.producer.push(self.metadata, data)
        f.wait()
        self.producer.flush()


if __name__ == '__main__':
    unittest.main()
