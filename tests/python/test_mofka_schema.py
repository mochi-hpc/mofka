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

        # create a topic
        schema = {
            "type": "object",
            "properties": {
                "x": { "type": "string" },
                "y": { "type": "integer"},
                "z": { "type": "boolean"}
            },
            "require": ["x", "y"]
        }

        name = "my_topic"
        self.service.create_topic(name, schema=schema)
        self.service.add_memory_partition(name, 0)

        # Create a producer
        self.topic = self.service.open_topic(name)
        batchsize = mofka.AdaptiveBatchSize
        thread_pool = mofka.ThreadPool(0)
        ordering = mofka.Ordering.Strict
        self.producer = self.topic.producer(name, batchsize, thread_pool, ordering)

    def tearDown(self):
        del self.service
        del self.topic
        del self.producer
        self.bedrock_server.finalize()
        del self.bedrock_server

    def test_produce_valid(self):
        metadata = {"x": "abc", "y": 123, "z": True}
        future = self.producer.push(metadata)
        future.wait()

    def test_produce_invalid(self):
        metadata = {"x": 123, "y": "abc", "z": "abc"}
        with self.assertRaises(mofka.ClientException) as context:
            self.producer.push(metadata).wait()


if __name__ == '__main__':
    unittest.main()
