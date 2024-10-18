import os
import sys
import unittest
import json
import string
import random

from mochi.bedrock.server import Server as BedrockServer
import mochi.mofka.client as mofka
import my_broker_selector


class TestMofkaDriver(unittest.TestCase):

    def setUp(self):
        with open("kafka.json", "w+") as f:
            f.write(json.dumps({"bootstrap.servers": "localhost:9092"}))
        self.service = mofka.KafkaDriver("kafka.json")

    def tearDown(self):
        del self.service
        os.remove("kafka.json")

    def test_create_open_topic(self):
        """Test create and open a topic"""
        name = "my_topic"
        self.service.create_topic(name, num_partitions=2, replication_factor=1)
        topic = self.service.open_topic(name)


if __name__ == '__main__':
    unittest.main()
