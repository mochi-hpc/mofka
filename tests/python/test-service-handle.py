import os
import sys
import unittest
import json
import string
import random

from mochi.bedrock.server import Server as BedrockServer
import mochi.mofka.client as mofka
import my_broker_selector


class TestServiceHandle(unittest.TestCase):

    def setUp(self):
        bedrock_config_file = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "config.json")
        with open(bedrock_config_file) as f:
            self.bedrock_server = BedrockServer("na+sm", config=f.read())
        self.client = mofka.Client(self.bedrock_server.margo.mid)
        self.service = self.client.connect("mofka.json")

    def tearDown(self):
        del self.client
        del self.service
        self.bedrock_server.finalize()
        del self.bedrock_server

    def test_get_servers(self):
        """Test get num servers"""
        n = self.service.num_servers

    def test_get_client(self):
        """Test get client"""
        self.service.client

    def test_create_open_topic(self):
        """Test create and open a topic"""
        name = "my_topic"
        validator = mofka.Validator.from_metadata(
            {"__type__":"my_validator:libmy_validator.so"})
        selector = mofka.PartitionSelector.from_metadata(
            {"__type__":"my_partition_selector:libmy_partition_selector.so"})
        serializer = mofka.Serializer.from_metadata(
            {"__type__":"my_serializer:libmy_serializer.so"})
        self.service.create_topic(name, validator, selector, serializer)
        topic = self.service.open_topic(name)

    def test_add_memory_partition(self):
        """Test add partition"""
        topic_name = "my_topic"
        validator = mofka.Validator.from_metadata(
            {"__type__":"my_validator:libmy_validator.so"})
        selector = mofka.PartitionSelector.from_metadata(
            {"__type__":"my_partition_selector:libmy_partition_selector.so"})
        serializer = mofka.Serializer.from_metadata(
            {"__type__":"my_serializer:libmy_serializer.so"})
        server_rank = 0
        self.service.create_topic(topic_name, validator, selector, serializer)
        self.service.add_memory_partition(topic_name, server_rank)


if __name__ == '__main__':
    unittest.main()
