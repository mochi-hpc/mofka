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
        bedrock_config_file = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "config.json")
        with open(bedrock_config_file) as f:
            self.bedrock_server = BedrockServer("na+sm", config=f.read())
        self.service = mofka.MofkaDriver("mofka.json", self.bedrock_server.margo.mid)

    def tearDown(self):
        del self.service
        self.bedrock_server.finalize()
        del self.bedrock_server

    def test_get_servers(self):
        """Test get num servers"""
        n = self.service.num_servers

    def test_create_open_topic(self):
        """Test create and open a topic"""
        name = "my_topic"
        validator = mofka.Validator.from_metadata("my_validator:libmy_validator.so")
        selector = mofka.PartitionSelector.from_metadata("my_partition_selector:libmy_partition_selector.so")
        serializer = mofka.Serializer.from_metadata("my_serializer:libmy_serializer.so")
        self.service.create_topic(name, validator, selector, serializer)
        topic = self.service.open_topic(name)

    def test_add_memory_partition(self):
        """Test add memory partition"""
        topic_name = "my_topic"
        validator = mofka.Validator.from_metadata("my_validator:libmy_validator.so")
        selector = mofka.PartitionSelector.from_metadata("my_partition_selector:libmy_partition_selector.so")
        serializer = mofka.Serializer.from_metadata("my_serializer:libmy_serializer.so")
        server_rank = 0
        self.service.create_topic(topic_name, validator, selector, serializer)
        self.service.add_memory_partition(topic_name, server_rank)
        topic = self.service.open_topic(topic_name)

    def test_add_default_partition(self):
        """Test add default partition"""
        topic_name = "my_topic"
        validator = mofka.Validator.from_metadata("my_validator:libmy_validator.so")
        selector = mofka.PartitionSelector.from_metadata("my_partition_selector:libmy_partition_selector.so")
        serializer = mofka.Serializer.from_metadata("my_serializer:libmy_serializer.so")
        server_rank = 0
        self.service.create_topic(topic_name, validator, selector, serializer)
        metadata_provider = self.service.add_default_metadata_provider(server_rank)
        data_provider = self.service.add_default_data_provider(server_rank)
        self.service.add_default_partition(topic_name, server_rank,
                                           metadata_provider=metadata_provider,
                                           data_provider=data_provider)
        topic = self.service.open_topic(topic_name)


if __name__ == '__main__':
    unittest.main()
