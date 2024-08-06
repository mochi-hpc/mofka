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
import my_broker_selector

class TestClient(unittest.TestCase):

    def setUp(self):
        bedrock_config_file = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "config.json")
        with open(bedrock_config_file) as f:
            self.bedrock_server = BedrockServer("na+sm", config=f.read())
        self.mid = self.bedrock_server.margo.mid

    def tearDown(self):
        del self.mid
        self.bedrock_server.finalize()

    def test_create_client(self):
        """Test client creation."""
        client = mofka.Client(mid=self.mid)

    def test_connect_client(self):
        """Test connecting a client to a group"""
        client = mofka.Client(mid=self.mid)
        service = client.connect("mofka.json")

    def test_get_config(self):
        """Test get client config"""
        client = mofka.Client(mid=self.mid)
        config = client.config

    # TODO check/edit config


if __name__ == '__main__':
    unittest.main()
