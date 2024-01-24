import os
import sys
import unittest
import json
import string
import random
import subprocess
import tempfile
import time

wd = os.getcwd()
sys.path.append(wd + "/../python")

from pymargo.core import Engine
from mochi.bedrock.server import Server as BedrockServer
import pymofka_client as mofka

class TestClient(unittest.TestCase):

    def setUp(self):
        bedrock_config_file = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "config.json")
        with open(bedrock_config_file) as f:
            self.bedrock_server = BedrockServer("na+sm", config=f.read())
        self.mid = self.bedrock_server.margo_instance_id

    def tearDown(self):
        self.bedrock_server.finalize()

    def test_create_client(self):
        """Test client creation."""
        client = mofka.Client(mid=self.mid)


if __name__ == '__main__':
    unittest.main()
