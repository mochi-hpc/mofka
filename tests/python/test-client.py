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
        self.mid = self.bedrock_server.margo.mid

    def tearDown(self):
        self.bedrock_server.finalize()

    def test_create_client(self):
        """Test client creation."""
        client = mofka.Client(mid=self.mid)
    """
    def test_connect_client(self):
        client = mofka.Client(mid=self.mid)
        ssg_file = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "mofka.ssg")
        service = client.connect(ssg_file)
    """    
    def test_get_config(self):
        client = mofka.Client(mid=self.mid)
        client.get_config()

class TestThreadPool(unittest.TestCase):
    def test_create_thread_pool(self):
        self.count = random.randint(1, 64)
        self.thread_pool = mofka.ThreadPool(self.count)
        print(self.count)
        print(self.thread_pool)

    def test_get_thread_count(self):
        count = self.thread_pool.thread_count()

        

if __name__ == '__main__':
    unittest.main()
