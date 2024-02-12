import os
import sys
import unittest
import json
import string
import random
import subprocess
import tempfile
import time
import numpy as np

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
        self.gid = self.bedrock_server.ssg["mofka_group"].handle

    def tearDown(self):
        del self.mid
        del self.gid
        self.bedrock_server.finalize()

    def test_create_client(self):
        """Test client creation."""
        client = mofka.Client(mid=self.mid)

    def test_connect_client(self):
        """Test connecting a client to a ssg group"""
        client = mofka.Client(mid=self.mid)
        service = client.connect(self.gid)
 
    def test_get_config(self):
        """Test get client config"""
        client = mofka.Client(mid=self.mid)
        client.get_config()

class TestThreadPool(unittest.TestCase):

    def setUp(self):
        self.engine = Engine("na+sm")
        self.count = random.randint(1, 64)

    def tearDown(self):
        del self.count
        self.engine.finalize()

    def test_create_thread_pool(self):
        """Test create thread pool"""
        thread_pool = mofka.ThreadPool(self.count)

    def test_get_thread_count(self):
        """Test get thread count"""
        thread_pool = mofka.ThreadPool(self.count)
        count = thread_pool.thread_count()
        self.assertEqual(self.count, count)

class TestMetadata(unittest.TestCase):

    def setUp(self):
        metadata = dict()
        letters = string.ascii_letters
        for i in range(0,8):
            key_len = random.randint(8, 64)
            val_len = random.randint(16, 128)
            key = ''.join(random.choice(letters) for i in range(key_len))
            val = ''.join(random.choice(letters) for i in range(val_len))
            metadata[key] = val
        self.str_metadata = str(metadata) 
    
    def tearDown(self):
        del self.str_metadata

    def test_create_metadata(self):
        mofka.Metadata(self.str_metadata, validate=True)

    def test_is_valid_json(self):
        metadata = mofka.Metadata(self.str_metadata, validate=True)

class TestData(unittest.TestCase):

    def setUp(self):
        metadata = dict()
        self.list_str_data = list()
        self.list_num_data = list()
        letters = string.ascii_letters
        for i in range(0,8):
            key_len = random.randint(8, 64)
            val_len = random.randint(16, 128)
            key = ''.join(random.choice(letters) for i in range(key_len))
            val = ''.join(random.choice(letters) for i in range(val_len))
            str_data = ''.join(random.choice(letters) for i in range(random.randint(1024, 2048)))
            num_data = np.random.random(random.randint(1024, 2048)).data
            metadata[key] = val
            self.list_str_data.append(str_data.encode('ascii'))
            self.list_num_data.append(num_data)
        self.str_metadata = str(metadata) 
      
    def tearDown(self):
        del self.list_num_data
        del self.list_str_data
        del self.str_metadata

    def test_create_empty_data(self):
        """Test create empty data"""
        mofka.Data()
    
    def test_create_data(self):
        """Test create a data from string"""
        mofka.Data(self.list_str_data[0])

    def test_create_data(self):
        """Test create a data from numerical buffer"""
        mofka.Data(self.list_num_data[0])
    
    def test_create_data(self):
        """Test create data from list of strings"""
        mofka.Data(self.list_str_data)
    
    def test_create_data(self):
        """Test create data from list of arrays"""
        mofka.Data(self.list_num_data)

    def test_get_segments(self):
        """Test get segments"""
        data = mofka.Data(self.list_num_data)
        data.segments
        
    def test_get_size(self):
        """Test get data size"""
        data = mofka.Data(self.list_num_data)
        data.size

class TestValidator(unittest.TestCase):

    def setUp(self):
        metadata = dict()
        letters = string.ascii_letters
        key_len = random.randint(8, 64)
        val_len = random.randint(16, 128)
        key = ''.join(random.choice(letters) for i in range(key_len))
        val = ''.join(random.choice(letters) for i in range(val_len))
        self.str_data = ''.join(random.choice(letters) for i in range(random.randint(1024, 2048)))
        metadata[key] = val
        self.str_metadata = str(metadata)

    def test_create_validator(self):
        "Test create an empty validator"
        mofka.Validator()

    def test_get_metadata(self):
        validator = mofka.Validator()
        validator.metadata
    
    def test_from_metadata(self):
        metadata = mofka.Metadata(self.str_metadata)
        validator = mofka.Validator(metadata)
    
    def test_validate(self):
        """Test validate"""
        validator = mofka.Validator()
        metadata = mofka.Metadata(self.str_metadata)
        data = mofka.Data(self.str_data.encode('ascii'))
        validator.validate(metadata, data)

class TestServiceHandle(unittest.TestCase):

    def setUp(self):
        bedrock_config_file = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "config.json")    
        with open(bedrock_config_file) as f:
            self.bedrock_server = BedrockServer("na+sm", config=f.read())
        self.mid = self.bedrock_server.margo.mid
        self.gid = self.bedrock_server.ssg["mofka_group"].handle
        self.client = mofka.Client(mid=self.mid)
        self.service = self.client.connect(self.gid)
        
    def tearDown(self):
        del self.mid
        del self.gid
        del self.service
        del self.client
        self.bedrock_server.finalize()

    def test_get_servers(self):
        """Test get num servers"""
        self.service.num_servers

    def test_get_client(self):
        """Test get client"""
        self.service.client    

    def test_create_open_topic(self):
        """Test create and open a topic"""
        name = "my_topic"
        validator = mofka.Validator()
        selector = mofka.PartitionSelector()
        serializer = mofka.Serializer()
        self.service.create_topic(name, validator, selector, serializer)
        topic = self.service.open_topic(name)

    def test_add_partition(self):
        """Test add partition"""
        topic_name = "my_topic"
        server_rank = 0
        validator = mofka.Validator()
        selector = mofka.PartitionSelector()
        serializer = mofka.Serializer()
        self.service.create_topic(topic_name, validator, selector, serializer)
        self.service.add_partition(topic_name, server_rank)


class TestTopicHandle(unittest.TestCase):

    def setUp(self):
        bedrock_config_file = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "config.json")    
        with open(bedrock_config_file) as f:
            self.bedrock_server = BedrockServer("na+sm", config=f.read())
        self.mid = self.bedrock_server.margo.mid
        self.gid = self.bedrock_server.ssg["mofka_group"].handle
        self.client = mofka.Client(mid=self.mid)
        self.service = self.client.connect(self.gid)
        name = "my_topic"
        validator = mofka.Validator()
        selector = mofka.PartitionSelector()
        serializer = mofka.Serializer()
        self.service.create_topic(name, validator, selector, serializer)
        self.topic = self.service.open_topic(name)

    def tearDown(self):
        del self.mid
        del self.gid
        del self.service
        del self.client
        del self.topic
        self.bedrock_server.finalize()
    
    def test_create_producer(self):
        """Test create a producer associated with a topic"""
        name = "myproducer"
        batchsize = random.randint(1,8)
        thread_pool = mofka.ThreadPool(random.randint(1,8))
        ordering = mofka.Ordering.Strict
        producer = self.topic.producer(name, batchsize, thread_pool, ordering)

class TestProducer(unittest.TestCase):
    def setUp(self):
        bedrock_config_file = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "config.json")    
        with open(bedrock_config_file) as f:
            self.bedrock_server = BedrockServer("na+sm", config=f.read())
        self.mid = self.bedrock_server.margo.mid
        self.gid = self.bedrock_server.ssg["mofka_group"].handle
        self.client = mofka.Client(mid=self.mid)
        self.service = self.client.connect(self.gid)
        name = "my_topic"
        validator = mofka.Validator()
        selector = mofka.PartitionSelector()
        serializer = mofka.Serializer()
        self.service.create_topic(name, validator, selector, serializer)
        self.topic = self.service.open_topic(name)   
        batchsize = random.randint(1,10)
        thread_pool = mofka.ThreadPool(random.randint(1,10))
        ordering = mofka.Ordering.Strict
        self.producer = self.topic.producer(name, batchsize, thread_pool, ordering)

        metadata = dict()
        letters = string.ascii_letters
        key_len = random.randint(8, 64)
        val_len = random.randint(16, 128)
        key = ''.join(random.choice(letters) for i in range(key_len))
        val = ''.join(random.choice(letters) for i in range(val_len))
        self.str_data = ''.join(random.choice(letters) for i in range(random.randint(1024, 2048)))
        metadata[key] = val
        self.str_metadata = str(metadata) 

    def tearDown(self):
        del self.mid
        del self.gid
        del self.service
        del self.client
        del self.topic
        del self.producer
        self.bedrock_server.finalize()

    def test_push_flush(self):
        data = mofka.Data(self.str_data.encode('ascii'))
        metadata = mofka.Metadata(self.str_metadata)
        self.producer.push(metadata, data)
        self.producer.flush()

if __name__ == '__main__':
    unittest.main()
