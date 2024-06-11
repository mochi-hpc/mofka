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
        count = thread_pool.thread_count
        self.assertEqual(self.count, count)

class TestValidator(unittest.TestCase):

    def setUp(self):
        self.metadata = dict()
        letters = string.ascii_letters
        key_len = random.randint(8, 64)
        val_len = random.randint(16, 128)
        key = ''.join(random.choice(letters) for i in range(key_len))
        val = ''.join(random.choice(letters) for i in range(val_len))
        self.metadata[key] = val

    def tearDown(self):
        del self.metadata

    def test_create_validator_from_dict(self):
        """Test create validator from metadata"""
        validator = mofka.Validator.from_metadata(self.metadata)

class TestDataDescriptor(unittest.TestCase):

    def setUp(self):
        letters = string.ascii_letters
        val_len = random.randint(16, 128)
        self.location = ''.join(random.choice(letters) for i in range(val_len))
        self.size = random.randint(16, 1024)

    def tearDown(self):
        del self.location
        del self.size

    def test_create_empty_data_descriptor(self):
        """Test create empty data descriptor"""
        mofka.DataDescriptor()

    def test_create_data_descriptor(self):
        """Test create data descriptor"""
        dd = mofka.DataDescriptor(self.location, self.size)
        self.assertEqual(dd.size, self.size)
        self.assertEqual("".join(dd.location), self.location)

    def test_get_size_location(self):
        """Test get size and location"""
        data_descriptor = mofka.DataDescriptor(self.location, self.size)
        size = data_descriptor.size
        location = data_descriptor.location
        self.assertEqual(self.size, size)
        self.assertEqual(self.location, "".join(location))

    def test_make_sub_view(self):
        """Test make sub view of a datadescriptor"""
        data_descriptor = mofka.DataDescriptor(self.location, self.size)
        offset = random.randint(0, self.size)
        size = random.randint(1, self.size - offset)
        ds = data_descriptor.make_sub_view(offset, size)
        self.assertEqual(size, ds.size)
        self.assertEqual(self.location, ''.join(ds.location)) # TODO check if correct

    @unittest.skip("make_stride_view is not yet supported")
    def test_make_stride_view(self):
        """Test make stride view"""
        data_descriptor = mofka.DataDescriptor(self.location, self.size)
        offset = random.randint(0, self.size//2)
        blocksize = random.randint(1, 16)
        numblocks = random.randint(1, (self.size - offset)// blocksize)
        gapsize = random.randint(0, (self.size - offset) // (blocksize*numblocks)  )
        ds = data_descriptor.make_stride_view(offset, numblocks, blocksize, gapsize)
        print(data_descriptor.size, offset, blocksize, numblocks, gapsize, flush=True)
        self.assertEqual(ds.size, numblocks*blocksize)

class TestServiceHandle(unittest.TestCase):

    def setUp(self):
        bedrock_config_file = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "config.json")
        with open(bedrock_config_file) as f:
            self.bedrock_server = BedrockServer("na+sm", config=f.read())
        self.mid = self.bedrock_server.margo.mid
        self.client = mofka.Client(mid=self.mid)
        self.service = self.client.connect("mofka.json")
        self.metadata = dict()
        letters = string.ascii_letters
        key_len = random.randint(8, 64)
        val_len = random.randint(16, 128)
        key = ''.join(random.choice(letters) for i in range(key_len))
        val = ''.join(random.choice(letters) for i in range(val_len))
        self.str_data = ''.join(random.choice(letters) for i in range(random.randint(1024, 2048)))
        self.metadata[key] = val
        self.str_metadata = json.dumps(self.metadata)
        self.location = ''.join(random.choice(letters) for i in range(val_len))
        self.size = random.randint(16, 128)

    def tearDown(self):
        del self.mid
        del self.service
        del self.client
        del self.metadata
        del self.str_data
        del self.str_metadata
        del self.location
        del self.size
        self.bedrock_server.finalize()

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


class TestTopicHandle(unittest.TestCase):

    def setUp(self):
        bedrock_config_file = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "config.json")
        with open(bedrock_config_file) as f:
            self.bedrock_server = BedrockServer("na+sm", config=f.read())
        self.mid = self.bedrock_server.margo.mid
        self.client = mofka.Client(mid=self.mid)
        self.service = self.client.connect("mofka.json")

        self.metadata = dict()
        letters = string.ascii_letters
        key_len = random.randint(8, 64)
        val_len = random.randint(16, 128)
        key = ''.join(random.choice(letters) for i in range(key_len))
        val = ''.join(random.choice(letters) for i in range(val_len))
        self.metadata[key] = val
        self.location = ''.join(random.choice(letters) for i in range(val_len))
        self.size = random.randint(16, 128)

        name = "my_topic"
        validator = mofka.Validator.from_metadata(
            {"__type__":"my_validator:libmy_validator.so"})
        selector = mofka.PartitionSelector.from_metadata(
            {"__type__":"my_partition_selector:libmy_partition_selector.so"})
        serializer = mofka.Serializer.from_metadata(
            {"__type__":"my_serializer:libmy_serializer.so"})
        self.service.create_topic(name, validator, selector, serializer)
        self.topic = self.service.open_topic(name)

    def tearDown(self):
        del self.mid
        del self.service
        del self.client
        del self.topic
        del self.metadata
        del self.location
        del self.size
        self.bedrock_server.finalize()

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
        name = "myconsumer"
        batchsize = random.randint(1,8)
        thread_pool = mofka.ThreadPool(random.randint(1,8))
        data_descriptor = mofka.DataDescriptor(self.location, self.size)
        consumer = self.topic.consumer(name, batchsize, thread_pool, my_broker_selector.broker, my_broker_selector.selector, self.topic.partitions)

class TestProducer(unittest.TestCase):

    def setUp(self):
        # Create and connect a create
        bedrock_config_file = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "config.json")
        with open(bedrock_config_file) as f:
            self.bedrock_server = BedrockServer("na+sm", config=f.read())
        self.mid = self.bedrock_server.margo.mid
        self.client = mofka.Client(mid=self.mid)
        self.service = self.client.connect("mofka.json")

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
        validator = mofka.Validator.from_metadata(
            {"__type__":"my_validator:libmy_validator.so"})
        selector = mofka.PartitionSelector.from_metadata(
            {"__type__":"my_partition_selector:libmy_partition_selector.so"})
        serializer = mofka.Serializer.from_metadata(
            {"__type__":"my_serializer:libmy_serializer.so"})
        self.service.create_topic(name, validator, selector, serializer)
        self.service.add_memory_partition(name, 0)

        # Create a producer
        self.topic = self.service.open_topic(name)
        batchsize = mofka.AdaptiveBatchSize
        thread_pool = mofka.ThreadPool(random.randint(1,10))
        ordering = mofka.Ordering.Strict
        self.producer = self.topic.producer(name, batchsize, thread_pool, ordering)

    def tearDown(self):
        del self.mid
        del self.metadata
        del self.str_data
        del self.service
        del self.client
        del self.topic
        del self.producer
        self.bedrock_server.finalize()

    def test_push_flush_dict_buffer(self):
        data = self.str_data.encode('ascii')
        f = self.producer.push(self.metadata, data)
        f.wait()
        self.producer.flush()


class TestPartitionSelector(unittest.TestCase):

    def setUp(self):
        self.metadata = dict()
        letters = string.ascii_letters
        key_len = random.randint(8, 64)
        val_len = random.randint(16, 128)
        key = ''.join(random.choice(letters) for i in range(key_len))
        val = ''.join(random.choice(letters) for i in range(val_len))
        self.metadata[key] = val

    def tearDown(self):
        del self.metadata

    def test_create_partition_selector_metadata(self):
        """Test create partition selector from metadata"""
        ps = mofka.PartitionSelector.from_metadata(self.metadata)
        # metadata = ps.metadata
        # self.assertEqual(str(metadata), self.str_metadata)

    def test_create_partition_selector_dict(self):
        """Test create partition selector from dict"""
        ps = mofka.PartitionSelector.from_metadata(self.metadata)
        # metadata = ps.metadata
        # self.assertEqual(str(metadata), self.metadata)


if __name__ == '__main__':
    unittest.main()
