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
        config = client.get_config()
    
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
        count = thread_pool.thread_count()
        self.assertEqual(self.count, count)

class TestValidator(unittest.TestCase):

    def setUp(self):
        self.metadata = dict()
        letters = string.ascii_letters
        key_len = random.randint(8, 64)
        val_len = random.randint(16, 128)
        key = ''.join(random.choice(letters) for i in range(key_len))
        val = ''.join(random.choice(letters) for i in range(val_len))
        self.str_data = ''.join(random.choice(letters) for i in range(random.randint(1024, 2048)))
        self.metadata[key] = val
        self.str_metadata = json.dumps(self.metadata)

    def test_create_validator(self):
        """Test create an empty validator"""
        mofka.Validator()

    def test_get_metadata(self):
        """Test get validator metadata"""
        validator = mofka.Validator()
        m = validator.metadata
        self.assertEqual(str(m), '{"type":"default"}')

    def test_create_validator_from_metadata(self):
        """Test create validator from metadata"""
        metadata = mofka.Metadata(self.str_metadata)
        validator = mofka.Validator(metadata)
        m = validator.metadata
        self.assertEqual(str(m), self.str_metadata)
    
    def test_create_validator_from_dict(self):
        """Test create validator from metadata"""
        validator = mofka.Validator(self.metadata)
        m = validator.metadata
        self.assertEqual(str(m), self.str_metadata)

    def test_validate(self):
        """Test validate"""
        validator = mofka.Validator()
        validator.validate(self.metadata, self.str_data.encode('ascii'))

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
        self.str_metadata = json.dumps(metadata) 
    
    def tearDown(self):
        del self.str_metadata

    def test_create_metadata_from_str(self):
        """Test create metadata from str"""
        m = mofka.Metadata(self.str_metadata, validate=True)

    def test_is_valid_json(self):
        """Test metadata is valid json"""
        metadata = mofka.Metadata(self.str_metadata, validate=True)
        b = metadata.is_valid_json()
        self.assertEqual(b, True)

    # TODO test non valid metadata

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
        self.str_metadata = json.dumps(metadata) 
        self.location = ''.join(random.choice(letters) for i in range(val_len))
        self.size = random.randint(16, 128)

    def tearDown(self):
        del self.list_num_data
        del self.list_str_data
        del self.str_metadata

    def test_create_empty_data(self):
        """Test create empty data"""
        mofka.Data()
    
    def test_create_data_string(self):
        """Test create a data from string"""
        data = mofka.Data(self.list_str_data[0])

    def test_create_data_num(self):
        """Test create a data from numerical buffer"""
        mofka.Data(self.list_num_data[0])
    
    def test_create_data_list_str(self):
        """Test create data from list of strings"""
        mofka.Data(self.list_str_data)
    
    def test_create_data_list_num(self):
        """Test create data from list of arrays"""
        mofka.Data(self.list_num_data)

    def test_get_segments(self):
        """Test get segments"""
        data = mofka.Data(self.list_num_data)
        data.segments
        
    def test_get_size(self):
        """Test get data size"""
        data = mofka.Data(self.list_num_data)
        data_size = data.size
        numsize = 0
        for i in self.list_num_data: numsize =numsize + i.nbytes/i.itemsize
        self.assertEqual(data_size, numsize)

    def test_create_data_broker(self):
        """Test create a data broker"""
        data_descriptor = mofka.DataDescriptor(self.location, self.size)
        metadata = mofka.Metadata(self.str_metadata)
        broker = lambda metadata, data_descriptor : mofka.Data(self.list_str_data) 
        broker = broker(metadata, data_descriptor)

    def test_create_data_selector(self):
        """Test create a data selector"""
        data_descriptor = mofka.DataDescriptor(self.location, self.size)
        metadata = mofka.Metadata(self.str_metadata)
        def selector(metadata, data_descriptor):
            choice = random.choice([True, False])
            if choice:
                return data_descriptor
            else:
                return mofka.DataDescriptor()
        selector = selector(metadata, data_descriptor)

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

    def test_make_stride_view(self):
        """Test make stride view"""
        data_descriptor = mofka.DataDescriptor(self.location, 25) #self.size)
        offset = 0 #random.randint(0, self.size//2)
        blocksize =  10 #random.randint(1, 16)
        numblocks = 1 # random.randint(1, (self.size - offset)// blocksize)
        gapsize = 0 # random.randint(0, (self.size - offset) // (blocksize*numblocks)  )
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
        n = self.service.num_servers

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
        
        metadata = dict()
        letters = string.ascii_letters
        key_len = random.randint(8, 64)
        val_len = random.randint(16, 128)
        key = ''.join(random.choice(letters) for i in range(key_len))
        val = ''.join(random.choice(letters) for i in range(val_len))
        self.str_data = ''.join(random.choice(letters) for i in range(random.randint(1024, 2048)))
        metadata[key] = val
        self.str_metadata = json.dumps(metadata) 
        self.location = ''.join(random.choice(letters) for i in range(val_len))
        self.size = random.randint(16, 128)

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
        ordering = "Strict"
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

        def selector(metadata, data_descriptor):
            choice = random.choice([True, False])
            if choice:
                return data_descriptor
            else:
                return mofka.DataDescriptor()
                
        def broker(metadata, data_descriptor):
            return mofka.Data()

        name = "myconsumer"
        batchsize = random.randint(1,8)
        thread_pool = mofka.ThreadPool(random.randint(1,8))
        data_descriptor = mofka.DataDescriptor(self.location, self.size)
        consumer = self.topic.consumer(name, batchsize, thread_pool, broker, selector, self.topic.partitions)

    def test_create_consumer_2(self):
        """Test Create a consumer associated with a topic"""

        def selector(metadata = dict(), data_descriptor = DataDescriptor()):
            choice = random.choice([True, False])
            if choice:
                return data_descriptor
            else:
                return mofka.DataDescriptor()
                
        def broker(metadata = dict(), data_descriptor = DataDescriptor() ):
            return np.random.random(random.randint(1024, 2048)).data

        name = "myconsumer"
        batchsize = random.randint(1,8)
        thread_pool = mofka.ThreadPool(random.randint(1,8))
        data_descriptor = mofka.DataDescriptor(self.location, self.size)
        consumer = self.topic.consumer(name, batchsize, thread_pool, broker, selector, self.topic.partitions)


class TestProducer(unittest.TestCase):

    def setUp(self):
        self.metadata = dict()
        letters = string.ascii_letters
        key_len = random.randint(8, 64)
        val_len = random.randint(16, 128)
        key = ''.join(random.choice(letters) for i in range(key_len))
        val = ''.join(random.choice(letters) for i in range(val_len))
        self.str_data = ''.join(random.choice(letters) for i in range(random.randint(1024, 2048)))
        self.metadata[key] = val
        self.str_metadata = json.dumps(self.metadata) 

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
        ordering = "Strict"
        self.producer = self.topic.producer(name, batchsize, thread_pool, ordering)

    def tearDown(self):
        del self.mid
        del self.gid
        del self.service
        del self.client
        del self.topic
        del self.producer
        self.bedrock_server.finalize()

    def test_push_flush_metadata_data(self):
        data = mofka.Data(self.str_data.encode('ascii'))
        metadata = mofka.Metadata(self.str_metadata, validate=True)
        f = self.producer.push(metadata, data)
        f.wait()
        self.producer.flush()

    def test_push_flush_dict_buffer(self):
        data = self.str_data.encode('ascii')
        f = self.producer.push(self.metadata, data)
        f.wait() # TODO generates RuntimeError: PartitionSelector has no target to select from
        self.producer.flush()

class TestConsumer(unittest.TestCase):
    
    def setUp(self):
        # create data and metadata
        self.metadata = dict()
        letters = string.ascii_letters
        key_len = random.randint(8, 64)
        val_len = random.randint(16, 128)
        key = ''.join(random.choice(letters) for i in range(key_len))
        val = ''.join(random.choice(letters) for i in range(val_len))
        self.str_data = ''.join(random.choice(letters) for i in range(random.randint(1024, 2048)))
        self.metadata[key] = val
        self.str_metadata = json.dumps(self.metadata)

        # Create and connect a create
        bedrock_config_file = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "config.json")    
        with open(bedrock_config_file) as f:
            self.bedrock_server = BedrockServer("na+sm", config=f.read())
        self.mid = self.bedrock_server.margo.mid
        self.gid = self.bedrock_server.ssg["mofka_group"].handle
        self.client = mofka.Client(mid=self.mid)
        self.service = self.client.connect(self.gid)
        
        # create a topic
        name = "my_topic"
        validator = mofka.Validator()
        selector = mofka.PartitionSelector()
        serializer = mofka.Serializer()
        self.service.create_topic(name, validator, selector, serializer)
        self.topic = self.service.open_topic(name)   

        # Create a producer 
        name = "myproducer"
        batchsize = random.randint(1,8)
        thread_pool = mofka.ThreadPool(random.randint(1,8))
        ordering = "Strict"
        self.producer = self.topic.producer(name, batchsize, thread_pool, ordering)
        data = mofka.Data(self.str_data.encode('ascii'))
        metadata = mofka.Metadata(self.str_metadata)
        f = self.producer.push(metadata, data)
        #f = self.producer.push(self.metadata, self.str_data.encode('ascii'))
        self.producer.flush()

        # Create a consumer
        def selector(metadata, data_descriptor):
            choice = random.choice([True, False])
            if choice:
                return data_descriptor
            else:
                return mofka.DataDescriptor()
                
        def broker(metadata, data_descriptor):
            return mofka.Data(data_descriptor.location.data , descriptor.size)
        name = "myconsumer"
        location = ''.join(random.choice(letters) for i in range(val_len))
        data_descriptor = mofka.DataDescriptor(location, val_len)
        self.consumer = self.topic.consumer(name, batchsize, thread_pool, broker, selector, self.topic.partitions)


    def tearDown(self):
        del self.mid
        del self.gid
        del self.service
        del self.client
        del self.topic
        del self.consumer
        del self.producer
        del self.str_metadata
        del self.str_data
        self.bedrock_server.finalize()

    def test_get_name(self):
        """Test consumer get name"""
        name = self.consumer.name
        self.assertEqual(name, "myconsumer")
    
    def test_get_batchsize(self):
        """Test get consumer batchsize"""
        b = self.consumer.batchsize
        # TODO 

    def test_get_data_broker(self):
        """Test get data broker"""
        db = self.consumer.data_broker
      

    def test_get_topic(self):
        """Test get topic"""
        topic = self.consumer.topic

    def test_get_data_selector(self):
        """Test get data selector"""
        dataselector = self.consumer.data_selector

    def test_get_thread_pool(self):
        """Test get threadpool"""
        pool = self.consumer.thread_pool

    # def test_pull_process(self):
    #     f = self.consumer.pull()
    #     event = f.wait()
    #     data = event.data
    #     print("data", data, flush=True)

class TestPartitionSelector(unittest.TestCase):

    def setUp(self):
        self.metadata = dict()
        letters = string.ascii_letters
        key_len = random.randint(8, 64)
        val_len = random.randint(16, 128)
        key = ''.join(random.choice(letters) for i in range(key_len))
        val = ''.join(random.choice(letters) for i in range(val_len))
        self.metadata[key] = val
        self.str_metadata = json.dumps(self.metadata) 

    def tearDown(self):
        del self.metadata
        del self.str_metadata

    def test_create_default_partition_selector(self):
        """Test create a partition Selector"""
        ps = mofka.PartitionSelector()
        metadata = ps.metadata
        self.assertEqual(str(metadata), '{"type":"default"}')

    def test_create_partition_selector_metadata(self):
        """Test create partition selector from metadata"""
        ps = mofka.PartitionSelector(mofka.Metadata(self.str_metadata))
        metadata = ps.metadata
        self.assertEqual(str(metadata), self.str_metadata)
        
    def test_create_partition_selector_dict(self):
        """Test create partition selector from dict"""
        ps = mofka.PartitionSelector(self.metadata)
        metadata = ps.metadata
        self.assertEqual(str(metadata), self.metadata)
        
if __name__ == '__main__':
    unittest.main()
