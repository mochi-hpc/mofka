import os
import sys
import unittest
import json
import string
import random
import uuid

from mochi.bedrock.server import Server as BedrockServer
import mochi.mofka.client as mofka



class TestTopicHandle(unittest.TestCase):

    def setUp(self):
        with open("kafka.json", "w+") as f:
            f.write(json.dumps({"bootstrap.servers": "localhost:9092"}))
        self.service = mofka.KafkaDriver("kafka.json")
        name = "my_topic_" + str(uuid.uuid4())
        self.service.create_topic(name)
        import time
        time.sleep(2)
        self.topic = self.service.open_topic(name)

    def tearDown(self):
        del self.service
        del self.topic
        os.remove("kafka.json")

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
        import my_broker_selector
        name = "myconsumer"
        batchsize = random.randint(1,8)
        consumer = self.topic.consumer(
                name=name, batch_size=batchsize,
                data_broker=my_broker_selector.broker,
                data_selector=my_broker_selector.selector,
                targets=list(range(len(self.topic.partitions))))


if __name__ == '__main__':
    unittest.main()
