import os
import sys
import unittest
import json
import string
import random
import time
import random

from mochi.bedrock.server import Server as BedrockServer
import mochi.mofka.client as mofka


class TestProducer(unittest.TestCase):

    def setUp(self):
        # Create and connect a create
        with open("kafka.json", "w+") as f:
            f.write(json.dumps({"bootstrap.servers": "localhost:9092"}))
        self.service = mofka.KafkaDriver("kafka.json")

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
        self.service.create_topic(name)

        # Create a producer
        self.topic = self.service.open_topic(name)
        batchsize = mofka.AdaptiveBatchSize
        thread_pool = mofka.ThreadPool(0)
        ordering = mofka.Ordering.Strict
        self.producer = self.topic.producer(batch_size=batchsize, thread_pool=thread_pool, ordering=ordering)

    def tearDown(self):
        del self.metadata
        del self.str_data
        del self.service
        del self.topic
        del self.producer
        os.remove("kafka.json")

    def test_push_flush_dict_buffer(self):
        data = self.str_data.encode('ascii')
        f = self.producer.push(self.metadata, data)
        f.wait()
        self.producer.flush()


if __name__ == '__main__':
    unittest.main()
