# (C) 2024 The University of Chicago
# See COPYRIGHT in top-level directory.


import unittest
from .spec import *


class TestConfigSpace(unittest.TestCase):

    def test_mofka_config_space(self):
        space = MofkaServiceSpec.space(
            num_procs=4, num_pools_in_servers=4, num_xstreams=4,
            num_metadata_db_per_proc=8,
            num_data_storage_per_proc=2)

        space = MofkaServiceSpec.space(
            num_procs=2,
            master_db_needs_persistence=False)

        config = space.sample_configuration()

        spec = MofkaServiceSpec.from_config(config=config, address='ofi+tcp')

    def test_benchmark_topic_partition_config_space(self):
        import json
        space = BenchmarkTopicPartitionSpec.space(
            num_pools_in_servers=3,
            num_servers=4)
        #print(space)
        config = space.sample_configuration()
        #print(config)
        config = BenchmarkTopicPartitionSpec.from_config(config=config)
        #print(json.dumps(config, indent=4))

    def test_benchmark_topic_config_space(self):
        import json
        space = BenchmarkTopicSpec.space(
            num_servers=4,
            num_pools_in_servers=2,
            metadata_num_fields=10,
            metadata_key_sizes=8,
            metadata_val_sizes=16
        )
        #print(space)
        config = space.sample_configuration()
        #print(config)
        config = BenchmarkTopicSpec.from_config(config=config)
        #print(json.dumps(config, indent=4))

    def test_benchmark_producer_config_space(self):
        import json
        space = BenchmarkProducerSpec.space(
            num_producers=1,
            num_servers=4,
            num_pools_in_servers=2,
            metadata_num_fields=10,
            metadata_key_sizes=8,
            metadata_val_sizes=16
        )
        #print(space)
        config = space.sample_configuration()
        #print(config)
        config = BenchmarkProducerSpec.from_config(
            config=config,
            num_events=100)
        #print(json.dumps(config, indent=4))

    def test_benchmark_consumer_config_space(self):
        import json
        space = BenchmarkConsumerSpec.space(
            num_consumers=1,
        )
        #print(space)
        config = space.sample_configuration()
        #print(config)
        config = BenchmarkConsumerSpec.from_config(
            config=config, num_events=100)
        #print(json.dumps(config, indent=4))

    def test_benchmark_config_space(self):
        import json
        space = BenchmarkSpec.space(num_servers=2, num_producers=2, num_consumers=1,
                                    num_data_storage_per_proc=(1,2),
                                    num_metadata_db_per_proc=(1,3),
                                    num_pools_in_servers=(2,3), num_partitions=(1,3),
                                    metadata_key_sizes=8, metadata_val_sizes=16,
                                    metadata_num_fields=10)
        #print(space)
        config = space.sample_configuration()
        #print(config)
        config = BenchmarkSpec.from_config(config=config, num_events=100, address='ofi+tcp')
        #print(json.dumps(config, indent=4))

if __name__ == "__main__":
    unittest.main()
