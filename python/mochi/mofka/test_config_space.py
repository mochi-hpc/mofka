# (C) 2024 The University of Chicago
# See COPYRIGHT in top-level directory.


import unittest
from .spec import MofkaServiceSpec


class TestConfigSpace(unittest.TestCase):

    def test_mofka_config_space(self):
        space = MofkaServiceSpec.space(
            num_procs=4, num_pools=4, num_xstreams=4,
            num_metadata_db_per_proc=8,
            num_data_storage_per_proc=2)

        space = MofkaServiceSpec.space(
            num_procs=2,
            master_db_needs_persistence=False)
        print(space)

        config = space.sample_configuration()
        print(config)

        spec = MofkaServiceSpec.from_config(config=config, address='ofi+tcp')
        print(spec.to_json(indent=4))


if __name__ == "__main__":
    unittest.main()
