# (C) 2024 The University of Chicago
# See COPYRIGHT in top-level directory.


"""
.. module:: spec
   :synopsis: This package provides the configuration for a Mofka service
   and the corresponding ConfigurationSpace.

.. moduleauthor:: Matthieu Dorier <mdorier@anl.gov>


"""


from mochi.bedrock.spec import ProviderSpec, ProcSpec, ServiceSpec
from mochi.yokan.spec import YokanProviderSpec
from mochi.warabi.spec import WarabiProviderSpec


class MofkaServiceSpec(ServiceSpec):

    @staticmethod
    def space(num_procs: int = 1,
              num_metadata_db_per_proc: int|tuple[int,int] = 1,
              num_data_storage_per_proc: int|tuple[int,int] = 1,
              master_db_path_prefixes: list[str] = ['/tmp/mofka'],
              metadata_db_path_prefixes: list[str] = ['/tmp/mofka'],
              data_storage_path_prefixes: list[str] = ['/tmp/mofka'],
              master_db_needs_persistence: bool = False,
              metadata_db_needs_persistence: bool = False,
              data_storage_needs_persistence: bool = False,
              num_pools: int|tuple[int,int] = 1,
              **kwargs):
        import copy
        max_num_pools = num_pools if isinstance(num_pools, int) else num_pools[1]
        # Master database provider
        master_db_cs = YokanProviderSpec.space(
            paths=[f'{p}/master' for p in master_db_path_prefixes],
            need_sorted_db=True, need_values=True,
            need_persistence=master_db_needs_persistence,
            tags=['mofka:master'],
            max_num_pools=max_num_pools)
        # Metadata database provider
        metadata_db_cs = YokanProviderSpec.space(
            paths=[f'{p}/metadata' for p in metadata_db_path_prefixes],
            need_sorted_db=True, need_values=True,
            need_persistence=metadata_db_needs_persistence,
            tags=['mofka:metadata'],
            max_num_pools=max_num_pools)
        # Data storage provider
        data_storage_cs = WarabiProviderSpec.space(
            paths=[f'{p}/data' for p in data_storage_path_prefixes],
            need_persistence=data_storage_needs_persistence,
            tags=['mofka:data'],
            max_num_pools=max_num_pools)
        provider_space_factories = [
            {
                'family': 'master',
                'space' : master_db_cs,
                'count' : 1
            },
            {
                'family': 'metadata',
                'space' : metadata_db_cs,
                'count' : num_metadata_db_per_proc
            },
            {
                'family': 'data',
                'space' : data_storage_cs,
                'count' : num_data_storage_per_proc
            }
        ]
        # Process containing the master database
        main_proc_cs = ProcSpec.space(
            num_pools=num_pools,
            provider_space_factories=provider_space_factories,
            **kwargs)
        # Processes not containing the master database
        del provider_space_factories[0] # delete master database provider
        secondary_proc_cs = ProcSpec.space(
            num_pools=num_pools,
            provider_space_factories=provider_space_factories,
            **kwargs)
        # Service configuration space
        process_space_factories = [
            {
                'family': 'main',
                'space' : main_proc_cs,
                'count' : 1
            },
            {
                'family': 'secondary',
                'space' : secondary_proc_cs,
                'count' : num_procs-1
            }
        ]
        mofka_cs = ServiceSpec.space(process_space_factories=process_space_factories)
        return mofka_cs

    @staticmethod
    def from_config(config: 'Configuration', **kwargs):
        import uuid
        spec = ServiceSpec.from_config(config=config, **kwargs)
        # Add the libraries and the Flock providers
        # Also because processes may share the same node, we will add
        # a prefix to all the paths
        for proc in spec.processes:
            proc.libraries['yokan']  = 'libyokan-bedrock-module.so'
            proc.libraries['warabi'] = 'libwarabi-bedrock-module.so'
            proc.libraries['flock']  = 'libflock-bedrock-module.so'
            proc.libraries['mofka']  = 'libmofka-bedrock-module.so'
            proc.providers.add(
                    name='group', type='flock',
                    pool=proc.margo.argobots.pools[0],
                    provider_id=len(proc.providers)+1,
                    config={
                        'bootstrap': 'mpi',
                        'file': 'mofka.flock.json',
                        'group': { 'type': 'static' }
                    })
            for p in proc.providers:
                if p.type == 'yokan' and 'path' in p.config:
                    p.config['path'] = p.config['path'] + '_' + str(uuid.uuid4())[:6]
                if p.type == 'warabi' and 'path' in p.config['target']['config']:
                    path = p.config['target']['config']['path']
                    p.config['target']['config']['path'] = path + '_' + str(uuid.uuid4())[:6]
        return spec
