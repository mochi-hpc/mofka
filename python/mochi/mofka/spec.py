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
    def space(*, num_procs: int = 1,
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
    def from_config(*, config: 'Configuration', **kwargs):
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
                        'file': kwargs.get('flock_group_file', 'mofka.flock.json'),
                        'group': { 'type': 'static' }
                    })
            for p in proc.providers:
                if p.type == 'yokan' and 'path' in p.config:
                    p.config['path'] = p.config['path'] + '_' + str(uuid.uuid4())[:6]
                if p.type == 'warabi' and 'path' in p.config['target']['config']:
                    path = p.config['target']['config']['path']
                    p.config['target']['config']['path'] = path + '_' + str(uuid.uuid4())[:6]
        return spec


class BenchmarkTopicSpec:

    @staticmethod
    def space(*,
              metadata_num_fields: int|tuple[int,int]|list[int|tuple[int,int]],
              metadata_key_sizes: int|tuple[int,int]|list[int|tuple[int,int]],
              metadata_val_sizes: int|tuple[int,int]|list[int|tuple[int,int]],
              data_num_blocks: int|tuple[int,int]|list[int|tuple[int,int]] = 0,
              data_total_size: int|tuple[int,int]|list[int|tuple[int,int]] = 0,
              validator: list[str] = ['default', 'schema'],
              partition_selector: list[str] = ['default'],
              serializer: list[str] = ['default', 'property_list_serialized'],
              **kwargs):
        from ConfigSpace import ConfigurationSpace, Constant, Categorical
        from mochi.bedrock.spec import _IntegerOrConst, _CategoricalOrConst
        cs = ConfigurationSpace()
        if isinstance(metadata_num_fields, list):
            cs.add(Categorical('metadata.num_fields', metadata_num_fields,
                               default=metadata_num_fields[0]))
        else:
            cs.add(Constant('metadata.num_fields', metadata_num_fields))
        if isinstance(metadata_key_sizes, list):
            cs.add(Categorical('metadata.key_sizes', metadata_key_sizes,
                               default=metadata_key_sizes[0]))
        else:
            cs.add(Constant('metadata.key_sizes', metadata_key_sizes))
        if isinstance(metadata_val_sizes, list):
            cs.add(Categorical('metadata.val_sizes', metadata_val_sizes,
                               default=metadata_val_sizes[0]))
        else:
            cs.add(Constant('metadata.val_sizes', metadata_val_sizes))
        if isinstance(data_num_blocks, list):
            cs.add(Categorical('data.num_blocks', data_num_blocks,
                               default=data_num_blocks[0]))
        else:
            cs.add(Constant('data.num_blocks', data_num_blocks))
        if isinstance(data_total_size, list):
            cs.add(Categorical('data.total_size', data_total_size,
                               default=data_total_size[0]))
        else:
            cs.add(Constant('data.total_size', data_total_size))
        cs.add(_CategoricalOrConst('validator', validator,
                                   default=validator[0]))
        cs.add(_CategoricalOrConst('partition_selector', partition_selector,
                                   default=partition_selector[0]))
        cs.add(_CategoricalOrConst('serialized', serializer,
                                   default=serializer[0]))
        return cs

    @staticmethod
    def from_config(config: 'Configuration', prefix: str = '', **kwargs):
        topic = {'name': kwargs.get('topic_name', 'benchmark')}
        for param in config:
            if not param.startswith(prefix):
                continue
            topic[param[len(prefix):]] = config[param]
        return topic


class BenchmarkProducerSpec:

    @staticmethod
    def space(*,
              num_producers: int,
              producer_batch_size: int|tuple[int,int] = 1,
              producer_adaptive_batch_size: bool|list[bool] = [True, False],
              producer_ordering: str|list[str] = ['loose', 'strict'],
              producer_thread_count: int|tuple[int,int] = 0,
              producer_burst_size_min: int|tuple[int,int] = 1,
              producer_burst_size_max: int|tuple[int,int] = 1,
              producer_wait_between_events_ms_min: int|tuple[int,int] = 0,
              producer_wait_between_events_ms_max: int|tuple[int,int] = 0,
              producer_wait_between_bursts_ms_min: int|tuple[int,int] = 0,
              producer_wait_between_bursts_ms_max: int|tuple[int,int] = 0,
              producer_flush_between_bursts: bool|list[bool] = [True, False],
              producer_flush_every_min: int|tuple[int,int] = 1,
              producer_flush_every_max: int|tuple[int,int] = 1,
              **kwargs):
        from ConfigSpace import Constant, ConfigurationSpace,  EqualsCondition, ForbiddenGreaterThanRelation
        from mochi.bedrock.spec import _IntegerOrConst, _CategoricalOrConst
        cs = ConfigurationSpace()
        cs.add(Constant('count', num_producers))
        cs.add(_IntegerOrConst('batch_size', producer_batch_size))
        cs.add(_CategoricalOrConst('adaptive_batch_size', producer_adaptive_batch_size))
        cs.add(EqualsCondition(cs['batch_size'], cs['adaptive_batch_size'], False))
        cs.add(_CategoricalOrConst('ordering', producer_ordering))
        cs.add(_IntegerOrConst('thread_count', producer_thread_count))
        cs.add(_IntegerOrConst('burst_size_min', producer_burst_size_min))
        cs.add(_IntegerOrConst('burst_size_max', producer_burst_size_max))
        cs.add(ForbiddenGreaterThanRelation(cs['burst_size_min'], cs['burst_size_max']))
        cs.add(_IntegerOrConst('wait_between_events_ms_min', producer_wait_between_events_ms_min))
        cs.add(_IntegerOrConst('wait_between_events_ms_max', producer_wait_between_events_ms_max))
        cs.add(ForbiddenGreaterThanRelation(cs['wait_between_events_ms_min'], cs['wait_between_events_ms_max']))
        cs.add(_IntegerOrConst('wait_between_bursts_ms_min', producer_wait_between_bursts_ms_min))
        cs.add(_IntegerOrConst('wait_between_bursts_ms_max', producer_wait_between_bursts_ms_max))
        cs.add(ForbiddenGreaterThanRelation(cs['wait_between_bursts_ms_min'], cs['wait_between_bursts_ms_max']))
        cs.add(_CategoricalOrConst('flush_between_bursts', producer_flush_between_bursts))
        cs.add(_IntegerOrConst('flush_every_min', producer_flush_every_min))
        cs.add(_IntegerOrConst('flush_every_max', producer_flush_every_max))
        cs.add(ForbiddenGreaterThanRelation(cs['flush_every_min'], cs['flush_every_max']))
        topic_cs = BenchmarkTopicSpec.space(**kwargs)
        cs.add_configuration_space(
            prefix='topic', delimiter='.',
            configuration_space=topic_cs)
        return cs

    @staticmethod
    def from_config(*, config: 'Configuration', num_events: int,
                    rank_offset: int = 0, prefix: str = '', **kwargs):
        topic = BenchmarkTopicSpec.from_config(config, prefix=f'{prefix}topic.', **kwargs)
        def get_from_config(key):
            return config[f'{prefix}{key}']
        producer = {
            'topic': topic,
            'ranks': [rank_offset + r for r in range(int(get_from_config('count')))],
            'num_events': num_events,
            'group_file': kwargs.get('flock_group_file', 'mofka.flock.json'),
            'batch_size': 'adaptive' if bool(get_from_config('adaptive_batch_size')) \
                                     else int(get_from_config('batch_size')),
            'ordering': get_from_config('ordering'),
            'thread_count': int(get_from_config('thread_count')),
            'burst_size': [
                int(get_from_config('burst_size_min')),
                int(get_from_config('burst_size_max'))],
            'wait_between_events_ms': [
                int(get_from_config('wait_between_events_ms_min')),
                int(get_from_config('wait_between_events_ms_max'))],
            'wait_between_bursts_ms': [
                int(get_from_config('wait_between_bursts_ms_min')),
                int(get_from_config('wait_between_bursts_ms_max'))],
            'flush_every': [
                int(get_from_config('flush_every_min')),
                int(get_from_config('flush_every_max'))],
            'flush_between_bursts': bool(get_from_config('flush_between_bursts'))
        }
        return producer


class BenchmarkConsumerSpec:

    @staticmethod
    def space(*,
              num_consumers: int,
              consumer_batch_size: int|tuple[int,int] = 1,
              consumer_adaptive_batch_size: bool|tuple[bool,bool] = [True, False],
              consumer_check_data: bool|list[bool,bool] = [False],
              consumer_thread_count: int|tuple[int,int] = 0,
              consumer_data_selector_selectivity: float|tuple[float,float] = 1.0,
              consumer_data_selector_proportion_min: float|tuple[float,float] = 1.0,
              consumer_data_selector_proportion_max: float|tuple[float,float] = 1.0,
              consumer_data_broker_num_blocks_min: int|tuple[int,int] = 1,
              consumer_data_broker_num_blocks_max: int|tuple[int,int] = 1,
              **kwargs):
        from ConfigSpace import Constant, ConfigurationSpace,  EqualsCondition, ForbiddenGreaterThanRelation
        from mochi.bedrock.spec import _IntegerOrConst, _CategoricalOrConst, _FloatOrConst
        cs = ConfigurationSpace()
        cs.add(Constant('count', num_consumers))
        if num_consumers == 0:
            return cs
        cs.add(_IntegerOrConst('batch_size', consumer_batch_size))
        cs.add(_CategoricalOrConst('adaptive_batch_size', consumer_adaptive_batch_size))
        cs.add(_CategoricalOrConst('check_data', consumer_check_data))
        cs.add(EqualsCondition(cs['batch_size'], cs['adaptive_batch_size'], False))
        cs.add(_IntegerOrConst('thread_count', consumer_thread_count))
        cs.add(_FloatOrConst('data_selector.selectivity', consumer_data_selector_selectivity))
        cs.add(_FloatOrConst('data_selector_proportion_min', consumer_data_selector_proportion_min))
        cs.add(_FloatOrConst('data_selector_proportion_max', consumer_data_selector_proportion_max))
        cs.add(ForbiddenGreaterThanRelation(
            cs['data_selector_proportion_min'], cs['data_selector_proportion_min']))
        cs.add(_IntegerOrConst('data_broker_num_blocks_min', consumer_data_broker_num_blocks_min))
        cs.add(_IntegerOrConst('data_broker_num_blocks_max', consumer_data_broker_num_blocks_min))
        cs.add(ForbiddenGreaterThanRelation(
            cs['data_broker_num_blocks_min'], cs['data_broker_num_blocks_min']))
        return cs

    @staticmethod
    def from_config(*, config: 'Configuration', num_events: int,
                    rank_offset: int = 0, prefix: str = '', **kwargs):
        def get_from_config(key):
            return config[f'{prefix}{key}']
        if int(get_from_config('count')) == 0:
            return {}
        consumer = {
            'ranks': [rank_offset + r for r in range(int(get_from_config('count')))],
            'num_events': num_events,
            'group_file': kwargs.get('flock_group_file', 'mofka.flock.json'),
            'topic_name': kwargs.get('topic_name', 'benchmark'),
            'consumer_name': kwargs.get('consumer_name', 'consumer'),
            'batch_size': 'adaptive' if bool(get_from_config('adaptive_batch_size')) \
                                     else int(get_from_config('batch_size')),
            'check_data': bool(get_from_config('check_data')),
            'thread_count': int(get_from_config('thread_count')),
            'data_selector.selectivity': float(get_from_config('data_selector.selectivity')),
            'data_selector.proportion': [
                int(get_from_config('data_selector_proportion_min')),
                int(get_from_config('data_selector_proportion_max'))],
            'data_broker.num_blocks': [
                int(get_from_config('data_broker_num_blocks_min')),
                int(get_from_config('data_broker_num_blocks_max'))]
            }
        return consumer


class BenchmarkSpec:

    @staticmethod
    def space(*, num_servers: int = 1,
              num_producers: int = 1,
              num_consumers: int = 0,
              num_pools_in_servers: int|tuple[int,int] = 1,
              **kwargs):
        from ConfigSpace import ConfigurationSpace, Constant
        cs = ConfigurationSpace()
        # Mofka service configuration space
        mofka_cs = MofkaServiceSpec.space(
            num_pools=num_pools_in_servers,
            num_procs=num_servers, **kwargs)
        cs.add_configuration_space(
            prefix='servers', delimiter='.',
            configuration_space=mofka_cs)
        # Producers configuration space
        producer_cs = BenchmarkProducerSpec.space(num_producers=num_producers, **kwargs)
        cs.add_configuration_space(
            prefix='producers', delimiter='.',
            configuration_space=producer_cs)
        # Consumers configuration space
        consumer_cs = BenchmarkConsumerSpec.space(num_consumers=num_consumers, **kwargs)
        cs.add_configuration_space(
            prefix='consumers', delimiter='.',
            configuration_space=consumer_cs)
        return cs

    @staticmethod
    def from_config(*, config: 'Configuration',
                    prefix: str = '', **kwargs):
        # servers configuration
        mofka_spec = MofkaServiceSpec.from_config(
            config=config, prefix=f'{prefix}servers.', **kwargs)
        num_servers = len(mofka_spec.processes)
        c = {}
        c['address'] = kwargs['address']
        c['servers'] = {
            'ranks': [ r for r in range(0, num_servers) ],
            'config': mofka_spec.to_dict()['processes']
        }
        for rank, process_config in enumerate(c['servers']['config']):
            process_config['__if__'] = f'$MPI_COMM_WORLD.rank == {rank}'
        # producers configuration
        c['producers'] = BenchmarkProducerSpec.from_config(
            config=config, prefix=f'{prefix}producers.', rank_offset=num_servers, **kwargs)
        num_producers = len(c['producers']['ranks'])
        # consumers configuration
        c['consumers'] = BenchmarkConsumerSpec.from_config(
            config=config, prefix=f'{prefix}consumers.', rank_offset=num_servers+num_producers, **kwargs)
        if len(c['consumers']) == 0:
            del c['consumers']
        # add options
        c['options'] = {
            'simultaneous': kwargs.get('simultaneous_producer_and_consumer', False)
        }
        return c


