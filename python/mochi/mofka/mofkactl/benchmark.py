import typer
from typing_extensions import Annotated
from ..spec import BenchmarkSpec


app = typer.Typer()


def _convert_range(ctx: typer.Context, param: typer.CallbackParam, value: str):
    try:
        if "," in value:
            result = tuple(int(x) for x in value.split(","))
            if len(result) != 2 or result[0] > result[1]:
                raise typer.BadParameter(f"Invalid range for {param.name}: {value}")
        else:
            result = int(value)
    except ValueError:
        raise typer.BadParameter(f"Invalid format for {param.name}: {value} (expected int or int,int)")
    return result


def _convert_float_range(ctx: typer.Context, param: typer.CallbackParam, value: str):
    try:
        if "," in value:
            result = tuple(float(x) for x in value.split(","))
            if len(result) != 2 or result[0] > result[1]:
                raise typer.BadParameter(f"Invalid range for {param.name}: {value}")
        else:
            result = float(value)
    except ValueError:
        raise typer.BadParameter(f"Invalid format for {param.name}: {value} (expected float or float,float)")
    return result


def _convert_list_of_ranges(ctx: typer.Context, param: typer.CallbackParam, value: str):
    ranges = value.split(";")
    for i, v in enumerate(ranges):
        try:
            if "," in v:
                r = tuple(int(x) for x in v.split(","))
                if len(r) != 2 or r[0] > r[1]:
                    raise typer.BadParameter(f"Invalid range in {param.name}: {v}")
            else:
                r = int(v)
            ranges[i] = r
        except ValueError:
            raise typer.BadParameter(
                f"Invalid format for {param.name}: {value} (expected int,int;int,int...)")
    return ranges


def _split_argument(ctx: typer.Context, param: typer.CallbackParam, value: str):
    return value.split(",")


def _convert_list_of_booleans(ctx: typer.Context, param: typer.CallbackParam, value: str):
    values = value.split(",")
    for i, v in enumerate(values):
        if v == "true":
            values[i] = True
        elif v == "false":
            values[i] = False
        else:
            raise typer.BadParameter(
                f"Invalid format for {param.name}: {value} (expected true,false)")
    return values


@app.command()
def generate(
        ctx: typer.Context,
        # General
        address: Annotated[
            str, typer.Option("-a", "--address")],
        num_events: Annotated[
            int, typer.Option("-n", "--num-events")],
        # Mofka service
        num_servers: Annotated[
            int, typer.Option("--num-servers",
                              help="Number of servers")] = 1,
        num_metadata_db_per_proc: Annotated[
            str, typer.Option("--num-metadata-db-per-proc",
                              callback=_convert_range,
                              help="Number of metadata databases per server")] = "1",
        num_data_storage_per_proc: Annotated[
            str, typer.Option("--num-data-storage-per-proc",
                              callback=_convert_range,
                              help="Number of data storage targets per server")] = "1",
        master_db_path_prefixes: Annotated[
            str, typer.Option("--master-db-path-prefixes",
                              help="Prefixes for the master database paths")] = "/tmp/mofka-benchmark",
        metadata_db_path_prefixes: Annotated[
            str, typer.Option("--metadata-db-path-prefixes",
                              help="Prefixes for the metadata database paths")] = "/tmp/mofka-benchmark",
        data_storage_path_prefixes: Annotated[
            str, typer.Option("--data-storage-path-prefixes",
                              help="Prefixes for the data storage paths")] = "/tmp/mofka-benchmark",
        master_db_needs_persistence: Annotated[
            bool, typer.Option("--master-db-needs-persistence",
                               help="Whether the master database needs persistence")] = False,
        metadata_db_needs_persistence: Annotated[
            bool, typer.Option("--metadata-db-needs-persistence",
                               help="Whether the metadata databases need persistence")] = False,
        data_storage_needs_persistence: Annotated[
            bool, typer.Option("--data-storage-needs-persistence",
                               help="Whether the data storage targets need persistence")] = False,
        num_pools_in_servers: Annotated[
            str, typer.Option("--num-pools-in-servers",
                              callback=_convert_range,
                              help="Number of pools in each server")] = "1",
        num_xstreams_in_servers: Annotated[
            str, typer.Option("--num-xstreams-in-servers",
                              callback=_convert_range,
                              help="Number of ES in each server")] = "1",
        allow_more_pools_than_xstreams: Annotated[
            bool, typer.Option("--allow-more-pools-than-xstreams",
                               help="Allow more pools than ES in a server")] = False,
        # Topic
        num_partitions: Annotated[
            str, typer.Option("--num-partitions",
                              callback=_convert_range,
                              help="Number of partitions")] = "1",
        metadata_num_fields: Annotated[
            str, typer.Option("--metadata-num-fields",
                              callback=_convert_range,
                              help="Number of fields in events metadata")] = "8",
        metadata_key_sizes: Annotated[
            str, typer.Option("--metadata-key-sizes",
                              callback=_convert_list_of_ranges,
                              help="Size of the metadata keys")] = "8",
        metadata_val_sizes: Annotated[
            str, typer.Option("--metadata-val-sizes",
                              callback=_convert_list_of_ranges,
                              help="Size of the metadata values")] = "16",
        data_num_blocks: Annotated[
            str, typer.Option("--data-num-blocks",
                              callback=_convert_list_of_ranges,
                              help="Number of data blocks in events")] = "0",
        data_total_size: Annotated[
            str, typer.Option("--data-total-size",
                              callback=_convert_list_of_ranges,
                              help="Size of the data in each event")] = "0",
        validator: Annotated[
            str, typer.Option("--validator",
                              callback=_split_argument,
                              help="List of possible validators")] = "default",
        partition_selector: Annotated[
            str, typer.Option("--partition-selector",
                              callback=_split_argument,
                              help="List of possible partition selectors")] = "default",
        serializer: Annotated[
            str, typer.Option("--serializer",
                              callback=_split_argument,
                              help="List of possible serializers")] = "default",
        # Producers
        num_producers: Annotated[
            int, typer.Option("--num-producers",
                              help="Number of producers")] = 1,
        producer_batch_size: Annotated[
            str, typer.Option("--producer-batch-size",
                              callback=_convert_range,
                              help="Batch size to use for the producer")] = "-1",
        producer_adaptive_batch_size: Annotated[
            str, typer.Option("--producer-adaptive-batch-size",
                              callback=_convert_list_of_booleans,
                              help="Whether to use adaptive batch size in producer")] = "true",
        producer_ordering: Annotated[
            str, typer.Option("--producer-ordering",
                              callback=_split_argument,
                              help="Allowed event ordering")] = "loose",
        producer_thread_count: Annotated[
            str, typer.Option("--producer-thread-count",
                              callback=_convert_range,
                              help="Number of threads for the producer")] = "0",
        producer_burst_size_min: Annotated[
            str, typer.Option("--producer-burst-size-min",
                              callback=_convert_range,
                              help="Minimum number of events in a burst")] = "1",
        producer_burst_size_max: Annotated[
            str, typer.Option("--producer-burst-size-max",
                              callback=_convert_range,
                              help="Maximum number of events in a burst")] = "1",
        producer_wait_between_events_ms_min: Annotated[
            str, typer.Option("--producer-wait-between-events-ms-min",
                              callback=_convert_range,
                              help="Minimum delay (in ms) between two events")] = "0",
        producer_wait_between_events_ms_max: Annotated[
            str, typer.Option("--producer-wait-between-events-ms-max",
                              callback=_convert_range,
                              help="Maximum delay (in ms) between two events")] = "0",
        producer_wait_between_bursts_ms_min: Annotated[
            str, typer.Option("--producer-wait-between-bursts-ms-min",
                              callback=_convert_range,
                              help="Minimum delay (in ms) between two bursts")] = "0",
        producer_wait_between_bursts_ms_max: Annotated[
            str, typer.Option("--producer-wait-between-bursts-ms-max",
                              callback=_convert_range,
                              help="Maximum delay (in ms) between two bursts")] = "0",
        producer_flush_between_bursts: Annotated[
            str, typer.Option("--producer-flush-between-bursts",
                              callback=_convert_list_of_booleans,
                              help="Whether to flush between bursts")] = "true",
        producer_flush_every_min: Annotated[
            str, typer.Option("--producer-flush-every-min",
                              callback=_convert_range,
                              help="Minimum number of events between flush")] = "1",
        producer_flush_every_max: Annotated[
            str, typer.Option("--producer-flush-every-max",
                              callback=_convert_range,
                              help="Maximum number of events between flush")] = "1",
        # Consumers
        num_consumers: Annotated[
            int, typer.Option("--num-consumers",
                              help="Number of consumers")] = 1,
        consumer_batch_size: Annotated[
            str, typer.Option("--consumer-batch-size",
                              callback=_convert_range,
                              help="Batch size to use for the consumer")] = "-1",
        consumer_adaptive_batch_size: Annotated[
            str, typer.Option("--consumer-adaptive-batch-size",
                              callback=_convert_list_of_booleans,
                              help="Whether to use adaptive batch size in consumer")] = "true",
        consumer_check_data: Annotated[
            str, typer.Option("--consumer-check-data",
                              callback=_convert_list_of_booleans,
                              help="Whether consumer checks data integrity")] = "true",
        consumer_thread_count: Annotated[
            str, typer.Option("--consumer-thread-count",
                              callback=_convert_range,
                              help="Number of threads for the consumer")] = "0",
        consumer_data_selector_selectivity: Annotated[
            str, typer.Option("--consumer-data-selector-selectivity",
                              callback=_convert_float_range,
                              help="Selectivity of the consumer")] = "1.0",
        consumer_data_selector_proportion_min: Annotated[
            str, typer.Option("--consumer-data-selector-proportion-min",
                              callback=_convert_float_range,
                              help="Minimum proportion of data to read")] = "1.0",
        consumer_data_selector_proportion_max: Annotated[
            str, typer.Option("--consumer-data-selector-proportion-max",
                              callback=_convert_float_range,
                              help="Maximum proportion of data to read")] = "1.0",
        consumer_data_broker_num_blocks_min: Annotated[
            str, typer.Option("--consumer-data-broker-num-blocks-min",
                              callback=_convert_range,
                              help="Minimum number of blocks to use in the data broker")] = "1",
        consumer_data_broker_num_blocks_max: Annotated[
            str, typer.Option("--consumer-data-broker-num-blocks-max",
                              callback=_convert_range,
                              help="Maximum number of blocks to use in the data broker")] = "1",
        # Other options
        simultaneous_producer_and_consumer: Annotated[
            bool, typer.Option("--simultaneous-producer-and-consumer",
                              help="Whether to produce and consume simultaneously")] = False
        ):
    master_db_path_prefixes = master_db_path_prefixes.split(",")
    metadata_db_path_prefixes = metadata_db_path_prefixes.split(",")
    data_storage_path_prefixes = data_storage_path_prefixes.split(",")
    space = BenchmarkSpec.space(
        # Arguments for the MofkaServiceSpec
        num_servers=num_servers,
        num_metadata_db_per_proc=num_metadata_db_per_proc,
        num_data_storage_per_proc=num_data_storage_per_proc,
        master_db_path_prefixes=master_db_path_prefixes,
        metadata_db_path_prefixes=metadata_db_path_prefixes,
        data_storage_path_prefixes=data_storage_path_prefixes,
        master_db_needs_persistence=master_db_needs_persistence,
        metadata_db_needs_persistence=metadata_db_needs_persistence,
        data_storage_needs_persistence=data_storage_needs_persistence,
        num_pools_in_servers=num_pools_in_servers,
        num_xstreams_in_servers=num_xstreams_in_servers,
        allow_more_pools_than_xstreams=allow_more_pools_than_xstreams,
        # Arguments for the topic
        num_partitions=num_partitions,
        metadata_num_fields=metadata_num_fields,
        metadata_key_sizes=metadata_key_sizes,
        metadata_val_sizes=metadata_val_sizes,
        data_num_blocks=data_num_blocks,
        data_total_size=data_total_size,
        validator=validator,
        partition_selector=partition_selector,
        serializer=serializer,
        # Arguments for producers
        num_producers=num_producers,
        producer_batch_size=producer_batch_size,
        producer_adaptive_batch_size=producer_adaptive_batch_size,
        producer_ordering=producer_ordering,
        producer_thread_count=producer_thread_count,
        producer_burst_size_min=producer_burst_size_min,
        producer_burst_size_max=producer_burst_size_max,
        producer_wait_between_events_ms_min=producer_wait_between_events_ms_min,
        producer_wait_between_events_ms_max=producer_wait_between_events_ms_max,
        producer_wait_between_bursts_ms_min=producer_wait_between_bursts_ms_min,
        producer_wait_between_bursts_ms_max=producer_wait_between_bursts_ms_max,
        producer_flush_between_bursts=producer_flush_between_bursts,
        producer_flush_every_min=producer_flush_every_min,
        producer_flush_every_max=producer_flush_every_max,
        # Arguments for consumers
        num_consumers=num_consumers,
        consumer_batch_size=consumer_batch_size,
        consumer_adaptive_batch_size=consumer_adaptive_batch_size,
        consumer_check_data=consumer_check_data,
        consumer_thread_count=consumer_thread_count,
        consumer_data_selector_selectivity=consumer_data_selector_selectivity,
        consumer_data_selector_proportion_min=consumer_data_selector_proportion_min,
        consumer_data_selector_proportion_max=consumer_data_selector_proportion_max,
        consumer_data_broker_num_blocks_min=consumer_data_broker_num_blocks_min,
        consumer_data_broker_num_blocks_max=consumer_data_broker_num_blocks_max
        ).freeze()
    config = space.sample_configuration()
    spec = BenchmarkSpec.from_config(
        config=config, address=address, num_events=num_events,
        simultaneous_producer_and_consumer=simultaneous_producer_and_consumer)
    import json
    print(json.dumps(spec, indent=4))
