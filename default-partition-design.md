# Default Partition Manager Design

## Overview

The Default Partition Manager is a file-based `PartitionManager` implementation for
Mofka. It stores event metadata, data, and descriptors in append-only log files
on the local filesystem, using [ABT-IO](https://github.com/mochi-hpc/mochi-abt-io)
for non-blocking I/O. This replaces the dependency on Yokan (key-value store) and
Warabi (data store) required by the legacy partition manager, simplifying deployment
while still providing persistence.

Registered as the `"default"` partition type via:

```cpp
MOFKA_REGISTER_PARTITION_MANAGER_WITH_DEPENDENCIES(
    default, DefaultPartitionManager,
    {"abt_io", "abt_io", true, false, false});
```

Source files:

- `src/DefaultPartitionManager.hpp` — class declaration
- `src/DefaultPartitionManager.cpp` — implementation

## Dependencies

The partition manager has a single Bedrock dependency:

| Name     | Type     | Required | Array | Description                         |
|----------|----------|----------|-------|-------------------------------------|
| `abt_io` | `abt_io` | yes      | no    | ABT-IO instance for non-blocking I/O |

ABT-IO is always a **local** component (it exposes no RPCs). The dependency is
resolved by Bedrock as a `ComponentPtr`, from which the raw `abt_io_instance_id`
handle is obtained:

```cpp
auto component = dependencies.at("abt_io")[0]->getHandle<bedrock::ComponentPtr>();
auto abt_io    = static_cast<abt_io_instance_id>(component->getHandle());
```

When specifying ABT-IO dependencies in topic arguments or `addDefaultPartition()`,
the instance name must **not** include an `@address` suffix (e.g. `"my_abt_io"`,
not `"my_abt_io@local"`), because ABT-IO is resolved as a local component rather
than a remote provider handle.

## Configuration

Passed as JSON in the partition config:

```json
{
    "path": "/data/mofka-partitions",
    "max_chunk_size": 67108864,
    "max_events_per_chunk": 1000000,
    "sync": true,
    "bulk_cache": {
        "enabled": true,
        "initial_buffer_size": 1048576
    }
}
```

| Field                           | Type    | Required | Default    | Description                                              |
|---------------------------------|---------|----------|------------|----------------------------------------------------------|
| `path`                          | string  | yes      | —          | Base directory for chunk files                           |
| `max_chunk_size`                | integer | no       | 64 MiB     | Max combined `.meta` + `.data` size before chunk rotation |
| `max_events_per_chunk`          | integer | no       | 1,000,000  | Max events per chunk before rotation                     |
| `sync`                          | boolean | no       | `true`     | Call `abt_io_fdatasync` after each `receiveBatch`        |
| `bulk_cache.enabled`            | boolean | no       | `true`     | Enable RDMA bulk buffer caching (see below)              |
| `bulk_cache.initial_buffer_size`| integer | no       | `0`        | Pre-allocate content buffers to this size at startup (bytes). 0 = no pre-allocation |
| `ack_early.enabled`             | boolean | no       | `false`    | Enable deferred writes with early producer acknowledgment (see [Early Acknowledgment](#early-acknowledgment-ack_early)) |
| `ack_early.max_pending_batches` | integer | no       | `8`        | Max queued unwritten batches before backpressure blocks the producer |

The configuration is validated against a JSON Schema at creation time.

## On-Disk Layout

Each partition gets its own directory at `<path>/<topic_name>-<partition_uuid>/`.
Within that directory, data is organized into numbered chunks, each consisting of
four files:

```
<path>/<topic_name>-<uuid>/
    chunk-000000.meta    # concatenated metadata content
    chunk-000000.data    # concatenated raw event data
    chunk-000000.desc    # concatenated serialized DataDescriptors
    chunk-000000.idx     # array of fixed-size IndexRecord structs
    chunk-000001.meta
    chunk-000001.data
    chunk-000001.desc
    chunk-000001.idx
    ...
```

All four files within a chunk are append-only. A new chunk is created when either
`max_chunk_size` or `max_events_per_chunk` is reached (whichever comes first).

### File Roles

- **`.meta`** — Raw metadata bytes for each event, concatenated in order. Event
  boundaries are tracked by the index.
- **`.data`** — Raw event data (payload), concatenated in order.
- **`.desc`** — Serialized `DataDescriptor` objects, one per event. Each descriptor
  embeds a `FileDataDescriptor` location that records where the event's data lives
  on disk.
- **`.idx`** — Array of fixed-size `IndexRecord` structs. This is the on-disk
  counterpart of the in-memory index and enables recovery on restart.

### IndexRecord (40 bytes per event)

```cpp
struct IndexRecord {
    uint64_t metadata_offset;     // byte offset in .meta
    uint32_t metadata_size;       // byte size in .meta
    uint64_t data_offset;         // byte offset in .data
    uint32_t data_size;           // byte size in .data
    uint64_t data_desc_offset;    // byte offset in .desc
    uint32_t data_desc_size;      // byte size in .desc
};
```

### FileDataDescriptor (embedded in DataDescriptor)

```cpp
struct FileDataDescriptor {
    uint32_t chunk_id;   // which chunk the data lives in
    uint64_t offset;     // byte offset within .data
    uint32_t size;       // byte size within .data
};
```

This struct is stored as the DataDescriptor's opaque location blob. Consumers
receive it when events are fed, and it is used by `getData()` to locate and
read the raw data.

## In-Memory State

The partition manager maintains several in-memory structures:

- **Index cache** — `std::vector<IndexRecord> m_index` plus
  `std::vector<uint32_t> m_event_chunk_ids`. Loaded from `.idx` files at startup
  and appended during writes. At ~44 bytes per event, 1M events ≈ 42 MB.
- **Write cursor** — Current chunk ID, four open file descriptors, and byte offsets
  for `.meta`, `.data`, and `.desc`.
- **Event counter** — `m_total_events`, protected by `m_events_mtx` and notified
  via `m_events_cv` to wake waiting consumers.
- **Assigned events counter** — `std::atomic<size_t> m_assigned_events`. When
  `ack_early` is enabled, this is used for ID assignment (advanced before writes
  complete). When `ack_early` is disabled, `m_total_events` is used instead.
- **Consumer cursors** — `std::unordered_map<std::string, EventID>` tracking each
  consumer's position, protected by `m_consumer_cursor_mtx`.
- **Write lock** — `m_write_mtx` serializes all writes to chunk files.
- **Bulk buffer caches** — grow-only buffer caches that avoid per-call memory
  allocation and Mercury bulk registration (see [RDMA Bulk Buffer Cache](#rdma-bulk-buffer-cache) below).
- **Pending write queue** — (ack_early only) `std::queue<PendingWrite>` with
  associated mutex and condition variables for backpressure and signaling
  (see [Early Acknowledgment](#early-acknowledgment-ack_early) below).

## Operations

### `create()` — Factory / Recovery

1. Validate the config JSON against the schema (requires `"path"`).
2. Extract the ABT-IO handle from resolved Bedrock dependencies.
3. Create the partition directory (`mkdir -p` equivalent).
4. **Recovery scan**: iterate over existing `.idx` files starting from chunk 0.
   For each chunk, read the `IndexRecord` array to rebuild the in-memory index
   and determine write offsets. Stops at the first missing chunk.
5. Open the current chunk's four files.

### `receiveBatch()` — Write Path

1. **Lock `m_write_mtx`**.
2. **Prepare RDMA buffers** via `DualBulkCache` (or per-call allocation if the
   bulk cache is disabled). The cache reuses already-registered bulk handles when
   the buffer capacity has not changed.
3. **Pull metadata** from the producer via Thallium RDMA bulk transfer into the
   cached buffer. Uses `bulk().select(0, actual_size)` to transfer only the needed
   portion of the (possibly larger) cached bulk.
4. **Pull data** from the producer in the same fashion.
5. Compute per-event offsets and build `IndexRecord` entries.
6. For each event, construct a `FileDataDescriptor` (chunk_id, offset, size),
   wrap it in a `DataDescriptor`, and serialize it.
7. **Batch-write** to the four chunk files (one `abt_io_pwrite` per file for the
   entire batch).
8. If `sync` is enabled, call `abt_io_fdatasync` on all four file descriptors.
9. Append to the in-memory index cache.
10. Rotate chunk if thresholds are reached.
11. **Unlock**, update `m_total_events`, notify `m_events_cv`.

### `feedConsumer()` — Read Path

1. Look up the consumer's cursor position.
2. Loop while the consumer has not been stopped:
   a. Wait on `m_events_cv` if no new events are available.
   b. Determine batch range from the in-memory index (sizes come from
      `IndexRecord` — no file I/O needed for sizes).
   c. Read metadata content and descriptor content from chunk files using
      `abt_io_pread` into `DualBulkCache` buffers (or per-call vectors if the
      bulk cache is disabled).
   d. Expose as Thallium bulk handles (reused from cache when possible) and
      call `consumerHandle.feed()`.
   e. Advance the cursor.

### `getData()` — Data Retrieval

Called when a consumer requests the actual event data (which is not sent during
`feedConsumer` — only metadata and descriptors are pushed). Protected by
`m_getdata_mtx` to serialize access to the shared data buffer cache.

1. For each `DataDescriptor`, extract the `FileDataDescriptor` (chunk_id, offset,
   size).
2. Read the full event data from the `.data` chunk file via `abt_io_pread` into
   a cached buffer (`BulkCache`, or a per-call vector if the bulk cache is
   disabled). The buffer is grow-only and avoids re-allocation once it reaches
   the high-water-mark size.
3. Apply `desc.flatten()` to handle sub-view selections — this produces
   `(offset, size)` segments relative to the event data.
4. Expose the selected segments as a Thallium bulk and push to the consumer.
   (The bulk registration in `getData` is always per-call because the segment
   layout varies with each request.)

### `acknowledge()` — Cursor Update

Stores `event_id + 1` as the consumer's cursor position.

### `wakeUp()`

Broadcasts `m_events_cv` to wake consumers waiting for new events.

### `destroy()`

Currently a no-op (returns success). The destructor closes the four chunk file
descriptors.

## Chunk Rotation

Rotation is checked after each `receiveBatch`:

```cpp
bool shouldRotate() const {
    if (m_events_in_current_chunk >= m_max_events_per_chunk) return true;
    if ((m_meta_offset + m_data_offset) >= m_max_chunk_size)  return true;
    return false;
}
```

When triggered, the current chunk's file descriptors are closed, the chunk ID is
incremented, write offsets are reset to zero, and the new chunk's four files are
opened.

## Client API

### C++

```cpp
void MofkaDriver::addDefaultPartition(
    std::string_view topic_name,
    size_t server_rank,
    std::string_view abt_io_instance = {},   // e.g. "my_abt_io"; discovered if empty
    const diaspora::Metadata& config = {},
    std::string_view pool_name = "");
```

If `abt_io_instance` is empty, the driver queries the server's configuration via
a Jx9 script to find an `abt_io` provider.

### Python

```python
driver.add_default_partition(
    topic_name="my_topic",
    server_rank=0,
    abt_io_instance="",           # auto-discover
    partition_config={"path": "/tmp/mofka-data"},
    pool_name="")
```

### Topic Arguments (for tests / `create_topic`)

```json
{
    "partitions": 1,
    "partitions_type": "default",
    "partitions_config": {
        "path": "/tmp/mofka-default-test"
    },
    "partitions_dependencies": {
        "abt_io": "my_abt_io"
    }
}
```

## Server Configuration

The Bedrock server must have an ABT-IO provider configured. Minimal example:

```json
{
    "libraries": [
        "libmofka-bedrock-module.so",
        "libflock-bedrock-module.so",
        "libyokan-bedrock-module.so",
        "libabt-io-bedrock-module.so"
    ],
    "providers": [
        {
            "name": "my_abt_io",
            "type": "abt_io",
            "provider_id": 5,
            "config": {},
            "dependencies": {
                "pool": "__primary__"
            }
        }
    ]
}
```

## Comparison with Other Partition Managers

| Feature           | Default (file-based)          | Legacy (Yokan + Warabi)       | Memory               |
|-------------------|-------------------------------|-------------------------------|-----------------------|
| Persistence       | Yes (local files)             | Yes (Yokan DB + Warabi store) | No                   |
| External deps     | ABT-IO only                   | Yokan + Warabi                | None                 |
| Data location     | Local filesystem              | Configurable backends         | In-process heap      |
| I/O model         | Append-only chunked logs      | Key-value + object store      | Direct memory access |
| Recovery          | Scan `.idx` files at startup  | Backend-managed               | N/A                  |
| Sub-view support  | Yes (`flatten()` in getData)  | Yes                           | Yes                  |
| Sync control      | Configurable (`sync` flag)    | Backend-dependent             | N/A                  |

## RDMA Bulk Buffer Cache

### Problem

Every call to `receiveBatch`, `feedConsumer`, and `getData` previously allocated
fresh `std::vector` buffers and registered them with Mercury via
`engine.expose()` → `margo_bulk_create()`. Bulk registration pins memory pages
and is expensive on RDMA-capable transports. For a high-throughput event stream,
this per-call overhead adds up.

### Solution

Two helper classes (`BulkCache` and `DualBulkCache`) implement a **grow-only
(high-water-mark)** buffer caching strategy. Buffers grow when needed but never
shrink, and the associated Thallium bulk handle is re-registered only when the
underlying vector reallocates (i.e., its capacity increases).

#### `BulkCache`

A single `std::vector<char>` buffer. Used by `getData()` as a simple buffer
cache (no bulk registration caching, because the segment layout varies per call).

#### `DualBulkCache`

A pair of vectors — `std::vector<size_t>` for per-event sizes and
`std::vector<char>` for concatenated content — backed by a single two-segment
`thallium::bulk` handle. The bulk is registered over the full vector
**capacity** (not just the current size), so that subsequent calls with equal or
smaller data can reuse the existing registration. `bulk().select(0, actual_size)`
is used at transfer time to limit the RDMA operation to the needed bytes.

```
prepare(num_events, content_size):
    1. m_sizes.resize(num_events)
    2. m_content.resize(max(content_size, 1))   // ensure valid memory
    3. if either vector's capacity > registered capacity:
         re-register bulk over full capacities
    4. return
```

After warm-up (a few batches), the buffers stabilize at the high-water-mark and
no further `margo_bulk_create` calls are made.

### Cache Instances

| Member                   | Used by          | Bulk mode    | Protected by   |
|--------------------------|------------------|--------------|----------------|
| `m_recv_metadata_cache`  | `receiveBatch`   | `write_only` | `m_write_mtx`  |
| `m_recv_data_cache`      | `receiveBatch`   | `write_only` | `m_write_mtx`  |
| `m_feed_metadata_cache`  | `feedConsumer`   | `read_only`  | `m_events_mtx` |
| `m_feed_desc_cache`      | `feedConsumer`   | `read_only`  | `m_events_mtx` |
| `m_getdata_cache`        | `getData`        | (buffer only)| `m_getdata_mtx`|

### Thread Safety

- **`m_recv_*` caches**: protected by `m_write_mtx` (the RDMA pull and file
  writes are now both inside this lock).
- **`m_feed_*` caches**: protected by `m_events_mtx` (already held during the
  `feedConsumer` loop body).
- **`m_getdata_cache`**: protected by a dedicated `m_getdata_mtx`, since
  `getData` can be called concurrently for different consumers.

### Fallback

When `bulk_cache.enabled` is `false`, each call falls back to per-call
`std::vector` allocation and `engine.expose()` — identical to the original
behavior. The fallback vectors are declared at the same scope as the raw
pointers used by the rest of the function, avoiding any lifetime issues.

### Pre-allocation

Setting `bulk_cache.initial_buffer_size` to a non-zero value causes all content
buffers to be pre-allocated (and their bulk handles registered) at construction
time. This avoids re-registration overhead during the first few batches at the
cost of upfront memory usage.

## Early Acknowledgment (`ack_early`)

### Motivation

By default, `receiveBatch` performs the full pipeline synchronously: RDMA pull,
file writes, optional fsync, then returns the EventID. The producer blocks for the
entire duration. For workloads where durability can be relaxed in exchange for
lower producer-perceived latency, the `ack_early` mode defers file writes to a
background ULT and acknowledges the producer as soon as data has been pulled into
server memory.

### Enabling

`ack_early` requires configuration on **both sides**:

1. **Partition config** — enables the capability on the server:

   ```json
   {
       "path": "/data/mofka",
       "ack_early": {
           "enabled": true,
           "max_pending_batches": 8
       }
   }
   ```

2. **Producer options** — requests early acknowledgment per-producer:

   ```cpp
   diaspora::Metadata producer_options;
   producer_options.json()["ack_early"] = true;
   auto producer = topic.producer("myproducer", producer_options);
   ```

   If the partition does not support `ack_early` (e.g. memory or legacy backends),
   the flag is silently ignored and the synchronous path is used.

### Interface Changes

#### `PartitionManager` base class (`src/PartitionManager.hpp`)

Two virtual methods with default implementations were added:

```cpp
virtual bool supportsAckEarly() const { return false; }

virtual Result<diaspora::EventID> receiveBatchAckEarly(
    const thallium::endpoint& sender,
    const std::string& producer_name,
    size_t num_events,
    const BulkRef& metadata_bulk,
    const BulkRef& data_bulk) {
    // Default: fall back to synchronous path
    return receiveBatch(sender, producer_name, num_events, metadata_bulk, data_bulk);
}
```

Memory and Legacy partition managers inherit the defaults and require no changes.

#### RPC signature (`mofka_producer_send_batch`)

A `bool ack_early` parameter was added after `count`:

```
Before: (producer_name, count, metadata_bulk, data_bulk) → Result<EventID>
After:  (producer_name, count, ack_early, metadata_bulk, data_bulk) → Result<EventID>
```

This is a wire-breaking change (acceptable for pre-1.0 software where client and
server are co-deployed).

#### `ProviderImpl` dispatch (`src/ProviderImpl.hpp`)

The `receiveBatch` handler checks both the producer's request and the partition's
capability:

```cpp
bool use_ack_early = ack_early_requested
                  && m_partition_manager
                  && m_partition_manager->supportsAckEarly();
```

- **ack_early path**: calls `receiveBatchAckEarly()`, then manually calls
  `req.respond(result)` to send the RPC response before writes complete.
- **synchronous path**: uses `tl::auto_respond` to send the response after the
  full `receiveBatch()` completes (unchanged behavior).

#### Producer pipeline

The `ack_early` flag flows through: `MofkaTopicHandle::makeProducer()` parses it
from producer options JSON → `MofkaProducer` stores it → `ProducerBatch` includes
it in the RPC call.

### Data Flow (ack_early enabled)

```
Producer                    ProviderImpl                   DefaultPartitionManager
   |                            |                                |
   |--- RPC(ack_early=true) --->|                                |
   |                            |--- receiveBatchAckEarly() ---->|
   |                            |    1. Backpressure wait        |
   |                            |    2. RDMA pull (blocks)       |
   |                            |    3. Assign EventID           |
   |                            |    4. Enqueue PendingWrite     |
   |                            |<-- return EventID -------------|
   |<-- respond(EventID) -------|                                |
   |                            |              Background writer ULT:
   |                            |              5. Dequeue PendingWrite
   |                            |              6. Compute offsets/descriptors
   |                            |              7. abt_io_pwrite (4 files)
   |                            |              8. fsync (if configured)
   |                            |              9. Update m_total_events
   |                            |              10. Notify consumers
```

### `receiveBatchAckEarly()` — Fast Path

1. **Backpressure wait**: under `m_pending_writes_mtx`, blocks if the pending
   queue has reached `max_pending_batches`. This prevents unbounded memory growth
   if the producer is faster than the disk.
2. **RDMA pull** metadata and data into **owned vectors** (not the shared bulk
   cache, since the data must outlive this call). Uses per-call `engine.expose()`
   for the local bulk handles.
3. **Assign EventID**: `first_id = m_assigned_events.fetch_add(num_events)`.
   This is atomic so concurrent calls (if any) get non-overlapping ID ranges.
4. **Enqueue** a `PendingWrite` struct containing the pulled data, sizes,
   event count, and assigned first ID.
5. **Signal** `m_pending_writes_ready_cv` to wake the background writer.
6. **Return** `first_id` — the RPC response is sent immediately after.

The RDMA pull **must** complete before returning, because the producer's bulk
handles become invalid once the RPC responds.

### `PendingWrite` struct

```cpp
struct PendingWrite {
    std::vector<size_t> metadata_sizes;
    std::vector<char>   metadata_content;
    std::vector<size_t> data_sizes;
    std::vector<char>   data_content;
    size_t              num_events;
    diaspora::EventID   first_id;
};
```

Each `PendingWrite` owns all the data needed to perform the file writes
independently.

### `backgroundWriterLoop()` — Drain Pending Writes

Spawned as an Argobots ULT on the engine's handler pool at partition creation
time (only when `ack_early` is enabled):

```cpp
thallium::pool(engine.get_handler_pool()).make_thread(
    [mgr]() { mgr->backgroundWriterLoop(); },
    thallium::anonymous{});
```

The loop:

1. Lock `m_pending_writes_mtx`.
2. Wait on `m_pending_writes_ready_cv` while the queue is empty and
   `m_writer_stop` is false.
3. If stopped and empty, break.
4. Dequeue one `PendingWrite`, signal `m_pending_writes_cv` to release
   backpressure.
5. Unlock.
6. Lock `m_write_mtx` and call `processPendingWrite()` — this performs the same
   offset computation, descriptor serialization, and four `abt_io_pwrite` calls
   as the synchronous `receiveBatch`, followed by in-memory index updates and
   chunk rotation if needed.
7. Update `m_total_events += num_events`.
8. Unlock, notify `m_events_cv` to wake waiting consumers.

### Shutdown

The destructor sets `m_writer_stop = true`, signals the condition variable, and
waits on `m_writer_done` (a `thallium::eventual<void>`). All pending writes are
drained before chunk files are closed.

### EventID Assignment

When `ack_early` is enabled, the synchronous `receiveBatch` also uses
`m_assigned_events.fetch_add()` for ID assignment instead of `m_total_events`.
This prevents duplicate IDs if a non-ack_early producer sends to an ack_early-
capable partition while background writes are still in flight.

### Consumer Visibility

Consumers see events only after writes complete: `m_total_events` is incremented
by the background writer (not by `receiveBatchAckEarly`), and the consumer's
`feedConsumer` loop waits on `m_events_cv` which is notified after each
successful write. This means consumers always read from written data.

### Concurrency and Locking

| Lock                     | Protects                                      | Held by                        |
|--------------------------|-----------------------------------------------|--------------------------------|
| `m_pending_writes_mtx`   | `m_pending_writes` queue, `m_writer_stop`     | `receiveBatchAckEarly`, writer |
| `m_write_mtx`            | Chunk file state, offsets, in-memory index    | `receiveBatch`, writer         |
| `m_events_mtx`           | `m_total_events`, consumer wait               | Writer, `feedConsumer`         |

The background writer and synchronous `receiveBatch` both acquire `m_write_mtx`
before writing, so they serialize correctly.

### Error Handling

On write failure in the background writer, the error is logged via
`spdlog::error` and the batch is dropped (events are lost). `m_total_events` is
not incremented, so consumers never see the lost events. The producer has already
received its EventID — this is the durability trade-off of `ack_early`.

### Testing

The `MofkaAckEarlyTest` test (`tests/MofkaAckEarlyTest.cpp`) exercises the full
ack_early pipeline:

- Creates a default partition with `ack_early` enabled (`max_pending_batches: 4`)
- Produces 100 events with data using a producer with `ack_early: true`
- Consumes all 100 events and verifies event IDs, metadata content, and data
  content match expected values

## Limitations and Future Work

- **Single-node**: data is stored on the local filesystem of the server hosting
  the partition. No built-in replication or remote storage.
- **No compaction or GC**: chunk files are never deleted or compacted. Old chunks
  accumulate until the partition is destroyed.
- **File descriptor usage**: each `feedConsumer` and `getData` call opens and
  closes chunk files per event. A file descriptor cache could reduce overhead.
- **Recovery is read-only**: the scan rebuilds the in-memory index but does not
  repair partial writes (e.g. from a crash mid-batch).
- **ack_early data loss**: when `ack_early` is enabled, events that have been
  acknowledged to the producer but not yet written to disk will be lost if the
  server crashes. The producer has no way to know which events were persisted.
- **ack_early bulk cache bypass**: `receiveBatchAckEarly` uses per-call
  `engine.expose()` for RDMA buffers (not the shared `DualBulkCache`), because
  the pulled data must outlive the RPC call. This means the bulk cache
  optimization does not apply to the RDMA pull in the ack_early path.
