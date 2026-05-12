Default partition manager — architecture
========================================

This page describes the design of Mofka's *default* partition manager: how
events are laid out on disk, how a producer's batch flows from the wire
through to the chunk files, how consumers are fed back from the same files,
and which configuration parameters govern each piece. It is a companion to
the field reference in :doc:`advanced`, which lists the configuration keys
and their default values.


Overview
--------

The default partition manager stores events in append-only chunked log
files on a local filesystem, using
`ABT-IO <https://mochi.readthedocs.io/en/latest/abt-io.html>`_ for
asynchronous, Argobots-friendly I/O. Unlike the *legacy* partition manager
— which stores metadata in a Yokan database and data blobs in a Warabi
target — the default partition needs no external Mochi services. Its only
required dependency is an :code:`io_controller` (an ABT-IO instance running
in the same Bedrock process).

Trade-off: legacy gives you whatever durability, indexing, and remote
storage your Yokan/Warabi backends provide; default trades that flexibility
for a much simpler, dependency-free deployment and a small, predictable
on-disk format.


On-disk layout
--------------

A partition is a directory:

.. code-block:: text

   <path>/<topic>-<uuid>/

where :code:`<path>` is the configured :code:`path`, :code:`<topic>` is the
topic name, and :code:`<uuid>` is the partition UUID. Inside that directory,
events live in **chunks**. Every chunk is *four* sibling files sharing the
prefix :code:`chunk-NNNNNN.` (six-digit zero-padded chunk id):

.. list-table::
   :header-rows: 1
   :widths: 15 85

   * - Extension
     - Content
   * - :code:`.meta`
     - Concatenated event metadata, packed back-to-back (variable length).
   * - :code:`.data`
     - Concatenated event data, packed back-to-back (variable length).
   * - :code:`.desc`
     - Concatenated serialized :code:`DataDescriptor` records (one per event).
   * - :code:`.idx`
     - Fixed-size :code:`IndexRecord` array (36 bytes per entry, one per
       event). Each record holds the offset and size of that event's
       metadata, data, and descriptor in the three sibling files.

The :code:`.idx` file is what makes the format self-describing: every event
in the chunk has exactly one fixed-width record pointing into the variable-
length sibling files. That same property drives crash recovery (below).

Chunk rotation
~~~~~~~~~~~~~~

The manager opens a *current* chunk (id 0 at first) and appends to it until
either of the following triggers, after which it closes the four file
descriptors and opens a new chunk with id incremented by one:

* the chunk has reached :code:`max_events_per_chunk` events, or
* the combined size of its :code:`.meta` and :code:`.data` files has reached
  :code:`max_chunk_size` bytes.

**Trade-off.** Smaller chunk-size limits give more files (more
:code:`open` and :code:`fdatasync` overhead per partition over time, but
finer-grained crash recovery and more opportunities for parallel reads).
Larger limits give fewer, larger files.

Crash recovery and restart
~~~~~~~~~~~~~~~~~~~~~~~~~~

When a :code:`DefaultPartitionManager` is created with a UUID matching an
existing partition directory, it scans :code:`chunk-*.idx` files in numeric
order and rebuilds its in-memory state — the index, the per-event
chunk-id table, the running write offsets, the total event count, and the
current chunk id. New events are then appended to the last chunk
(triggering a rotation if it is already full).

This means restart is the same code path as fresh start: pass the same
:code:`<path>` and the same partition UUID, and the partition resumes
exactly where it left off.


Write path — producer to disk
-----------------------------

A producer's :code:`mofka_producer_send_batch` RPC is handled by
:code:`receiveBatch`. The work is split across two ULTs to keep the RPC
handler free for the next request while the heavy lifting (RDMA pull +
disk writes + optional fsync) happens in the background.

1. **The handler ULT** allocates a :code:`PushOperation` describing the
   batch, takes the write-queue lock, assigns the batch's first event
   id (incrementing :code:`m_assigned_events` by the batch size — this
   linearizes id assignment across concurrent senders), pushes the
   operation onto the write queue, signals the write-loop ULT, and starts
   the RDMA pulls.

2. **The RDMA pulls** acquire one buffer from each of the two
   *producer-side* buffer pools, sized to fit the batch's metadata sizes
   array + concatenated metadata content (and likewise for data). Both
   pulls run concurrently and overlap with whatever the write-loop ULT
   is doing for an earlier batch.

3. **The single per-partition write-loop ULT** picks the next operation
   off the queue, waits for its two RDMA pulls to complete, then writes
   to disk in this order:

   a. Build :code:`IndexRecord`\ s and a serialized descriptor blob in
      memory (one descriptor per event — these are what consumers will
      receive in the descriptor stream).
   b. Issue four :code:`abt_io_pwrite` calls — one to :code:`.meta`, one
      to :code:`.data`, one to :code:`.desc`, one to :code:`.idx` — at
      the current per-file offsets.
   c. If :code:`sync=true`, issue four :code:`abt_io_fdatasync` calls.
      Producers are *not* acknowledged before this step.
   d. Bump the file offsets, append the new index records to the
      in-memory index, and bump :code:`m_total_events` (which wakes any
      consumer ULTs blocked on :code:`m_events_cv`).
   e. If the rotation triggers fire, close the four FDs and open a new
      chunk.
   f. Send the producer's response.

Because step 1 assigns ids under the queue lock and step 3 drains the
queue serially, batches are stored in submission order — the in-memory
index, the on-disk :code:`.idx` records, and the consumer-visible event
ids all agree.

**Parameters that affect this path.**
:code:`producers.metadata_buffer_pool.*` and :code:`producers.data_buffer_pool.*`
shape the incoming RDMA buffer pools (see *Caches and buffer pools*
below). :code:`sync` flips the per-batch :code:`fdatasync` on or off.
:code:`max_chunk_size` and :code:`max_events_per_chunk` decide when the
write loop rotates to a new chunk.


Read path — feeding a consumer
------------------------------

A :code:`mofka_consumer_request_events` RPC drives the per-consumer feed
loop in :code:`feedConsumer`. The loop pipelines disk reads with RDMA
pushes so the network and the disk stay busy in parallel:

1. **Wait for events.** Block on :code:`m_events_cv` until the index has
   advanced past the consumer's cursor (or the consumer is asked to stop).
2. **Pick the next batch.** Read the upcoming :code:`min(batch_size, available)`
   index records to compute total metadata and total descriptor bytes.
3. **Allocate two outgoing RDMA buffers** — one from
   :code:`consumer_metadata_buffer_pool`, one from
   :code:`consumer_desc_buffer_pool` — each sized to fit the per-event sizes
   array plus the concatenated content for that batch.
4. **Issue parallel disk reads.** For each event in the batch, look up the
   chunk id and pread the metadata and descriptor regions out of the chunk
   file. Reads are grouped per chunk and the read-only file descriptor for
   each chunk comes from an LRU **FD cache** (see below). Reads use the
   non-blocking :code:`abt_io_pread_nb` so all reads in the batch are in
   flight together.
5. **Wait for disk reads to complete**, then **wait for the previous
   batch's RDMA push to drain**, then **start the next push**. The
   pipeline depth is one batch deep — the buffers from the previous push
   are recycled at the moment they become safe.

When the consumer is stopped with no events left, the loop sends a final
empty :code:`feed(0, NoMoreEvents, ...)`.

**Parameters that affect this path.**
:code:`consumers.metadata_buffer_pool.*` and
:code:`consumers.desc_buffer_pool.*` size the outgoing RDMA buffers. If a
batch needs a buffer larger than the largest tier currently has, the pool
grows on demand — correctness-preserving, but at the cost of allocating
and registering a new buffer mid-flight. :code:`fd_cache_capacity` decides
how many distinct chunk files can be hot in the read cache simultaneously.


Read path — random access (`getData`)
-------------------------------------

When a consumer asks for the *data* bytes for events whose descriptors it
already holds, the request lands in :code:`getData` (driven by the
:code:`mofka_consumer_request_data` RPC):

1. Decode each :code:`DataDescriptor` to recover its :code:`{chunk_id,
   offset, size}` triple.
2. Allocate a flat buffer sized to the sum of all requested sizes.
3. For each chunk referenced by the request, open the chunk's
   :code:`.data` file with :code:`abt_io_open(O_RDONLY)`, issue blocking
   :code:`abt_io_pread`\ s for the events from that chunk, then close the
   file before moving to the next chunk.
4. Build a segment list that respects the descriptor-level
   :code:`flatten()` layout (so non-contiguous selections are honored),
   expose those segments as a single read-only bulk handle, and push to
   the consumer.

This path **does not currently use the FD cache or a buffer pool** — it
opens and closes each chunk per call and allocates a fresh
:code:`std::vector<char>` for the response. For workloads that randomly
fetch data across many chunks, that per-call open cost can dominate;
caching here is an obvious future improvement.


Caches and buffer pools
-----------------------

There are five distinct data structures whose sizes are driven by
configuration. The table summarises which parameter sizes each one and
which data-flow path uses it:

.. list-table::
   :header-rows: 1
   :widths: 35 35 30

   * - Structure
     - Sized by
     - Used in
   * - FD cache (LRU read-only file descriptors)
     - :code:`fd_cache_capacity`
     - :code:`feedConsumer` (.meta, .desc reads)
   * - Producer metadata buffer pool (write_only)
     - :code:`producers.metadata_buffer_pool.*`
     - :code:`receiveBatch` RDMA pull
   * - Producer data buffer pool (write_only)
     - :code:`producers.data_buffer_pool.*`
     - :code:`receiveBatch` RDMA pull
   * - Consumer metadata buffer pool (read_only)
     - :code:`consumers.metadata_buffer_pool.*`
     - :code:`feedConsumer` RDMA push
   * - Consumer descriptor buffer pool (read_only)
     - :code:`consumers.desc_buffer_pool.*`
     - :code:`feedConsumer` RDMA push

The FD cache is a simple LRU keyed by chunk-file path. A miss costs an
:code:`abt_io_open`; a hit reuses an already-open read-only descriptor.
Eviction skips entries that are still referenced by an in-flight read,
so a small cache won't break correctness — but it will trigger more
opens.

The four buffer pools are :code:`thallium::bulk_buffer_pool` instances.
Each pool is *tiered*: it holds :code:`num_tiers` tiers of buffers, and
the k-th tier holds buffers of size
:code:`first_size * size_multiple^k`. Each tier preallocates
:code:`num_buffers` buffers (:code:`0` means "start empty, grow on
demand"). Buffers are pre-registered with Mercury for RDMA, so reusing
a pool buffer avoids the registration cost that an ad-hoc allocation
would pay. A request for size :code:`S` is served from the smallest tier
whose buffers are :code:`>= S`; if no such buffer is free, the pool
grows.

The producer-side pools are :code:`write_only` (the partition manager
*receives* into them). The consumer-side pools are :code:`read_only`
(the partition manager *sends* from them).


Tuning notes
------------

* **`max_chunk_size` / `max_events_per_chunk`.** Larger values amortize
  per-chunk overhead. Smaller values give finer-grained crash recovery
  (less to replay) and let consumer reads parallelize across more files.
* **`sync`.** :code:`true` calls :code:`fdatasync` on all four chunk
  files after every batch — a server crash loses at most the in-flight
  batch. :code:`false` lets the kernel flush lazily, which is faster but
  exposes a wider crash window.
* **`fd_cache_capacity`.** Bump it if consumers regularly read across
  many old chunks. Each miss is an :code:`abt_io_open`; each hit is
  free.
* **Producer buffer pools.** Set :code:`first_size` close to the typical
  batch payload size, and consider preallocating a few buffers
  (:code:`num_buffers > 0`) for the smallest tier if you expect a steady
  stream of similarly-sized batches.
* **Consumer buffer pools.** Set :code:`first_size` close to
  :code:`batch_size * average event metadata` (or descriptor) size.
  Undersizing isn't a correctness bug but causes runtime growth; gross
  oversizing wastes pinned memory.

The full list of parameters and their defaults lives in :doc:`advanced`
under "More configuration".
