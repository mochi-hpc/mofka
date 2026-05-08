Advanced deployments
====================

To understand how Mofka works, we should understand its architecture,
shown in the figure bellow, which represents the software stack in one server process.

.. figure:: ../_static/mofka-server.svg

   Mofka's architecture.

Mofka is built using the `Mochi <https://mochi.readthedocs.io>`_ collection of components,
in particular Argobots, Mercury, Margo, Thallium, Yokan, Flock, ABT-IO, plus custom
components (partition managers) that Mofka defines.

We have seen in the previous section that, to deploy a Mofka server, we need to call
:code:`bedrock`. Bedrock is our bootstrapping component. It takes a JSON configuration file
and instanciates the rest of the components according to this configuration file.
Each part of this stack can be configured and finely tuned. This section will explain how,
and along the way we will make our deployment fully distributed and persistent.
The final, more complete configuration file will be shown at the end.

Let's start with the configuration from the previous section.

.. literalinclude:: ../_code/simple-config.json
   :language: json

This configuration first lists the libraries (:code:`lib*-bedrock-module.so`) that bedrock
needs to load in order to instanciate the required components. It then defines three
*providers*.

* The "group_manager" is a component that handles group membership. It uses the
  `Flock <https://mochi.readthedocs.io/en/latest/flock.html>`_ Mochi library and currently
  uses the "self" bootstrap method. This method is appropriate for a single-process
  service.

* The "master_database" is a component that stores information about all the available
  topics. It relies on the `Yokan <https://mochi.readthedocs.io/en/latest/yokan.html>`_
  Mochi library and currently uses a "map" database, which is implemented as a C++
  :code:`std::map` and is therefore in memory.

* The "io_controller" is a component that will manage the I/O operations from our partitions.
  It uses the `ABT-IO <https://mochi.readthedocs.io/en/latest/abt-io.html>`_ Mochi
  library and is currently configured to rely on the same thread pool as the rest of the
  components.


Making the master database persistent
-------------------------------------

The first change we will want to make is to make the master database persistent.
This can be done by installing the :code:`mochi-yokan` package with a persistent backend.
For example, if we are within a spack environment, we can request Yokan to be built
with the RocksDB backend as follows.

.. code-block:: bash

   # you may uninstall the previous version of yokan first
   spack uninstall --dependents -y mochi-yokan
   # then add the new one to your environment
   spack add mochi-yokan+rocksdb
   # finally, reconcretize and rebuild it
   spack concretize -f
   spack install

Once this is done, we can change the "database" part of our master database configuration
as follows.

.. code-block:: json

   "database" : {
       "type": "rocksdb",
       "config": {
           "create_if_missing": true,
           "path": "/tmp/mofka/master"
       }
   }

We are now indicating that we want a RocksDB database, that we want it created if it does
not exist, and we provide the path at which this database should live.

.. note::

   Some backends (such as RocksDB) won't create the directories for their database
   if they don't exist, so go ahead and :code:`mkdir /tmp/mofka`.

You may now start Mofka as follows.

.. code-block:: bash

   bedrock na+sm -c config.json -v trace

Ensure that bedrock is running fine and has created the RocksDB database.


Running multiple processes
--------------------------

Next, we will distribute Mofka across multiple processes, first on a single machine, then
on multiple machines. We will start Mofka as an MPI program using :code:`mpirun`, but will
first have to do some modification to its configuration.

The first component we need to change is the group manager (flock), which is currently
setup to use the "self" bootstrap method. If we leave it like this, each process will
build its own single-member group. Instead, we want them to rely on MPI to form the group.
Go ahead and change :code:`"bootstrap": "self"` into :code:`"bootstrap": "mpi"` .

Next, only one process should have a master database. This can be solved by adding
an :code:`"__if__"` condition to our master_database component as follows.

.. code-block:: json

   {
      "__if__": "$MPI_COMM_WORLD.rank == 0",
      "name": "master",
      "provider_id": 2,
      "type": "yokan",
      ...

This :code:`"__if__"` condition will be evaluated at bootstrap and, if false, disable
the object it appears in. This way, only the process with rank 0 will have a master database.

You may now start Mofka as follows.

.. code-block::bash

   mpirun -np 2 bedrock na+sm -c config.json -v trace

:code:`na+sm` is a shared-memory transport and will only work on a single machine.
To run on a cluster, use an appropriate transport, such as "tcp", "verbs", "cxi", etc.

.. important::

   Please refer to the documentation `here <https://mochi.readthedocs.io/en/latest/hello-mochi.html>`_
   to find which protocols are available in your cluster. The :code:`margo-info` executable
   is installed in your Spack environment as part of the Mochi stack. If you need help, do not
   hesitate to contact the Mochi developers on Slack.


Creating persistent partitions
------------------------------

Before continuing, make sure Mofka is now running and take note of the location where
the *mofka.json* file was created (you may note that this file now contains the addresses
of all the Mofka servers).

Export the :code:`DIASPORA_CTL_DRIVER_OPTIONS` environment variable as follows so it can
be found by :code:`diaspora-ctl`.

.. code-block:: bash

   export DIASPORA_CTL_DRIVER_OPTIONS="--driver mofka --driver.group_file /path/to/mofka.json"

In the quickstart tutorial, we have created a topic with a single in-memory partition as follows.

.. code-block:: bash

   diaspora-ctl topic create --name my_topic --topic.num_partitions 1

The more complete version of this command is the following.

.. code-block:: bash

   diaspora-ctl topic create --name my_topic \
        --topic.num_partitions 1 \
        --topic.config.type memory \
        --topic.dependencies.pool __primary__

We will discuss the :code:`--topic.dependencies.pool` later, for now note that by default,
not specifying a partition type gives us an in-memory partition. Instead, we would like
a partition that is stored on disk. The following command will do that for us.

.. code-block:: bash

   diaspora-ctl topic create --name my_topic \
        --topic.config.type default \
        --topic.num_partitions 1 \
        --topic.config.partition.path /tmp/mofka


This command should execute and we should find that a new folder *my_topic-<uuid>* has been
created, where *<uuid>* is a randomly-generated UUID.

Note that a more complete version of the above command would be the following.

.. code-block:: bash

   diaspora-ctl topic create --name my_topic \
        --topic.num_partitions 1 \
        --topic.config.type default \
        --topic.config.partition.path /tmp/mofka \
        --topic.dependencies.io_controller io_controller \
        --topic.dependencies.pool __primary__

If :code:`--topic.dependencies.io_controller` is not specified, the first listed ABT-IO
component from the server will be used. If the pool is not specified, the :code:`__primary__`
pool is used.

The above code however lets us better understand how configuration is passed to the
partitions. The :code:`--topic.<X>` arguments are translated verbatim into a JSON
options object that mirrors the resulting Bedrock provider configuration.
The above command will convert its arguments into the following.

.. code-block:: json

   {
       "config": {
           "type": "default",
           "partition": {
               "path": "/tmp/mofka"
           }
       },
       "dependencies": {
           "io_controller": "io_controller",
           "pool": "__primary__"
       }
   }

That is: :code:`--topic.config.type` selects the partition manager,
:code:`--topic.config.partition.<field>` sets a partition-manager-specific field
(here, :code:`path`), and :code:`--topic.dependencies.<name>` resolves a Bedrock
dependency. You can confirm the full effective configuration with
:code:`bedrock-query`, where :code:`config/type`, :code:`config/partition/*`, and
:code:`dependencies/*` will appear in the same shape as the arguments above.

.. important::

   If you start Mofka (i.e. Bedrock) on multiple servers, "num_partitions" represents the total
   number of partitions to create, and these partitions will be assigned to servers in a
   round-robin manner.


More configuration
------------------

If we run the following:

.. code-block:: bash

   bedrock-query tcp -f mofka.json -p

We can see that the returned configuration is much larger than the one we provided.
In particular, the "config/partition" object of the Mofka provider holding our partition has
many more fields than just "path". The table below explains each of these fields.

.. list-table::
   :header-rows: 1
   :widths: 35 20 45

   * - Field
     - Default
     - Description
   * - :code:`path`
     - *(required)*
     - Base directory in which the partition's chunk files are written. The
       partition manager creates a subdirectory :code:`<path>/<topic>-<uuid>/`.
   * - :code:`max_chunk_size`
     - :code:`67108864` (64 MiB)
     - Maximum size in bytes that a chunk file (data, metadata, descriptors,
       or index) is allowed to reach before the partition rotates to a new
       chunk.
   * - :code:`max_events_per_chunk`
     - :code:`1000000`
     - Maximum number of events written to a single chunk before the
       partition rotates to a new one.
   * - :code:`sync`
     - :code:`true`
     - If :code:`true`, the partition issues an :code:`fdatasync` on each
       chunk file after every batch of events is written, ensuring durability
       at the cost of throughput. Set to :code:`false` to let the OS flush
       lazily.
   * - :code:`fd_cache_capacity`
     - :code:`64`
     - Capacity of the LRU cache of read-only file descriptors used when
       serving consumer requests. Larger values reduce the cost of opening
       chunk files repeatedly when consumers read across many chunks.
   * - :code:`producers.metadata_buffer_pool.num_tiers`
     - :code:`1`
     - Number of size tiers in the buffer pool used to receive event
       *metadata* from producers via RDMA. Each tier holds buffers of the
       same size; sizes grow geometrically across tiers (see
       :code:`first_size` and :code:`size_multiple`).
   * - :code:`producers.metadata_buffer_pool.num_buffers`
     - :code:`0`
     - Number of pre-allocated buffers per tier. :code:`0` means the pool
       starts empty and grows on demand.
   * - :code:`producers.metadata_buffer_pool.first_size`
     - :code:`65536` (64 KiB)
     - Size in bytes of the buffers in the first (smallest) tier.
   * - :code:`producers.metadata_buffer_pool.size_multiple`
     - :code:`4.0`
     - Geometric ratio between successive tiers. With :code:`first_size=64KiB`
       and :code:`size_multiple=4.0`, tier sizes are 64KiB, 256KiB, 1MiB, ...
   * - :code:`producers.data_buffer_pool.num_tiers`
     - :code:`1`
     - Same as above, but for the buffer pool used to receive event *data*
       from producers.
   * - :code:`producers.data_buffer_pool.num_buffers`
     - :code:`0`
     - Pre-allocated buffers per tier in the producer data pool.
   * - :code:`producers.data_buffer_pool.first_size`
     - :code:`67108864` (64 MiB)
     - First-tier buffer size for incoming producer data.
   * - :code:`producers.data_buffer_pool.size_multiple`
     - :code:`4.0`
     - Geometric ratio between tiers in the producer data pool.
   * - :code:`consumers.metadata_buffer_pool.num_tiers`
     - :code:`1`
     - Number of tiers in the buffer pool used to send event *metadata* back
       to consumers via RDMA.
   * - :code:`consumers.metadata_buffer_pool.num_buffers`
     - :code:`0`
     - Pre-allocated buffers per tier in the consumer metadata pool.
   * - :code:`consumers.metadata_buffer_pool.first_size`
     - :code:`65536` (64 KiB)
     - First-tier buffer size for outgoing consumer metadata.
   * - :code:`consumers.metadata_buffer_pool.size_multiple`
     - :code:`4.0`
     - Geometric ratio between tiers in the consumer metadata pool.
   * - :code:`consumers.desc_buffer_pool.num_tiers`
     - :code:`1`
     - Number of tiers in the buffer pool used to send event *data
       descriptors* (small, fixed-size records pointing at chunk locations)
       back to consumers via RDMA.
   * - :code:`consumers.desc_buffer_pool.num_buffers`
     - :code:`0`
     - Pre-allocated buffers per tier in the consumer descriptor pool.
   * - :code:`consumers.desc_buffer_pool.first_size`
     - :code:`4096` (4 KiB)
     - First-tier buffer size for outgoing data descriptors.
   * - :code:`consumers.desc_buffer_pool.size_multiple`
     - :code:`4.0`
     - Geometric ratio between tiers in the consumer descriptor pool.

These fields can be provided as command-line argument as we did with "path" before,
but it is much easier to aggregate them in a "topic-config.json" configuration file as follows.

.. code-block:: json

   {
       "fd_cache_capacity": 64,
       "max_chunk_size": 67108864,
       "max_events_per_chunk": 1000000,
       "consumers": {
           ...
       },
       ...
   }

We can then pass this configuration file to :code:`diaspora-ctl topic create` as follows.

.. code-block:: bash

   diaspora-ctl topic create --name my_topic \
        --topic.config.type default \
        --topic.num_partitions 1 \
        --topic.config.partition.path /tmp/mofka \
        --topic-config topic-config.json

.. note::

   Any argument passed using the :code:`--topic.config.` prefix will overwrite arguments
   from the configuration file passed to :code:`--topic-config`. In the above scenario,
   if the *topic-config.json* file had a "path" entry, it would be overwritten with
   "/tmp/mofka" as a consequence of passing
   :code:`--topic.config.partition.path /tmp/mofka`.


Setting up multithreading
-------------------------

In all the configurations we have used so far, every Mofka provider, the
network progress loop, and every RPC handler share the same default
Argobots execution stream (:code:`__primary__`). That means RPCs serialize
behind progress and behind one another — a bottleneck that becomes visible
as soon as multiple producers or consumers hit a single server, or when
chunk writes are slow enough to stall the handler that's currently running.

The simplest way to relieve this pressure is to ask Margo to spawn extra
Argobots resources for you. Two shortcut fields in the :code:`margo`
section of the Bedrock configuration do exactly that:

.. code-block:: json

   "margo": {
       "use_progress_thread": true,
       "rpc_thread_count": 4
   }

* :code:`use_progress_thread` moves Mercury's progress loop to its own
  dedicated execution stream, so polling no longer competes with RPC
  handlers.
* :code:`rpc_thread_count` creates an :code:`__rpc__` pool and N execution
  streams pulling from it; every RPC handler is then dispatched into that
  pool. With four ES, up to four RPCs can run truly concurrently.

.. note::

   In our case it is not necessary to set "use_progress_thread" to :code:`true`,
   by default the progress loop will use the :code:`__primary__` execution
   stream, which, if we specify a non-zero rpc_thread_count, is already
   separated from the execution streams servicing RPCs.

After restarting Bedrock, :code:`bedrock-query` will show the expanded
:code:`margo.argobots` section with the additional pool and the additional
xstreams that Margo generated for you.

For finer control — CPU pinning, choosing the Argobots scheduler, or
declaring multiple custom pools — you can write the long-form
:code:`argobots.pools` and :code:`argobots.xstreams` arrays directly in
the :code:`margo` section, and reference them by name from
:code:`progress_pool`, :code:`rpc_pool`, or any provider's :code:`pool`
dependency. The full schema is documented in
`Margo's JSON configuration reference
<https://mochi.readthedocs.io/en/latest/margo/09_config.html>`_. The next
section uses that long form to dedicate execution streams to ABT-IO.


Improving I/O performance
-------------------------

By default, the :code:`io_controller` (an ABT-IO provider) depends on
:code:`__primary__`. Every read and write a partition issues runs there,
contending with whatever else lives in that pool. Two complementary
techniques help: dedicate execution streams to ABT-IO, and/or switch to
the io_uring backend.


Dedicating execution streams to ABT-IO
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Declare a new pool and two xstreams pulling from it in the :code:`margo`
section:

.. code-block:: json

   "argobots": {
       "pools": [
           { "name": "__primary__", "kind": "fifo_wait", "access": "mpmc" },
           { "name": "io_pool",     "kind": "fifo_wait", "access": "mpmc" }
       ],
       "xstreams": [
           { "name": "__primary__",
             "scheduler": { "type": "basic_wait", "pools": ["__primary__"] } },
           { "name": "io_es_0",
             "scheduler": { "type": "basic_wait", "pools": ["io_pool"] } },
           { "name": "io_es_1",
             "scheduler": { "type": "basic_wait", "pools": ["io_pool"] } }
       ]
   }

Then point the :code:`io_controller` provider at the new pool by changing
its :code:`pool` dependency:

.. code-block:: json

   {
       "name": "io_controller",
       "type": "abt_io",
       "provider_id": 3,
       "config": {},
       "dependencies": { "pool": "io_pool" }
   }

Two execution streams sharing one :code:`mpmc` pool means ABT-IO ULTs
can run on whichever ES is free, and they no longer compete with RPC
handling for CPU time. If your workload is read-heavy and consumers
fan out across many partitions, raising the xstream count further can
help.


Switching to io_uring
~~~~~~~~~~~~~~~~~~~~~

ABT-IO can also use Linux's `io_uring
<https://en.wikipedia.org/wiki/Io_uring>`_ kernel interface as its
backend. With io_uring, the kernel asynchronously processes submitted
I/Os and reports completions through a ring buffer; the ABT-IO ULT only
submits and then waits for completion. Because the kernel does the
actual work, dedicating extra Argobots execution streams to this
backend would mostly waste cores — :code:`__primary__` is the right
target pool.

Add a second ABT-IO provider alongside the first one:

.. code-block:: json

   {
       "name": "io_uring_controller",
       "type": "abt_io",
       "provider_id": 4,
       "config": {
           "num_urings": 1,
           "liburing_flags": ["IOSQE_ASYNC"]
       },
       "dependencies": { "pool": "__primary__" }
   }

A partition can then be told which ABT-IO instance to use by naming it
in the partition's dependencies:

.. code-block:: bash

   diaspora-ctl topic create --name fast_topic \
        --topic.config.type default \
        --topic.num_partitions 1 \
        --topic.config.partition.path /tmp/mofka \
        --topic.dependencies.io_controller io_uring_controller

.. note::

   io_uring requires a recent enough Linux kernel and an :code:`abt-io`
   build with liburing support (in Spack, the :code:`mochi-abt-io
   +liburing` variant). If either is missing, fall back to the
   thread-based backend with a dedicated pool as shown above.

Full configuration
------------------

After all the modifications done above, here is the final configuration.

.. literalinclude:: ../_code/advanced-config.json
     :language: json
