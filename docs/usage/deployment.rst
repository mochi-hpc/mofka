Deployment
==========

Mofka is an extremely modular service. The advantage is that it can be fine-tuned
exactly for the target use-case. The downside is that you will need more knowledge
about how to configure it in order to properly set it up for your use case.
In this section we will learn how to go from a one-process deployment with a single
in-memory partition, to a full multi-node deployment backed up by persistent
databases and storage devices.

.. note::

   In the following we will assume that all the processes run on the same machine,
   so we will use the :code:`na+sm` protocol. Replace this protocol with the appropriate
   one for a distributed deployment on your own HPC machine.


Isolating the master and storage servers
----------------------------------------

The *master database* is a Yokan provider marked with the tag :code:`mofka:master`.
It stores information about the topics that have been created, including their
validator, partition selector, and serializer, as well as their list
of partitions. We will start by isolating this master database in its own server.

.. tabs::

   .. group-tab:: master.json

      .. literalinclude:: ../_code/master-memory-config.json
         :language: json

   .. group-tab:: storage.json

      .. literalinclude:: ../_code/storage-memory-config.json
         :language: json


Copy the above *master.json* and *storage.json* files and start them separately
using Bedrock, as follows.

.. code-block:: bash

   bedrock na+sm -c master.json

This command will create a *mofka.json* file which will be used by the storage servers
to connect to the master server. Next, spin up storage servers as follows.

.. code-block:: bash

   bedrock na+sm -c storage.json

We will just use one storage server for now.
With this deployment of Mofka, we may now create a topic and add partitions as we
have learned in :ref:`Creating a topic and a partition`.

.. important::

   You will need to use :code:`--rank 1` instead of :code:`--rank 0` (or equivalent
   in C++ and Python) for the partition, since rank 0 is our master server and rank 1
   is our storage server.

.. note::

   Contrary to previous sections, note that the Flock provider, which is
   instantiated in both master and storage servers and is in charge of group membership,
   now uses a *centralized* backend. This backend expects the first server (here the master)
   to remain alive at all time. This first server will ping other servers periodically to
   check that they are alive. This functionality will be useful when elasticity and fault
   tolerance become available in Mofka.


Making the master database persistent
-------------------------------------

Right now our master database is still in memory, so if we shut down Mofka and restart it,
we will loose all the information about the topics that have been created. To avoid this,
let's make this database persistent.

`Yokan <https://mochi.readthedocs.io/en/latest/yokan.html>`_ has
`a few persistent backends <https://mochi.readthedocs.io/en/latest/yokan/07_backends.html>`_.
We will use RocksDB as an example. First we need to ensure that RocksDB is available as
a backend for Yokan. With our Spack environment activated, type :code:`spack find -v mochi-yokan`.
If :code:`+rocksdb` appears in the returned specification, RocksDB is available. If :code:`~rocksdb`
appears, we need to re-install Yokan with RocksDB support. Type :code:`spack add mochi-yokan+rocksdb`,
then :code:`spack concretize -f`, and finally :code:`spack install`.

The following *master.json* configuration replaces the *map* database with a *rocksdb* database
stored in */tmp/mofka/master*. First, create the */tmp/mofka* directory, then launch Bedrock
as shown before, with the following configuration.

.. literalinclude:: ../_code/master-rocksdb-config.json
   :language: json

You will see database files created in */tmp/mofka/master*, and our topic information will persist
between deployments of Mofka (don't forget to erase this directory if you want to restart from scratch!).


Making the storage server persistent
------------------------------------

Our *storage.json* still relies on a *map* Yokan database for metadata and a *memory* Warabi target
for data, both of which are in-memory and won't keep the data if the service is shutdown and restarted.
In this section we will make both Yokan and Warabi providers persistent. This change is reflected in
the following *storage.json* configuration.

.. literalinclude:: ../_code/storage-persistent-config.json
   :language: json


Persistent metadata
```````````````````

Metadata in Mofka is handled using Yokan providers. We can make them persistent the same way we did
the master database, by using a persistent backend such as RocksDB. This is what we do in the above
configuration by relying on a RocksDB database at */tmp/mofka/metadata*.

Persistent data
```````````````

Data in Mofka is handled using Warabi providers. We can make them persistent by relying on one of
Warabi's persistent backends: *pmdk* or *abtio*. The former uses `PMDK <https://pmem.io/pmdk/>`_ to
manage a fixed-size storage target. The latter uses
`ABT-IO <https://mochi.readthedocs.io/en/latest/abtio.html>`_, Mochi's Argobots-aware POSIX I/O library.

In the above configuration, we use the PMDK backend and request the creation of a 10MB storage
target at */tmp/mofka/data*.


Running multiple storage servers on the same node
-------------------------------------------------

Since we now use persistent locations for our databases and storage targets, spinning up more than
one storage server on the same node with the same *storage.json* file becomes a problem since they
will try to access the same database/target files.

Rather than writing a separate *storage.json* file for each server, we will rely on Bedrock's
`JX9 <https://jx9.symisc.net/>`_ configuration feature to use parametric configurations.
Copy the following *storage.jx9* file.

.. literalinclude:: ../_code/storage-persistent-config.jx9
   :language: cpp

This JX9 file contains a script that mostly looks like the *storage.json* configuration we
had before, except for the fact that this configuration is *returned* from the script after
being completed with two parameters, :code:`$database_path` and :code:`$target_path`, which
are themselves generated from an :code:`$id` parameter.

We can run this configuration with Bedrock as follows.

.. code-block:: bash

   bedrock na+sm -c storage.jx9 --jx9 --jx9-context id=42


The :code:`--jx9` flag indicates that the configuration provided is in JX9 format, not JSON.
The :code:`--jx9-context` flag allows to specify parameters in the form of a comma-separated list
of definitions :code:`key1=value1,key2=value2,...`. Here we use it to pass the value of the
:code:`$id` parameter. Passing 42 leads the :code:`$database_path` and :code:`$target_path` variables
to become */tmp/mofka/metadata_42* and */tmp/mofka/data_42* respectively. These values then get
inserted in the configuration.

.. note::

   :code:`$id` doesn't have to be a number. It could be a name, a UUID, etc.


You now know how to deploy a Mofka service with a persistent master database, and potentially
multiple metadata and data storage servers, on the same machine and on distinct ones.
The :ref:`Restarting Mofka` explains how to properly shutdown Mofka and restart it without
loosing the topics and events stored in it. The rest of this page dives deeper into configuring
Mofka for our needs.


Separating data, metadata, and partitions
-----------------------------------------

Until now we have worked with a *storage.json* configuration containing a Flock provider
for group membership management, a Yokan provider for metadata storage, and a Warabi provider
for data storage. In :ref:`Getting started` we were also running our master database in
the same configuration. You may now realize how flexible Mofka's configuration can be.
In fact, we could run every component in a separate process if we wanted. The following set
of configurations is meant for four processes: a *master* server, a *metadata* server,
a *data* server, and a *partition* server.


.. tabs::

   .. group-tab:: master.json

      .. literalinclude:: ../_code/master-rocksdb-config.json
         :language: json

   .. group-tab:: metadata.json

      .. literalinclude:: ../_code/split-metadata.json
         :language: json

   .. group-tab:: data.json

      .. literalinclude:: ../_code/split-data.json
         :language: json

   .. group-tab:: partition.json

      .. literalinclude:: ../_code/split-partition.json
         :language: json


All these configurations need to deploy a Flock provider to be part of the Mofka group.
Both the metadata server and the master server rely just on a Yokan provider for their
respective role. The data server runs the Warabi provider to store data. Finally the
partition server runs... nothing yet, apart from the Flock provider. This is because
:code:`mofkactl partition add` will be used to create the actual Mofka partition providers
in it.

Assuming these configurations have been started using Bedrock one after the other,
the master is considered server 0, the metadata server rank 1, the data server rank 2,
and the partition server rank 3. Their addresses can be found in the *mofka.json* file
generated and maintained by the service.

After creating a topic *mytopic*, we can create a partition for it with :code:`mofkactl`
as follows (replacing :code:`<address-of-metadata-server>` and :code:`<address-of-data-server>`
appropriately).

.. code-block:: bash

   mofkactl partition add mytopic -g mofka.json -r 3 \
       --metadata my_metadata_provider@<address-of-metadata-server> \
       --data my_data_provider@<address-of-data-server>

Using more providers per server process
---------------------------------------

Conservely, we could be tempted to run more than one Yokan or Warabi provider in a given
server process. The following configuration (which uses memory backends to make for a
shorter example) corresponds to a storage server with two Warabi providers and two Yokan
providers.


.. literalinclude:: ../_code/complex-storage-server.json
   :language: json


Again, when creating a topic partition with :code:`mofkactl`, we can specify which metadata
and data provider to use for this partition using :code:`--metadata` and :code:`--data`.

This configuration actually shows a lot more than just how to deploy multiple providers of
the same kind in the same process. The :code:`"margo"` section examplifies how to setup the
threading and scheduling for these providers. In this section, we create two Argobots pools,
*my_pool_1* and *my_pool_2*, and two execution streams (Argobots' name for hardware threads)
*my_es_1* and *my_es_2*. *my_es_1* is set to run any work that is pushed into *my_pool_1* and,
if *my_pool_1* is empty, run work from *my_pool_2*. *my_es_2* is set to run any work pushed into
*my_pool_2* (in some way, *my_es_1* steals work from *my_es_2* when it has no work to run from
*my_pool_1*).

Our various providers are then associated with pools. For instance, *my_metadata_provider_1* is
set to push work into *my_pool_1*, and *my_metadata_provider_2* into *my_pool_2*.

By configuring pools, execution streams, and providers in various ways and across different
processes, you can now see how modular Mofka really is. Associating a provider with a pool
backed up by more execution streams could allow more concurrency when accessing said provider,
allowing us to tune providers to their expected usage: a stream that sees an event coming every
second from a single source may need fewer resource than one that has to absorbe millions of
events from hundreds of producers. The latter may require more partition providers, or providers
backed by more execution streams.

.. note::

   Note shown by the configuration is a default pool called *__primary__* and a default execution
   stream also called *__primary__*. These two entities will always be present in any server, with
   the primary execution stream set to run work from the primary pool. If unspecified, a provider
   will default to pushing work into the primary pool. The network progress loop will also run from
   this primary pool. So in the above configuration, the Flock provider, which doesn't specify a
   pool, will be using the primary pool.
