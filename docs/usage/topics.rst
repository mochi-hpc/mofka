Topics
======

Events in Mofka are pushed into *topics*. A topic is a distributed collection
of *partitions* to which events are appended. When creating a topic, users have to
give it a name, and optionally provide three objects.

* **Validator**: a validator is an object that validates that the metadata and data
  part comply with whatever is expected for the topic. Metadata are JSON documents
  by default, so for instance a validator could check that some expected fields
  are present. If the metadata part describes the data part in some way, a validator
  could check that this description is actually correct. This validation will happen
  before the event is sent to any server, resulting in an exception if the event is
  not valid. If not provided, the default validator will accept all the events it is
  presented with.

* **Partition selector**: a partition selector is an object that is given a list of
  available partitions for a topic and that will make a decision on which partition
  each event will be sent to, based on the event's metadata, or based on any other
  strategy. If not provided, the default partition selector will cycle through the
  partitions in a round robin manner.

* **Serializer**: a serializer is an object that can serialize a :code:`Metadata` object
  into a binary representation, and deserialize a binary representation back into a
  :code:`Metadata` object. If not provided, the default serializer will convert the
  :code:`Metadata` into a string representation.

.. image:: ../_static/TopicPipeline-dark.svg
   :class: only-dark

.. image:: ../_static/TopicPipeline-light.svg
   :class: only-light

Mofka will take advantage of multithreading to parallelize and pipeline the execution
of the validator, partition selector, and serializer over many events. These objects
can be customized and parameterized. For instance, a validator that checks the content
of a JSON metadata could be provided with a list of fields it expects to find in the
metadata of each event.

.. topic:: A motivating example

   Hereafter, we will create a topic accepting events that represent collisions in a
   particle accelerator. We will require that the metadata part of such events have
   an *energy* value, represented by an unsigned integer (just so we can show
   what optimizations could be done with Mofka's modularity). Furthermore, let's say that
   the detector is calibrated to output energies from 0 to 99. We can create a validator that
   checks that the energy field is not only present, but that its value is also stricly lower
   than 100. If we would like to aggregate events with similar energy values into the same partition,
   we could have the partition selector make its decision based on this energy value.
   Finally, since we know that the energy value is between 0 and 99 and is the only relevant
   part of an event's metadata, we could serialize this value into a single byte (:code:`uint8_t`),
   drastically reducing the metadata size compared with a string like :code:`{"energy":42}`.

.. important::

   In the following, we will still run a single-process Mofka server. Multi-process/node
   deployments will be covered in :ref:`Deployment`.


Creating a topic
----------------

The following code snippets show how to create a topic in C++, in Python,
and with Mofka's :code:`mofkactl` command-line tool. Our custom validator, partition selector,
and serializer are provided using the :code:`"name:library.so"` format. This tells the
Mofka client to dynamically load the specified libraries to get access to their
implementation.

.. tabs::

   .. group-tab:: C++

      .. literalinclude:: ../_code/energy_topic.cpp
         :language: cpp
         :start-after: START CREATE TOPIC
         :end-before: END CREATE TOPIC
         :dedent: 8

   .. group-tab:: Python

      Work in progress...

   .. group-tab:: mofkactl

      .. literalinclude:: ../_code/energy_topic.sh
         :language: bash
         :start-after: START CREATE TOPIC
         :end-before: END CREATE TOPIC

      Configuration parameters of each objects are passed using hierarchical
      command-line options. For instance, :code:`-p.x 42 -p.y.z abc`
      will produce the configuration :code:`{ "x": 42, "y": { "z": "abc" }}`.

      The group file is the name/path of the SSG group file specified in the
      server's JSON configuration. If not provided, :code:`mofkactl` will
      look for a *"mofka.json"* file in the current working directory.

Let's take a look at the implementation of the validator, partition selector,
and serializer classes.

.. literalinclude:: ../_code/energy_validator.cpp
   :language: cpp

The :code:`EnergyValidator` class inherits from :code:`mofka::ValidatorInterface`
and provides the :code:`validate` member function. This function checks for the
presence of an :code:`energy` field of type unsigned integer and checks that
its value is less than an :code:`energy_max` value provided when creating the
validator. If validation fails, the :code:`validate` function throws an exception.

.. important::

   The :code:`MOFKA_REGISTER_VALIDATOR` macro must be used to tell Mofka
   about the :code:`EnergyValidator` class. Its first argument is the name by
   which we will refer to the class in user code (*"energy_validator"*), the
   second argument is the name of the class itself (*EnergyValidator*).

.. literalinclude:: ../_code/energy_partition_selector.cpp
   :language: cpp

The :code:`EnergyPartitionSelector` is also initialized with an :code:`energy_max`
value and uses it to aggregate events into uniform "bins" of similar energy values.
It inherits from :code:`mofka::PartitionSelectorInterface` and we call
:code:`MOFKA_REGISTER_PARTITION_SELECTOR` to make it available for Mofka to use.

.. literalinclude:: ../_code/energy_serializer.cpp
   :language: cpp

The :code:`EnergySerializer` is also initialized with an :code:`energy_max` value.
This value is used to choose an appropriate number of bytes for the raw representation
of the energy when it is serialized. :code:`EnergySerializer` inherits from
:code:`SerializerInterface` and is registered with Mofka using
:code:`MOFKA_REGISTER_SERIALIZER`.


Adding partitions
-----------------

Topics are created without any partitions, so trying to push events into our "collisions"
topic will result in an error right now. We need add partitions to it.

.. note::

   Right now Mofka won't do any rebalancing if we add more partitions after having pushed
   some events in a topic, so we recommend setting up partitions right after creating the
   topic, and before having applications use it.

Partitions in Mofka are managed by a *partition manager*. A partition manager
is the object that will receive and respond to RPCs targetting the partition's
data and metadata. While it is possible to implement your own partition manager,
Mofka already comes with two implementations.

* **Memory**: The *"memory"* partition manager is a manager that keeps the metadata
  and data in the local memory of the process it runs on. This partition manager
  doesn't have any dependency and is easy to use for testing, for instance, but it
  won't provide persistence and will be limited by the amount of memory available
  on the node.
* **Default**: The *"default"* partition manager is a manager that relies on a
  `Yokan <https://mochi.readthedocs.io/en/latest/yokan.html>`_ provider for storing
  metadata and on a `Warabi <https://github.com/mochi-hpc/mochi-warabi>`_
  provider for storing data. Yokan is a key/value storage component with a number
  of database backends available, such as RocksDB, LevelDB, BerkeleyDB, etc.
  Warabi is a blob storage component also with a variety of backend implementations
  including Pmem.

A "memory" partition manager was used in the :ref:`Getting started` example. In the following
we will deploy a "memory" partition manager as well as a "default" partition manager. Since the
latter relies on Yokan and Warabi, we need to start Bedrock with a slightly longer
JSON configuration, shown hereafter.

.. literalinclude:: ../_code/default-config.json
   :language: json

This configuration instantiates three providers: two Yokan providers and a Warabi provider.
The first Yokan provider is used to store information about the topics created. The second
Yokan provider will be used to store event metadata for some Mofka partitions. The Warabi
provider will store the data.

.. note::

   Right now the two Yokan providers use Yokan's "map" backend, which is in-memory, and the
   Warabi provider uses Warabi's "memory" backend, which, you guessed it... is in memory.
   So we haven't improved much compared with a simpler "memory" partition manager. But
   this will allow us to complexify this configuration further later.

We can now add a partition that uses these providers.

.. tabs::

   .. group-tab:: C++

      .. literalinclude:: ../_code/energy_topic.cpp
         :language: cpp
         :start-after: START ADD PARTITION
         :end-before: END ADD PARTITION
         :dedent: 8

      Adding a partition is done via the :code:`ServiceHandle` instance by calling
      :code:`addMemoryPartition()` or :code:`addDefaultPartition()`. These functions
      takes at least two arguments: the topic name, and the rank of the server in which
      to add the partition. Servers are number contiguously from :code:`0` to :code:`N-1`
      where `N` can be obtained by calling :code:`sh.numServers()`.

      An optional argument is the Argobots pool to use to execute RPCs sent to the partition
      manager.

   .. group-tab:: Python

      Work in progress...

   .. group-tab:: mofkactl

      .. literalinclude:: ../_code/energy_topic.sh
         :language: bash
         :start-after: START ADD PARTITION
         :end-before: END ADD PARTITION

Two required arguments when adding partitions are the name of the topic and the rank
of the server to which the partition should be added. Here because we only have one
server, the rank is 0.

With a default partition manager, we can specify the metadata provider in the form
of an "address" interpretable by Bedrock. Here *"my_metadata_provider@local"* asks
Bedrock to look for a provider named *"my_metadata_provider"* in the same process as
the partition manager. In :ref:`Deployment` we will see that we could easily run these
providers on different processes.

.. note::

   If we don't specify the metadata (resp. data) provider in the above
   code/commands, Mofka will look for a Yokan (resp. Warabi)
   provider with the tag :code:`"mofka:metadata"` (resp. :code:`"mofka:data"` ) in the
   target server process and use that as the metadata (resp. data) provider.
   If multiple such providers exist, Mofka will choose the first one it finds in the
   configuration file.

.. note::

   We could have relied on a single Yokan provider and given it both tags :code:`"mofka:master"`
   and :code:`mofka:metadata`. This is perfectly valid. However, storing topic information
   in the same database as events is not recommended as it could lead to a bottleneck and
   it could make it difficult later on to delete partitions.
