Topics
======

Events in Mofka are pushed into *topics*. A topic is a distributed collection
of *partitions* to which events are appended. When creating a topic, users have to
give it a name, and optionally provide three objects.

* **Validator**: a validator is an object that validates that the Metadata and Data
  part comply with whatever is expected for the topic. Metadata are JSON documents
  by default, so for instance a validator could check that some expected fields
  are present. If the Metadata part describes the Data part in some way, a validator
  could check that this description is actually correct. This validation will happen
  before the event is sent to any server, resulting in an exception if the event is
  not valid. If not provided, the default validator will accept all the events it is
  presented with.

* **Partition selector**: a partition selector is an object that is given a list of
  available partitions for a topic and that will make a decision on which partition
  each event will be sent to, based on the event's metadata, or based on any other
  strategy. If not provided, the default partition selector will cycle through the
  partitions in a round robin manner.

* **Serializer**: a serializer is an object that can serialize a Metadata object into
  a binary representation, and deserialize a binary representation back into a Metadata
  object. If not provided, the default serializer will convert the Metadata into a
  string representation.

These objects can be customized and can be parameterized. For instance, a validator
that checks the content of a JSON Metadata could be provided with the list of
expected fields.

.. topic:: A motivating example

   Hereafter, we will create a topic accepting events that
   represent collisions in a particle accelerator. The Metadata part of such events will
   need to have an *energy* value, represented by an unsigned integer (just so we can show
   what optimizations could be done with Mofka's modularity). Furthermore, let's say the
   detector is calibrated to output energies from 0 to 99. We can create a validator that
   checks that the energy field is present and that its value is stricly lower than 100.
   If we would like to aggregate events with similar energy values into the same partition,
   we could have the partition selector make its decision based on this energy value.
   Finally, since we know that the energy value is between 0 and 99 and is the only relevant
   part of an event's Metadata, we could serialize this value into a single byte (:code:`uint8_t`),
   drastically reducing the metadata size compared with a string like :code:`{"energy":42}`.


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

      Work in progress...


We can now take a look at the implementation of these classes.

.. literalinclude:: ../_code/energy_validator.cpp
   :language: cpp

The :code:`EnergyValidator` class inherits from :code:`mofka::ValidatorInterface`
and provides the :code:`validate` member function. This function checks for the
presence of an :code:`energy` field of type unsigned integer and checks that
its value is less than an :code:`energy_max` value provided when creating the
validator.

.. important::

   The :code:`MOFKA_REGISTER_VALIDATOR` macro must be used to make tell Mofka
   about the :code:`EnergyValidator` class. Its first argument is the name by
   which we will refer to the class in user code (*"energy_validator"*), the
   second argument is the name of the class itself.

.. literalinclude:: ../_code/energy_partition_selector.cpp
   :language: cpp

The :code:`EnergyPartitionSelector` is also initialized with an :code:`energy_max`
value and uses it to aggregate events into "bins" of similar energy values.
It inherits from :code:`mofka::PartitionSelectorInterface` and we call
:code:`MOFKA_REGISTER_PARTITION_SELECTOR` to make it available for Mofka to use.

.. literalinclude:: ../_code/energy_serializer.cpp
   :language: cpp

The :code:`EnergySerializer` is also initialized with an :code:`energy_max` value.
This value is used to choose an appropriate number of bytes for the raw representation
of the energy when it is serialized. :code:`EnergySerializer` inherits


Adding partitions
-----------------

TODO
