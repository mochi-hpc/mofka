Benchmarking
============

When built with the :code:`+benchmark` variant, Mofka will install a binary
called :code:`mofka-benchmark`. This program is a generic benchmark for
testing Mofka's performance. It is based on MPI and allows to configure
servers, producers, and consumers, as well as setup a workload to be executed.
This benchmark can be executed simply using the following command.

.. code-block:: bash

   mpirun -np 4 mofka-benchark config.json results.json

Replace :code:`mpirun` with your MPI launcher of choice and :code:`-np 4` with
your desied launcher parameters. In the following, we will assume a 4-process
execution of this benchmark.


Configuration
-------------

The benchmark's first argument is a *config.json* file describing the setup.
This configuration has the following basic format.

.. code-block:: json-object

   {
       "address": "na+sm",
       "servers": {
           "ranks": [0],
           "config": {
               // ...
           }
       },
       "producers": {
           "ranks": [1]
           // ...
       },
       "consumers": {
           "ranks": [2,3]
           // ...
       },
       "options": {
           // ...
       }
   }


The :code:`address` field provides the protocol to use (here :code:`na+sm`).
The :code:`servers`, :code:`producers`, :code:`consumers`, and :code:`options`
fields are explained hereafter.

Server configuration
````````````````````

The following JSON fragment shows an example for the :code:`servers` section
of the benchmark's configuration.

.. literalinclude:: ../_code/benchmark-config.json
   :language: json
   :start-after: BEGIN SERVERS
   :end-before: END SERVERS
   :dedent: 4


The servers configuration includes a :code:`ranks` field, containing an array
of MPI ranks that will act as Mofka servers. In the above configuration, rank 0
will be the only server.

Then comes a :code:`config` field. If the content of this field looks familiar,
it's because it corresponds to a Bedrock configuration as explained in
:ref:`the Deployment section<Deployment>`. Please refer to it for more information.

.. note::

   The Bedrock configuration can be instantiated differently depending on the
   MPI rank of the process that reads it. For example in the above configuration,
   :code:`"__if__": "$MPI_COMM_WORLD.rank == 0"` in the master database provider
   indicates that this provider should only be instantiated in rank 0.


Producer configuration
``````````````````````

Next, the following portion of the JSON configuration shows an example of
:code:`producers` section.

.. literalinclude:: ../_code/benchmark-config.json
   :language: json
   :start-after: BEGIN PRODUCERS
   :end-before: END PRODUCERS
   :dedent: 4


This configuration also includes a :code:`ranks` field to indicate in which ranks
a producer should be instantiated. Note that it is perfectly allowed to instantiate
a producer on the same rank as a server.

This configuration also includes the following fields.

* :code:`batch_size` (integer or "adaptive"): the size of the batches the producer will use.
* :code:`ordering` ("strict" or "loose"): the ordering of events.
* :code:`thread_count` (integer): the number of threads to use to help the producer.
* :code:`num_events` (integer): the number of events to produce (per producer).
* :code:`burst_size` (integer): the number of events to produce in each "burst".
* :code:`wait_between_bursts_ms` (number): the number of milliseconds to wait
  between each burst of events.
* :code:`wait_between_events_ms` (number): the number of milliseconds to wait
  between each event of the same burst.
* :code:`flush_between_bursts` (boolean): whether to flush the provider between two bursts.
* :code:`flush_every` (integer): number of events between each flush of the provider.
* :code:`group_file` (string): should be the same as specified in the Flock provider
  of the server configuration (here :code:`mofka.json`).

.. note::

   Some fields, including :code:`burst_size`, :code:`wait_between_bursts_ms`,
   :code:`wait_between_events_ms`, and :code:`flush_every`, can be replaced with
   an array of two numbers :code:`[a, b]`, in which case the value will be drawn
   uniformly in this interval everytime it is needed (e.g. if the burst size is
   set to :code:`[8, 32]`, then every burst will have a random number of events
   drawn uniformly between 8 and 32).

The :code:`topic` field describes the topic that the events will be pushed to.
This topic will be created at the beginning of execution and will be marked as
complete when all producers have finished publishing in it. Its fields are the following.

* :code:`name` (string): name of the topic.
* :code:`validator` (string): "default" for no validation, "schema" to validate that each
  event against an appropriate JSON schema (useful to evaluate the overhead of such validation).
* :code:`partition_selector` (string): must be "default" currently.
* :code:`serializer` (string): "default" to convert the metadata into a JSON string,
  "property_list_serializer" to use a serializer that is aware of the fields expected in
  the JSON and removes the keys to save space in the serialized metadata.
* :code:`metadata.num_fields` (integer): number of fields to generate in each event's metadata.
* :code:`metadata.key_sizes` (integer): size of the keys in the metadata.
* :code:`metadata.val_sizes` (integer): size of the values in the metadata.
* :code:`data.num_blocks` (integer): number of contiguous segments that make up each event's data.
* :code:`data.total_size` (integer): total number of bytes of each event's data.
* :code:`partitions`: list of partitions to create for the topic. Each partition needs at least
  the MPI rank in which to create it, and the type of partition ("default" above). Optionally,
  an Argobots pool can be specified.

.. note::

   The keys for the metadata will be generated once and will be common to all the events.
   The values however will change from one event to the next. This is to reflect a typical
   use-case where the metadata follows a predefined schema.

.. note::

   Again, many of the numerical fields can use an array of two numbers instead
   of a single number to see their value drawn randomly within the provided interval.


Consumer configuration
``````````````````````

Next, the following portion of the JSON configuration shows an example of
:code:`consumers` section.

.. literalinclude:: ../_code/benchmark-config.json
   :language: json
   :start-after: BEGIN CONSUMERS
   :end-before: END CONSUMERS
   :dedent: 4

The above configuration illustrates expressing the configuration in a "flat"
format, with keys written as :code:`"a.b.c": ... ` instead of
:code:`"a": { "b": { "c": { ... }}}`. The full benchmark configuration can be written
this way, which is often simpler to read for humans than the fully expanded JSON.

This configuration also includes a :code:`ranks` field to indicate in which ranks
a consumer should be instantiated. Note that it is perfectly allowed to instantiate
a consumer on the same rank as a server or a producer.

This configuration also includes the following fields.

* :code:`group_file` (string): should be the same as specified in the Flock provider
  of the server configuration (here :code:`mofka.json`).
* :code:`topic_name` (string): should be the same as the name used in the :code:`producer`
  section for the topic.
* :code:`consumer_name` (string): name to give to the consumer.
* :code:`num_events` (integer): maximum number of events each consumer will consume.
  If not specified, the consumer processes will consume all the events from the topic.
* :code:`batch_size` (integer): batch size for the consumer.
* :code:`thread_count` (integer): number of threads to help the consumer.
* :code:`check_data` (boolean): whether to check the data integrity upon receiving
  (used for testing that the benchmark works properly).
* :code:`data_selector.selectivity` (float between 0 and 1): proportion of events
  for which the data will be selected (e.g. a value of 0.25 will make the consumer ignore
  the data of a quarter of the events, on average).
* :code:`data_selector.proportion` (float between 0 and 1): for events that have
  their data selected, the proportion of the data that will be read (e.g. a value of 0.25
  will make the consumer read the first quarter of data of the events for which data
  is selected).
* :code:`data_broker.num_blocks`: number of blocks of memory to allocate for each event's data.


Additional options
``````````````````

The :code:`options` field provides global options. So far, the following are available.

* :code:`simultaneous` (boolean): whether to run the producer and consumer simultaneously
  or one after the other.


Benchmark output
----------------

The second argument of the benchmark program, :code:`results.json`, is the name of a file
to generate at the end of the execution and that provides statistics on the execution
of the producers and consumers. These statistics are presented in the format bellow.

.. code-block:: json-object

   {
       "<node-address>": {
           "producer": {
               "push": {
                   // statistics
               },
               "flush": {
                   // statistics
               },
               "runtime": 1.234
           },
           "consumer": {
               "pull": {
                   // statistics
               },
               "ack": {
                   // statistics
               },
               "runtime": 1.234
           }
       }
   }


In the above, :code:`runtime` is the total runtime of the producer or consumer.
Statistics include min, max, average (:code:`avg`), variance (:code:`var`) for
the specified operations: push, flush, pull, and acknowledgements.
