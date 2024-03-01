Producers
=========

Applications that need to produce events into one or more topics will need
to create a :code:`Producer` instance. This object is an interface to produce
events into a designated topic. It will internally run the Validator, Partition
selector, and Serializer on the events it is being passed to validate the event's
Metadata and Data, select a destination partition for each event, and serialize
the event's metadata into batches aimed at the same partition.

.. note::

   In Kafka, only one process can produce to a partition at any given time, limiting
   the parallelism of a parallel producing application to the number of partitions a
   topic has, rather than the number of producing processes. Mofka works differently:
   any process can push events to any partitions. It is up to the user to restrict
   this usage to one closer to Kafka's if needed.


Creating a producer
-------------------

To obtain a :code:`Producer` instance, one must first instantiate a :code:`Client`,
connect to obtain a :code:`ServiceHandle`, before obtaining a :code:`TopicHandle`
by opening a topic. The :code:`TopicHandle` can then be used to create a :code:`Producer`,
as examplified hereafter.

.. tabs::

   .. group-tab:: C++

      .. literalinclude:: ../_code/energy_topic.cpp
         :language: cpp
         :start-after: START PRODUCER
         :end-before: END PRODUCER
         :dedent: 8

   .. group-tab:: Python

      Work in progress...

A producer can be created with four optional parameters.

* **Name**: the producer name is not currently used by Mofka but may be in the future,
  it is therefore advised to give your producer a name. If you have a multi-process
  producer application, it is recommended to give all producer instances the same name.

* **Thread pool**: the producer will run the topic's validator, partition selector, and
  serializer in user-level threads pushed into a thread pool. The thread pool is backed
  by a number of hardware threads (here 4). By default, a thread pool with 1 hardware
  thread will be created by the producer if not provided. It is also possible to initialize
  a :code:`ThreadPool` with 0 hardware thread. In this case, the producer will run its tasks
  and send RPCs whenever the user code blocks on a Mofka Future.

* **Batch size**: the batch size is the number of events to batch together before the batch
  is sent to the target partition. :code:`mofka::BatchSize::Adaptive()` can be used to tell
  the producer to adapt the batch size at run time: the producer will aim to send batches
  as soon as possible but will increase the batch size if the server is not responding fast
  enough.

* **Ordering**: because the producer runs the validator, partition selector, and serializer
  in user-level threads posted to the thread pool, one could imagine an application posting
  event A then event B, but event A takes more time being validated than event B and event B
  ends up in the batch before event A. :code:`mofka::Ordering::Loose` allows this behavior.
  :code:`mofka::Ordering::Strict` forces events that target the same batch to be added to the
  batch in the same order they were produced by the application. This constraint may limit
  parallelism opportunities in the producer and should be used only if necessary.


Producing events
----------------

As explained earlier, Mofka splits events into two parts: Metadata and Data.
The Metadata part is JSON-structured, small, and can be batched with the Metadata
of other events to issue fewer RPCs to partition managers. The Data part is optional
and represents potentially larger, raw data that can benefit from being transferred
via zero-copy mechanisms such as RDMA.

As an example, imagine an application that produces high-resolution images out of a
series of detectors at regular intervalles. The metadata part of an event might
by a JSON fragment containing the timestamp and detector information (e.g., callibration
parameters), as well as information about the images (e.g., dimensions, pixel format).
The data part of an event would be the image itself.

The code bellow shows how to create the Data and Metadata pieces of an event.

.. tabs::

   .. group-tab:: C++

      .. literalinclude:: ../_code/energy_topic.cpp
         :language: cpp
         :start-after: START EVENT
         :end-before: END EVENT
         :dedent: 8

      The first Data object, :code:`data1`, is a view of a single contiguous
      segment of memory underlying the :code:`segment1` vector. The second
      Data object, :code:`data2`, is a view of two non-contiguous such segments.

      The first Metadata object, :code:`metadata1`, is created from a raw string
      representing a JSON object with and "energy" field. The second Metadata object
      contains the same information but is initialized using an :code:`nlohmann::json`
      instance, which is the library used by Mofka to manage JSON data in C++.

   .. group-tab:: Python

      Work in progress...

.. important::

   In C++, a :code:`mofka::Data` object is a **non-owning view** of a potentially
   non-contiguous series of memory segments. You can think of it as a list of
   :code:`std::span<char>`. This means that (1) you need to make sure that the application
   does not free the memory before it has been transferred, and (2) you need to make sure
   not to write the memory while it is being transferred.

   In Python, the equivalent of a :code:`mofka::Data` is a :code:`list` of any objects
   satisfying the `buffer protocol <https://docs.python.org/3/c-api/buffer.html>`_
   (e.g., bytes, bytearray, numpy arrays, etc.).
   When pushing the data into a producer, the producer will share ownership of
   this list, there is therefore no danger that the memory underlying these objects
   is freed. Howeber the user should still take care that they are not written to
   until the data has been transferred.

Having created the Metadata and the Data part of an event, we can now push the event
into the producer, as shown in the code bellow.

.. tabs::

   .. group-tab:: C++

      .. literalinclude:: ../_code/energy_topic.cpp
         :language: cpp
         :start-after: START PRODUCE EVENT
         :end-before: END PRODUCE EVENT
         :dedent: 8

   .. group-tab:: Python

      Work in progress...

The producer's :code:`push` function takes the Metadata and the Data and returns a :code:`Future`.
Such a future can be tested for completion (:code:`future.completed()`) and can be blocked
on until it completes (:code:`future.wait()`). The latter method returns the event ID of the
created event (64-bits unsigned integer). It is perfectly OK to drop the future if you do not care
to wait for its completion or for the resulting event ID, as examplified with the second event.
Event IDs are monotonically increasing and are per-partition, so two events stored in distinct
partitions may end up with the same ID.

Calling :code:`producer.flush()` is a blocking call that will force all the pending batches of events
to be sent, regardless of whether they have reached the requested size. It can be useful to ensure
that all the events have been sent either periodically or before terminating the application.

.. important::

   If the batch size used by the producer is anything else than :code:`mofka::BatchSize::Adaptive()`,
   a call to :code:`future.wait()` will block until the batch containing the corresponding event
   has been filled up to the requested size and sent to its target partition. Hence, and easy
   mistake to do is to call :code:`future.wait()` when the batch is not full and with no other threads
   filling it up. This situation will result in a deadlock.
