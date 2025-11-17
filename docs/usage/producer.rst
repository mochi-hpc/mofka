Producers
=========

Applications that need to produce events into one or more topics will need
to create a :code:`Producer` instance. This object is an interface to produce
events into a designated topic. It will internally run the validator, partition
selector, and serializer on the events it is being passed to validate the event's
metadata and data, select a destination partition for each event, and serialize
the event's metadata into batches aimed at the same partition.


Creating a producer
-------------------

To obtain a :code:`Producer` instance, one must first instantiate a :code:`Driver`,
before obtaining a :code:`TopicHandle` by opening a topic. The :code:`TopicHandle`
can then be used to create a :code:`Producer`, as exemplified hereafter.

.. tabs::

   .. group-tab:: C++

      .. literalinclude:: ../_code/energy_topic.cpp
         :language: cpp
         :start-after: START PRODUCER
         :end-before: END PRODUCER
         :dedent: 8

   .. group-tab:: Python

      .. literalinclude:: ../_code/energy_topic.py
         :language: python
         :start-after: START PRODUCER
         :end-before: END PRODUCER
         :dedent: 4

A producer can be created with five optional parameters.

* **Name**: the producer name is not currently used by Mofka but may be in the future,
  it is therefore advised to give your producer a name. If you have a multi-process
  producer application, it is recommended to give all producer instances the same name.

* **Thread pool**: the producer will run the topic's validator, partition selector, and
  serializer in user-level threads pushed into a thread pool. The thread pool is backed
  by a number of hardware threads. If not specified, the default thread pool of the
  driver will be used, which corresponds to a pool tied to the main thread of the application.

* **Batch size**: the batch size is the number of events to batch together before the batch
  is sent to the target partition. :code:`diaspora::BatchSize::Adaptive()` (:code:`AdaptiveBatchSize`
  in Python) can be used to tell the producer to adapt the batch size at run time: the
  producer will aim to send batches as soon as possible but will increase the batch size
  if the server is not responding fast enough.

* **Maximum batches***: this parameter controls the maximum number of batches that can
  be pending on the client before :code:`push` calls start blocking. By default this number
  is 2 (i.e., one batch is being sent while the next one is being filled through :code:`push`
  calls). Increasing this number may be useful in bursty applications.

* **Ordering**: because the producer may run the validator, partition selector, and serializer
  in user-level threads posted to the thread pool, one could imagine an application posting
  event A then event B, but event A takes more time being validated than event B and event B
  ends up in the batch before event A. :code:`diaspora::Ordering::Loose` (:code:`Ordering.Loose`
  in Python) allows this behavior.
  :code:`diaspora::Ordering::Strict` (:code:`Ordering.Strict`) forces events that target the
  same batch to be added to the batch in the same order they were produced by the application.
  This constraint may limit parallelism opportunities in the producer and should be used only
  if necessary.


.. note::

   As of Mofka 0.4.0, the thread pool of a producer will only run one task that operates
   the serialization of events into batches and the sending of these batches. A recommended
   practice is therefore to create a :code:`ThreadPool` with 1 thread.


Producing events
----------------

As explained earlier, Mofka splits events into two parts: metadata and data.
The metadata part is a small string, optionally JSON-structured, and can be batched with
the metadata of other events to issue fewer RPCs to partition managers. The data part is optional
and represents potentially larger, raw data that can benefit from being transferred
via zero-copy mechanisms such as RDMA.

As an example, imagine an application that produces high-resolution images out of a
series of detectors at regular intervalles. The metadata part of an event might
by a JSON fragment containing the timestamp and detector information (e.g., callibration
parameters), as well as information about the images (e.g., dimensions, pixel format).
The data part of an event would be the image itself.

.. note::

   As of Mofka 0.4.0, the metadata part does not have to be JSON-formatted. A raw string
   with any format may be used. the :code:`Metadata` class provides useful members functions
   such as :code:`json()` that will convert the content in JSON format in a lazy manner.
   Mofka itself won't use this function, and will not make assumption that the metadata is
   in JSON format.

The code bellow shows how to create the data and metadata pieces of an event.

.. tabs::

   .. group-tab:: C++

      .. literalinclude:: ../_code/energy_topic.cpp
         :language: cpp
         :start-after: START EVENT
         :end-before: END EVENT
         :dedent: 8

      The first :code:`diaspora::DataView data1` object is a view of a single contiguous
      segment of memory underlying the :code:`segment1` vector. The second
      :code:`diaspora::DataView data2` object is a view of two non-contiguous segments.

      The first :code:`diaspora::Metadata` object, :code:`metadata1`, is created from a
      raw string representing a JSON object with and "energy" field. The second :code:`Metadata`
      object contains the same information but is initialized using an :code:`nlohmann::json`
      instance, which is the library used by Mofka/Diaspora to manage JSON data in C++.

   .. group-tab:: Python

      .. literalinclude:: ../_code/energy_topic.py
         :language: python
         :start-after: START EVENT
         :end-before: END EVENT
         :dedent: 4

      The first variable :code:`data1` is a read-only :code:`bytes` buffer. :code:`data2`
      is a :code:`bytearray`, and :code:`data3` is a :code:`memoryview` of :code:`data1`.
      All three types adhere to the buffer protocol and can be used for the data part of
      an event. Other types such as NumPy arrays also adhere to this protocol.
      :code:`data4`, as a list of objects following the buffer protocol, can also be used
      to handle non-regular memory.

      The first metadata object, :code:`metadata1`, is a string containing JSON information.
      The second, :code:`metadata2`, is a dictionary. Both can be used for the metadata part
      of the event.


.. important::

   In C++, a :code:`diaspora::DataView` object is a **non-owning view** of a potentially
   non-contiguous series of memory segments. You can think of it as a list of
   :code:`std::span<char>`. This means that (1) you need to make sure that the application
   does not free the memory before it has been transferred, and (2) you need to make sure
   not to write the memory while it is being transferred.

   In Python, the equivalent of a :code:`mofka::Data` is a :code:`list` of any objects
   satisfying the `buffer protocol <https://docs.python.org/3/c-api/buffer.html>`_
   (e.g., bytes, bytearray, numpy arrays, etc.).
   When pushing the data into a producer, the producer will share ownership of
   this list, there is therefore no danger that the memory underlying these objects
   is freed. However the user should still take care that they are not written to
   until the data has been transferred.

Having created the metadata and the data part of an event, we can now push the event
into the producer, as shown in the code bellow.

.. tabs::

   .. group-tab:: C++

      .. literalinclude:: ../_code/energy_topic.cpp
         :language: cpp
         :start-after: START PRODUCE EVENT
         :end-before: END PRODUCE EVENT
         :dedent: 8

   .. group-tab:: Python

      .. literalinclude:: ../_code/energy_topic.py
         :language: python
         :start-after: START PRODUCE EVENT
         :end-before: END PRODUCE EVENT
         :dedent: 4


The producer's :code:`push` function takes the metadata and data objects and returns a
:code:`Future`. Such a future can be tested for completion (:code:`future.completed`) and
can be blocked on until it completes (:code:`future.wait()`). The latter method returns the
event ID of the created event (64-bits unsigned integer).
It is perfectly OK to drop the future if you do not care to wait for its completion or
for the resulting event ID, as examplified with the second event. Event IDs are monotonically
increasing and are per-partition, so two events stored in distinct partitions can end up with the same ID.

Calling :code:`producer.flush()` is a blocking call that will force all the pending batches of events
to be sent, regardless of whether they have reached the requested size. It can be useful to ensure
that all the events have been sent either periodically or before terminating the application.
