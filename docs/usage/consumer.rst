Consumers
=========

Applications that need to consume events from a topic will need
to create a :code:`Consumer` instance. This object is an interface to consume
events from a designated list of partitions of a topic.


Creating a consumer
-------------------

One can obtain a :code:`Consumer` instance from a :code:`TopicHandle` the same way
as producers, as examplified hereafter.

.. tabs::

   .. group-tab:: C++

      .. literalinclude:: ../_code/energy_topic.cpp
         :language: cpp
         :start-after: START CONSUMER
         :end-before: END CONSUMER
         :dedent: 8

   .. group-tab:: Python

      .. literalinclude:: ../_code/energy_topic.py
         :language: python
         :start-after: START CONSUMER
         :end-before: END CONSUMER
         :dedent: 4


A consumer can be created with five parameters, four of which are optional.

* **Name**: the consumer name is mandatory. Mofka will keep track of the last event
  *acknowledged* by consumers, so that if an application stops and restarts with the
  same consumer name, it will restart consuming events from the last acknowledged event.
  At present, :code:`Consumers` with the same name should not pull from the same partition.

* **Batch size**: the batch size is the number of events to batch together on the server
  side before the batch is sent to the consumer. :code:`diaspora::BatchSize::Adaptive()`
  (:code:`AdaptiveBatchSize` in Python) can be used to tell the server to adapt the batch
  size at run time: the server will aim to send batches as soon as possible but will increase
  the batch size if the consumer is not responding fast enough.

* **Maximum batches**: the maximum number of batches that can be pre-fetched by the consumer
  at any one time.

* **Data selector**: the consumer first receives the metadata part of an event and runs
  the user-provided data selector function on the metadata to know whether the data should
  be pulled. This function takes the metadata part of the event as well as a :code:`DataDescriptor`
  instance. The latter is an opaque key that Mofka can use to locate the actual data.
  The above code is an example of data selector that will tell the consumer to pull the data
  only if the *"energy"* field in the metadata is greater than 20. It does so by returning
  the provided :code:`DataDescriptor` if the field is greater than 20, and by returning
  :code:`diaspora::DataDescriptor{}` (or :code:`None` in Python) if it isn't. The data
  selector could tell Mofka to pull *only a subset of an event's data*. More on this in the
  :ref:`Data descriptors` section.

* **Data allocator**: if the data selector returned a non-null :code:`DataDescriptor`, the user-provided
  data allocator function is invoked by the consumer. This function takes the event's metadata
  and the :code:`DataDescriptor` returned by the data selector, and must return a :code:`diaspora::DataView`
  object pointing to the location in memory where the application wishes for the data to be placed.
  This memory could be non-contiguous, it could be allocated by the data allocator or it could point to
  some already allocated memory somewhere. Remember that a :code:`DataView` object does not own
  the memory it points to. The application is therefore responsible for keeping the memory alive
  while Mofka pulls the data into it, and freeing it later if necessary.

* **Thread pool**: a thread pool can be provided to run the data selector and data allocator on
  multiple events in parallel.

.. important::

   In Python, the consumer cannot yet make use of a :code:`ThreadPool` for consuming data
   (this is because Python has a global interpreter lock that prevents concurrent invokation
   of Python functions from multiple threads from C). Hence we use the driver's default thread pool
   in the code above (we could have, alternatively, not specified the :code:`thread_pool` argument
   at all).

In Python, if your consumer intends to alway request the full data part of each event, and would
like said data in the form of a :code:`bytearray`, you may use the :code:`FullDataSelector`
and :code:`ByteArrayAllocator` from the :code:`diaspora_stream.api` module as data selector and
data allocator respectively. These are variables, not classes. The latter will create a Python
:code:`bytearray` to host the data.


Pulling events
--------------

Now that we have a consumer fetching events (and potentially their data) in the background,
we can pull the events out of the consumer. The following code shows how to do this.

.. tabs::

   .. group-tab:: C++

      .. literalinclude:: ../_code/energy_topic.cpp
         :language: cpp
         :start-after: START CONSUME EVENTS
         :end-before: END CONSUME EVENTS
         :dedent: 8

   .. group-tab:: Python

      .. literalinclude:: ../_code/energy_topic.py
         :language: python
         :start-after: START CONSUME EVENTS
         :end-before: END CONSUME EVENTS
         :dedent: 4


:code:`consumer.pull()` is a non-blocking function that returns a
:code:`diaspora::Future<Event>` (:code:`FutureEvent` in Python) that can be tested for
completion and waited on. Waiting on the future gets us an :code:`Event` instance which
contains the event's metadata and data.

The call to :code:`event.acknowledge()` tells the Mofka partition manager that
all the events in the partition up to this one have been processed by this consumer
and should not be sent again, should the consumer restart.

.. note::

   In the C++ example we have allocated the memory for the data in our data allocator
   function, so we need to free it when we no longer need it. In Python, the event
   will share owneship of the data returned by the broker and garbage collection
   will free the buffer at a later time.


Data descriptors
----------------

.. important::

   The feature described hereafter is only partially implemented.

The :code:`DataDescriptor` class is an opaque key sent by a Mofka partition manager
to reference the data associated with an event. In the above example, the data selector
either selected the full data associated with an event by returning the descriptor that
was passed to it, or declines the data entirely by returning a default-constructed
:code:`diaspora::DataDescriptor` in C++ or :code:`None` in Python.

The :code:`DataDescriptor` class however provides methods to build a new
:code:`DataDescriptor` referencing *a subset* of the data. Let's consider the example
of events containing data that represent an image of dimensions :code:`W*H`, stored
as a row-major array of :code:`uint8_t` values (for simplicity, assuming monochrome image).
We wish to only access a rectangle region of dimensions :code:`w*h` at offset :code:`(x,y)`,
as shown in the picture bellow.

.. image:: ../_static/DataDescriptor-dark.svg
   :class: only-dark

.. image:: ../_static/DataDescriptor-light.svg
   :class: only-light

The data selector is given a descriptor :code:`D` for the full data. :code:`D.size()`
(:code:`D.size` in Python) will return :code:`W*H`. We can first use
:code:`audo d1 = D.makeSubView(y*W + x, W*h)` (:code:`D.make_sub_view` in Python) to select
only the rows containing the rectangle we are interested in. This function takes the offset
at which to start the selection and the size of the selection.

We can then use :code:`auto d2 = d1.makeStridedView(0, h, w, W-w)` (:code:`d1.make_strided_view` in Python).
This function takes the offset at which to start the selection, the number of "blocks", the length of
each block, and the gap between each block.

By having the data selector return :code:`d2`, the Mofka server will know that the consumer
is only interested in this sub-region of the data and will transfer only the requested data.

.. note::

   The above selection could have been simplified as :code:`D.makeStridedView(y*W+x, h, w, W-w)`,
   we presented it in two steps to showcase both :code:`makeSubView` and :code:`makeStridedView`.

A third function, :code:`makeUnstructuredView`, takes an arbitrary list of :code:`(offset, size)`
pairs to make an unstructure selection of the data.
