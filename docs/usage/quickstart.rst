Getting started
===============

Mofka is a Mochi service, which means that contrary to a monolithic system
like Kafka or any other streaming service, it is *defined by a composition
of specific building blocks*. The advantage of this approach is that Mofka
is infinitely more modular than other services. You can change nearly everything
about it, from the implementation of its databases, down to how they share
resources such as hardware threads and I/O devices, ensuring that you can
configure it to maximize performance on each individual platform and for
each individual use case. The downside of this approach, however, is that
you will need more knowledge about Mochi than you would need about the inner
workings of other services like Kafka.

In this section, we will quickly deploy the bare minimum for a single-node,
functional Mofka service accessible locally, before we can dive into the
client API for producers and consumer applications.


Deploying Mofka
---------------

The composition of microservices that defines Mofka is expressed using a JSON
configuration fed to `Bedrock <https://mochi.readthedocs.io/en/latest/bedrock.html>`_,
Mochi's bootstrapping service. Hereafter is a minimal JSON configuration for Mofka.

.. literalinclude:: ../_code/simple-config.json
   :language: json

Copy this configuration into a *config.json* file, then deploy it using Bedrock
as follows.

.. code-block:: bash

   bedrock na+sm -c config.json

You now have a Mofka service running locally. It will have created a *mofka.json*
file that client applications will use to connect to it (if you examine the content
of this file, you will find the address of your Mofka server, among other things).

In :ref:`this section<Deployment>` we will see how to deploy a more complex,
multi-node Mofka service.

.. note::

   If you encounter errors related to *dlopen*, make sure your `LD_LIBRARY_PATH`
   environment variable contains the path to the Bedrock modules that will be
   needed: *libflock-bedrock-module.so*, *libyokan-bedrock-module.so*,
   *libwarabi-bedrock-module.so*, and *libmofka-bedrock-module.so*. If you are using
   a Spack environment, activate it then type :code:`spack config edit config` and add
   the :code:`modules` section as follows. Deactivate it, and reactivate it for the
   changes to be taken into account.

   .. code-block:: yaml

       spack:
         ...
         modules:
           prefix_inspections:
             lib: [LD_LIBRARY_PATH]
             lib64: [LD_LIBRARY_PATH]


Creating a topic and a partition
--------------------------------

You can now use the :code:`mofkactl` command-line tool to create a topic.
In a separate terminate, with your Spack environment activated, enter the following command.

.. code-block:: bash

   mofkactl topic create my_topic --groupfile mofka.json

The topic *my_topic* has been created, but it does not have any partitions attached to it.
Add an in-memory partition using the following command.

.. code-block:: bash

   mofkactl partition add my_topic --type memory --rank 0 --groupfile mofka.json

Your topic now has a partition, we can start producing events into it.

Using the Mofka library
-----------------------

Mofka can be used in C++ or in Python (if built with Python support). The following
*CMakeLists.txt* file shows how to link a C++ application against the Mofka library in CMake.


.. literalinclude:: ../_code/CMakeLists.txt
   :language: cmake
   :end-before: CUSTOM TOPIC OBJECTS


In Python, most of Mofka's client interface is located in the :code:`mochi.mofka.client` module.
The sections hereafter show how to use both the C++ and Python interface to produce and consume events.


Simple producer application
---------------------------

The following code examplified a producer.

We first need to initialize a :code:`thallium::engine` in C++, or a :code:`pymargo.core.Engine` in
Python, which handles the Mochi runtime.

.. important::

   The engine needs to be initialized in *server mode* for Mofka to work.
   This is because Mofka servers will send RPCs to the clients.

Next, we create a :code:`MofkaDriver` object  using the file
created by our running Mofka server (*mofka.json*).

We then open the topic we have created, using :code:`driver.openTopic()`
(:code:`driver.open_topic()` in Python), which gives us a :code:`TopicHandle`
to interact with the topic.

We create a :code:`Producer` using :code:`topic.producer()`, and we use
it in a loop to create 100 events with their :code:`Metadata` and :code:`Data`
parts (we always send the same metadata here and we don't provide any data).
In Python, the metadata part can be a :code:`dict` convertible to JSON,
and the data part can be anything that satisfies the buffer protocol.

The :code:`push()` function is non-blocking. To ensure that the events
have all been sent, we call :code:`producer.flush()`.

.. tabs::

   .. group-tab:: C++

      .. literalinclude:: ../_code/producer.cpp
         :language: cpp

   .. group-tab:: Python

      .. literalinclude:: ../_code/producer.py
         :language: python

.. note::

   You may see a warning on your standard output about event ordering.
   You can ignore it for now.


Simple consumer application
---------------------------

The following code shows how to create a consumer and use it to consume the events.

The consumer object is created with a name. This is for Mofka to associate
the name with the last event that was acknowledged by the application.
In case of a crash of the application, it will be able to restart from the
last acknowledged event. This acknowledgement is done using the
:code:`Event`'s :code:`acknowledge()` function, which in the example bellow
is called every 10 events.

:code:`consumer.pull()` is a non-blocking function returning a :code`Future`.
Waiting for this future with :code:`.wait()` returns an :code:`Event` object
from which we can retrieve an event ID as well as the event's metadata and data.

As it is, the data associated with an event will not be pulled automatically
by the consumer, contrary to the event's metadata. Further in this documentation
you will learn how to pull this data, or part of it.

.. tabs::

   .. group-tab:: C++

      .. literalinclude:: ../_code/consumer.cpp
         :language: cpp

   .. group-tab:: Python

      .. literalinclude:: ../_code/consumer.py
         :language: python


Shutting down Mofka
-------------------

To shutdown Mofka properly, the :code:`bedrock-shutdown` command can be used as
follows.

.. code-block:: bash

   bedrock-shutdown na+sm -f mofka.json

