Using Kafka under Mofka
=======================

Kafka can be used under the Mofka client API instead of Mochi components.
To use it, you will first need a JSON configuration file as follows.

.. code-block:: json

   {
       "bootstrap.servers": "localhost:9092"
   }

Relpace :code:`localhost:9092` with the host and port of one of your Kafka server
(or a comma-separated list of servers).

In your client application, replace include :code:`mofka/KafkaDriver.hpp` (instead
or in addition to :code:`mofka/MofkaDriver.hpp`). You may then use the :code:`mofka::KafkaDriver`
class in place of the :code:`mofka::MofkaDriver` class. This class' constructor takes
the path to a JSON file as shown above. Its API is almost identical to that of :code:`mofka::MofkaDriver`.
In particular, the :code:`createTopic` and :code:`openTopic` methods can be used to
create and open topics. The rest of the classes (:code:`TopicHandle`, :code:`Producer`, etc.)
remain identical when using a :code:`KafkaDriver`.

Bellow are examples of producers and consumer codes using the :code:`KafkaDriver`.

Kafka producer application
--------------------------

.. tabs::

   .. group-tab:: C++

      .. literalinclude:: ../_code/kafka_producer.cpp
         :language: cpp

   .. group-tab:: Python

      .. literalinclude:: ../_code/kafka_producer.py
         :language: python


Kafka consumer application
--------------------------

.. tabs::

   .. group-tab:: C++

      .. literalinclude:: ../_code/kafka_consumer.cpp
         :language: cpp

   .. group-tab:: Python

      .. literalinclude:: ../_code/kafka_consumer.py
         :language: python

