.. Mofka documentation master file, created by
   sphinx-quickstart on Mon Feb 26 09:03:28 2024.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to Mofka's documentation!
=================================

Mofka is a streaming system for high-performance computing platforms and applications.
It is loosely inspired by cloud streaming systems like Kafka, but is implemented using
`Mochi <https://wordpress.cels.anl.gov/mochi/>`_, a set of tools and methodologies for
building highly-efficient HPC data services. As such, it benefits from
high-speed HPC networks with the `Mercury <https://mercury-hpc.github.io/>`_
RPC and RDMA library and a high level of on-node concurrency using
`Argobots <https://www.argobots.org/>`_.

Mofka follows the
`Diaspora Streaming API <https://github.com/diaspora-project/diaspora-stream-api>`_,
a C++ and Python API for developing streaming systems for HPC. The present documentation
is as much about the Diaspora Streaming API as it is about Mofka.

One of the particularities of this API is that it
splits events into two parts: a **data** part, referencing potentially large, raw data,
and a **metadata** part, which consists of structured information about the data
(usually expressed in JSON).
This separation allows the system to optimize independently the data and metadata
transfers, for instance by relying on zero-copy mechanisms, RDMA, or batching.
This interface is also often more adapted to HPC applications, which manipulate
large datasets and their metadata.


.. toctree::
   :maxdepth: 2
   :caption: Contents:

   usage/installation.rst
   usage/quickstart.rst
   usage/topics.rst
   usage/producer.rst
   usage/consumer.rst
   usage/deployment.rst
   usage/restarting.rst


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
