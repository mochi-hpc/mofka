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

Mofka provides a C++ and a Python interface. One of its particularities is that it
splits events into two parts: a *Data* part, referencing raw, potentially large data,
which Mofka will try its best not to copy more than necessary (e.g., by relying on
RDMA to transfer it directly from a client application's memory to a storage device
on servers) and a *Metadata* part, which consists of structured information about
the data (usually expressed in JSON). Doing so allows Mofka to store each part
independently, batch metadata together, and allow an event to reference (a subset of)
the data of another event. This interface is also often more adapted to HPC applications,
which manipulate large datasets and their metadata.


.. toctree::
   :maxdepth: 2
   :caption: Contents:

   usage/installation
   usage/quickstart
   usage/topics
   usage/producer.rst
   usage/consumer.rst
   usage/deployment.rst



Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
