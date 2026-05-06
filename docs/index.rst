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
a C++ and Python API for using and developing streaming systems for HPC.
Client applications (producers and consumers) using Mofka should refer to
its `documentation <https://diaspora-stream-api.readthedocs.io/>`_.
The present documentation only covers the configuration of the Mofka engine
and the use of the Mofka driver for the Diaspora Stream API.

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   usage/installation.rst
   usage/quickstart.rst


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
