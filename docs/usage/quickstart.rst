Getting started
===============

Mofka is a Mochi service, which means that contrary to a monolithic system
like Kafka or any other streaming service, it is *defined by a composition
of specific building blocks*. The advantage of this approach is that Mofka
is infinitely more modular than other services. You can change nearly everything
about it, from the implementation of its databases, down to how they share
resources such as hardware threads and storage devices, ensuring that you can
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

.. important::

   If you have built Mofka with the :code:`~legacy` option, remove
   *libwarabi-bedrock-module.so* from the list of libraries in the above
   configuration.

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
   *libwarabi-bedrock-module.so*, *libabt-io-bedrock-module.so*, and *libmofka-bedrock-module.so*.
   If you are using a Spack environment, activate it then type :code:`spack config edit config`
   and add the :code:`modules` section as follows. Deactivate it, and reactivate it for the
   changes to be taken into account.

   .. code-block:: yaml

       spack:
         ...
         modules:
           prefix_inspections:
             lib: [LD_LIBRARY_PATH]
             lib64: [LD_LIBRARY_PATH]


Interacting with Mofka with diaspora-ctl
----------------------------------------

Once Mofka is deployed, open a new terminal and activate your spack environment.
You can export the following environment variable for :code:`diaspora-ctl`
to interact with Mofka by default.

.. code-block:: bash

   export DIASPORA_CTL_DRIVER_OPTIONS="--driver mofka --driver.group_file /path/to/mofka.json"

.. important

   Make sure to provide an absolute path to *mofka.json* so that :code:`diaspora-ctl` can locate
   it from wherever it is executed.


Creating a topic and a partition
--------------------------------

.. code-block:: bash

   diaspora-ctl topic create --name my_topic --topic.num_partitions 1

The topic *my_topic* has been created, with one partition. This partition is in-memory
by default. The following can be used to check that the topic has indeed been created.

.. code-block:: bash

   diaspora-ctl topic list

In the next section, we will see how to setup proper persistent partitions in
a distributed deployment of Mofka.


Shutting down Mofka
-------------------

To shutdown Mofka properly, the :code:`bedrock-shutdown` command can be used as follows.

.. code-block:: bash

   bedrock-shutdown na+sm -f mofka.json
