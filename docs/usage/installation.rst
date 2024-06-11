Installing Mofka
================

Like all Mochi packages, Mofka can be built using `Spack <https://spack.io/>`_.
After you have installed and setup spack, add the *mochi-spack-packages* repository as follows.

.. code-block:: bash

   git clone https://github.com/mochi-hpc/mochi-spack-packages.git
   spack repo add mochi-spack-packages

We recommend that you update your clone of *mochi-spack-packages* from time to time
to get the latest versions of all the dependencies.

You can then install Mofka as follows.

.. code-block:: bash

   spack install mofka


Installing in a Spack environment
---------------------------------

Alternatively, you can isolate your Mofka installation in a Spack environment as follows
(replace *myenv* with the name you want to give the environment).

.. code-block:: bash

   spack env create myenv
   spack env activate myenv
   git clone https://github.com/mochi-hpc/mochi-spack-packages.git
   spack repo add mochi-spack-packages
   spack add mofka
   spack install

It is generally recommended to use Spack environment to better manage package isolation.
Adding *mochi-spack-packages* this way also ensures that it is added only to the environment
and not globally.


Installation options
--------------------

Mofka will be built with Python support by default. This Python support provides
not just a Python-based client API for Mofka, but also a command-line interface
to manage deployments. It is therefore recommended to keep this option on.
Should you wish to disable it, simply install `mofka~python`.
