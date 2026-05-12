Installing Mofka
================

Like all Mochi packages, Mofka can be built using `Spack <https://spack.io/>`_.
After you have installed and setup spack and added the diaspora-spack-packages
repository as explained
`here <https://diaspora-stream-api.readthedocs.io/en/latest/usage/installation.html>`_,
add the *mochi-spack-packages* repository as follows.

.. code-block:: bash

   git clone https://github.com/mochi-hpc/mochi-spack-packages.git
   spack repo add mochi-spack-packages/spack_repo/mochi

We recommend that you update your clone of this repository (and the diaspora-spack-packages
resoritory) from time to time to get the latest versions of all the dependencies.

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
   spack repo add https://github.com/mochi-hpc/mochi-spack-packages.git
   spack repo add https://github.com/diaspora-project/diaspora-spack-packages.git
   spack add mofka
   spack install

It is generally recommended to use a Spack environment to better manage package isolation.
Adding *mochi/diaspora-spack-packages* this way also ensures that it is added only to the
environment and not globally.
