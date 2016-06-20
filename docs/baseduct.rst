.. Ductworks documentation master file, created by
   sphinx-quickstart on Sun Jun 19 17:54:39 2016.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

.. _base_duct_docs:

Ductworks Base Classes and Socket Ducts
=======================================
 
This page documents the API for the ductworks.base_duct module.

Base Duct Objects
====================

Parent Raw Socket Duct
----------------------

.. autoclass:: ductworks.base_duct.RawDuctParent
   :members:

Child Raw Socket Duct
---------------------
.. autoclass:: ductworks.base_duct.RawDuctChild
   :members:

Supporting Functions and Datastructures
=======================================

.. autofunction:: ductworks.base_duct.unix_domain_socket_constructor

.. autofunction:: ductworks.base_duct.tcp_socket_constructor

.. autofunction:: ductworks.base_duct.unix_domain_socket_listener_destructor

.. autofunction:: ductworks.base_duct.tcp_socket_listener_destructor

.. autofunction:: ductworks.base_duct.client_socket_destructor

.. autoexception:: ductworks.message_duct.MessageProtocolException
   :members:

.. autoexception:: ductworks.base_duct.DuctworksException
   :members:

.. autoexception:: ductworks.base_duct.CommunicationFaultException
   :members:

.. autoexception:: ductworks.base_duct.AlreadyConnectedException
   :members:

.. autoexception:: ductworks.base_duct.NotConnectedException
   :members:

.. autoexception:: ductworks.base_duct.LocalSocketFault
   :members:
