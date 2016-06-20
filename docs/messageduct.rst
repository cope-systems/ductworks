.. Ductworks documentation master file, created by
   sphinx-quickstart on Sun Jun 19 17:54:39 2016.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

.. _message_duct_docs:


Ductworks Message Ducts
=======================
 
This page documents a few examples around the ductworks.message_duct module as well
as the API docs for the objects contained in this module that are public facing.

Examples
--------

Creating a Local Anonymous Duct Pair
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

This creates a duct pair that is a drop in interface-compatible replacement
for multiprocessing.Connections created via the multiprocessing.Pipe routine.

.. code-block:: python

    from ductworks.message_duct import create_psuedo_anonymous_duct_pair

    # Create a new duct pair, much as you might a connection pair
    parent_duct, child_duct = create_psuedo_anonymous_duct_pair()

    # Send the message across the wire
    parent_duct.send("hello!")

    # Wait for a message to come across
    assert child_duct.poll(1)    
    received = child_duct.recv()

    # Check to ensure we've received the right data and send it back
    assert received == "hello!"
    child_duct.send(received)

    # Wait for parent to get data back
    assert parent_duct.poll(1)
    received = parent_duct.recv()
     
    # Ensure that the bounced data is correct
    assert received == "hello!"

Creating an explicit TCP Duct Pair
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

For the situations that Ductworks usually targets, you'll want to instantiate the Ducts
individually. For example, if we wanted to open a Duct up over TCP (for potentially
remote systems), we can do as follows:

.. code-block:: python
 
    from __future__ import print_function
    from ductworks.message_duct import MessageDuctParent, MessageDuctChild

    # Create an anonymous TCP parent duct
    # This is by default bound to the first available open port on the
    # "localhost" interface; if you want to talk to a remote system, you should
    # either specify the desired interface address (good) or "0.0.0.0" 
    # (potentially less good)
    parent_duct = MessageDuctParent.psuedo_anonymous_tcp_parent_duct()
    
    # Bind parent duct to begin listening for incoming connections
    parent_duct.bind()

    # Get the interface address and port from the parent_duct
    # Note that this may be different from the specified bind address
    # and that the proper address to connect to should usually be extracted
    # this way.
    parent_interface_address, parent_port = parent.listener_address

    # At some point later, the child is given the parent addresses
    # and begins to conenct
    child_duct = MessageDuctChild.psuedo_anonymous_tcp_child_duct(
        parent_interface_address, parent_port
    )
    child_duct.connect()

    # While the child is connecting, the parent will continue to listen until
    # The child is connected...
    while not parent.listen(1):
        # Check at 1 second intervals forever for a child to connect.
        pass

    # The parent and child may now exchange data 
    parent.send("test")

    assert child.poll(1)
    assert child.recv()

    # After communication is finished, the child and parent can be explicitly closed.
    child.close()

    # At this point, any attempt to send data in the parent will raise RemoteDuctClosed
    # which inherits from EOFError and Ductworks exception, making it again a drop-in
    # replacement for multiprocessing.Pipe
 
    try:
        parent.recv()
    except EOFError:
        print("parent.recv() triggered an EOFError! Other side must be closed!")
    else:
        # Shouldn't ever be here.
        assert False


Message Duct Objects
====================

Parent Message Duct
-------------------

.. autoclass:: ductworks.message_duct.MessageDuctParent
   :members:

Child Message Duct
------------------
.. autoclass:: ductworks.message_duct.MessageDuctChild
   :members:

Supporting Functions and Datastructures
=======================================

.. autofunction:: ductworks.message_duct.create_psuedo_anonymous_duct_pair

.. autoexception:: ductworks.message_duct.MessageProtocolException
   :members:

.. autoexception:: ductworks.message_duct.RemoteDuctClosed
   :members:

.. autofunction:: ductworks.message_duct.serializer_with_encoder_constructor

.. autofunction:: ductworks.message_duct.deserializer_with_decoder_constructor

.. autodata:: ductworks.message_duct.default_serializer

.. autodata:: ductworks.message_duct.default_deserializer

.. autodata:: ductworks.message_duct.MAGIC_BYTE

