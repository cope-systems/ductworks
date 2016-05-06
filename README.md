# Ductworks

### Simple, capable IPC library for less simple situations

Ductworks is an IPC library for Python, similar to the Pipe object from the multiprocessing library. Like Pipe,
ductworks exposes a MessageDuct API with very simple send and receive semantics for end users (no worrying about 
streams, each send call corresponds to one recv call), while easily allowing for more flexible usage like having
a communication channel between a parent python process and a python process in a child subprocess (separated by 
both fork(2) and exec(2)). As well ductworks allows users to freely choose their serialization library (and uses JSON
by default) and uses regular BSD sockets; this makes connecting up programs in different languages (or even on 
different systems) relatively straightforward. Ductworks aims to make it simple to get socket pairs, when inheriting
a anonymous socket pair is difficult or outright infeasible, and to make IPC easier and less stressful.

### Example

    ```
    from ductworks.message_duct import create_psuedo_anonymous_duct_pair
    from ductworks.message_duct import MessageDuctParent, MessageDuctChild
    
    # Build a simple local pair of message ducts
    parent, child = create_psuedo_anonymous_duct_pair()
    parent.send("hello world")
    assert child.recv() == "hello world"
    parent.close()
    child.close()
    
    # For situations where a child can't be strictly inherited, create a parent and bind
    parent = MessageDuctParent.psuedo_anonymous_parent_duct()
    parent.bind()
    
    # Get the address for the child client to connect to.
    connect_address = parent.listener_address
    
    # Build and connect the child.
    child = MessageDuctChild.psuedo_anonymous_child_duct(connect_address)
    child.connect()
    
    # Finalize the connection on the parent side:
    assert parent.listen()
    
    # Send data freely
    parent, child = create_psuedo_anonymous_duct_pair()
    parent.send("hello world")
    assert child.recv() == "hello world"
    child.send("hello this is dog")
    assert parent.recv() == "hello this is dog"
    parent.close()
    child.close()
    ```