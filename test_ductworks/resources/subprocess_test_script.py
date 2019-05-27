from ductworks.message_duct import MessageDuctChild
import sys

connect_address = sys.argv[1]
child_duct = MessageDuctChild.psuedo_anonymous_child_duct(connect_address)
child_duct.connect()
while True:
    if child_duct.poll(0.01):
        received_payload = child_duct.recv()
        if received_payload is None:
            sys.exit(0)
        else:
            child_duct.send(received_payload)
