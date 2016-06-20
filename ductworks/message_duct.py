try:
    import anyjson as json
except ImportError:
    import json
import struct
import codecs
from binascii import hexlify
from tempfile import NamedTemporaryFile

from ductworks.base_duct import RawDuctParent, RawDuctChild, tcp_socket_constructor,\
    tcp_socket_listener_destructor, DuctworksException


MAGIC_BYTE = b'\x54'


class MessageProtocolException(DuctworksException):
    pass


class RemoteDuctClosed(EOFError, DuctworksException):
    pass


def serializer_with_encoder_constructor(serialization_func, encoder_type='utf-8', encoder_error_mode='strict'):
    """
    Wrap a serialization function with string encoding. This is important for JSON, as it serializes objects into
    strings (potentially unicode), NOT bytestreams. An extra encoding step is needed to get to a bytestream.

    :param serialization_func: The base serialization function.
    :param encoder_type: The encoder type. Default: 'utf-8'
    :param encoder_error_mode: The encode error mode. Default: 'strict'.
    :return: The serializer function wrapped with specified encoder.
    :rtype: T -> bytes | bytearray | str
    """
    encoder = codecs.getencoder(encoder_type)

    def serialize(payload):
        serialized, _ = encoder(serialization_func(payload), encoder_error_mode)
        return serialized
    return serialize


def deserializer_with_decoder_constructor(deserialization_func, decoder_type='utf-8', decoder_error_mode='replace'):
    """
    Wrap a deserialization function with string encoding. This is important for JSON, as it expects to operate on
    strings (potentially unicode), NOT bytetsteams. A decoding steps is needed in between.

    :param deserialization_func: The base deserialization function.
    :param decoder_type: The decoder type. Default: 'utf-8'
    :param decoder_error_mode: The decode error mode. Default: 'replace'.
    :return: The deserializer function wrapped with specified decoder.
    :rtype: bytes | bytearray | str -> T
    """
    decoder = codecs.getdecoder(decoder_type)

    def deserialize(payload):
        decoded, _ = decoder(payload, decoder_error_mode)
        return deserialization_func(decoded)
    return deserialize


default_serializer = serializer_with_encoder_constructor(json.dumps)
default_deserializer = deserializer_with_decoder_constructor(json.loads)


class MessageDuctParent(object):
    """
    The MessageDuctParent is an abstraction over the SocketDuctParent and provides an interface compatible
    with Python's multiprocessing.Connection (created by multiprocessing.Pipe). The Message Duct, much like
    multiprocessing Connections, can send native data structures over the wire and does so without the developer
    worrying about how to make the underlying stream based connections work correctly with arbitrary-size message
    based semantics. This means for the end user, a single send() is all that is needed to send data over the
    connection, and a single recv() on the other side will produce the correct data structure without any additional
    hassle.

    Unlike multiprocessing.Connection, MessageDuctParent supports some additional features; first, while a reasonable
    default choice is made for serialization and deserialization (UTF-8 encoded JSON), the user may specify the
    serialization and deserialization routines; this can include higher performance or more Python-centric
    serialization libraries such as marshal, pickle, or dill, or general high performance serialization libraries
    such as msgpack. It would not be outside of the realm of possibility to bolt-in other serialization libraries
    such as Cap'n Proto or messagebuffers though these would not be a direct fit because of the differing paradigms.

    Additional the message ducts allow an arbitrary lock structure to be used when communicating, which the sole
    requirement being that it must obey the same semantics as Python's built in lock structures. This can be as simple
    as using the anonymous POSIX thread locking (threading.Lock) to eliminate race conditions when the duct is shared
    between competing threads, to using multiprocessing.Lock, a wrapper around flock(2), or a named POSIX locks library
    to synchronize access between local processes. It is even conceivable to bolt-in distributed lock systems, like
    etcd.

    Finally, the Message Duct retains the full power of the underlying Socket Duct to pick and choose the connection
    constructor and destructor, which means that both TCP and stream-oriented Unix Domain Sockets are provided and
    supported, and other networking libraries such as nanomsg or ZeroMQ could also be used with minimal extra work
    if needed. This enables this library to allow simple pipe like semantics even when both ends are children of
    the same parent process, or even located on the same physical system.

    This side of the message duct must listen and wait for a MessageDuctChild to connect in order
    to begin communication.
    """

    def __init__(self, socket_duct, serialize=default_serializer, deserialize=default_deserializer, lock=None):
        self.socket_duct = socket_duct
        self.serialize = serialize
        self.deserialize = deserialize
        self.lock = lock

    @property
    def bind_address(self):
        """
        Access the address specified by the user to bind to. This may not be
        the actual address the parent is/was listening on.

        :return: The bound address
        :rtype: (str, int) | str
        """
        return self.socket_duct.bind_address

    @property
    def listener_address(self):
        """
        Access the actual address the parent socket is/was listening on.

        :return: The bound address
        :rtype: (str, int) | str
        """
        return self.socket_duct.listener_address

    def fileno(self):
        """
        Get the file descriptor for the connection socket in the socket duct.
        This is useful for integrating into other event loops.

        :return: The connection file descriptor.
        :rtype: int
        """
        return self.socket_duct.fileno()

    @classmethod
    def psuedo_anonymous_parent_duct(cls, bind_address=None, serialize=default_serializer,
                                     deserialize=default_deserializer, lock=None,
                                     timeout=RawDuctParent.DEFAULT_TIMEOUT):
        """
        Create a new psuedo-anonymous parent message duct with Unix Domain sockets.

        :param bind_address: The (filesystem) address to listen on, if any. If None a new random address will be
            chosen. Default: None.
        :type bind_address: basestring | None
        :param serialize: The serialization function for sending messages. Default: Encoded JSON.
        :param deserialize: The deserialization function for sending messages. Default: Encoded JSON.
        :param lock: A lock object to lock send/recv calls.
        :param timeout: The number of seconds to block a send/recv call waiting for completion.
        :type timeout: int | float
        :return: A new MessageDuctParent.
        :rtype: ductworks.message_duct.MessageDuctParent
        """
        if bind_address is None:
            tmp = NamedTemporaryFile()
            bind_address = tmp.name
            tmp.close()
        return cls(
            RawDuctParent(bind_address=bind_address, timeout=timeout),
            serialize=serialize, deserialize=deserialize, lock=lock
        )

    @classmethod
    def psuedo_anonymous_tcp_parent_duct(cls, bind_address='localhost', bind_port=0, serialize=default_serializer,
                                         deserialize=default_deserializer, lock=None,
                                         timeout=RawDuctParent.DEFAULT_TIMEOUT):
        """
        Create a new psuedo-anonymous parent message duct with TCP sockets.

        :param bind_address: The interface address to listen on, if any. Default: 'localhost'.
        :type bind_address: basestring
        :param bind_port: The port to bind the interface to. If 0, pick the a random open port. Default: 0.
        :type bind_port: int
        :param serialize: The serialization function for sending messages. Default: Encoded JSON.
        :param deserialize: The deserialization function for sending messages. Default: Encoded JSON.
        :param lock: A lock object to lock send/recv calls.
        :param timeout: The number of seconds to block a send/recv call waiting for completion.
        :type timeout: int | float
        :return: A new MessageDuctParent.
        :rtype: ductworks.message_duct.MessageDuctParent
        """

        return cls(
            RawDuctParent(
                bind_address=(bind_address, bind_port),
                server_listener_socket_constructor=tcp_socket_constructor,
                server_listener_socket_destructor=tcp_socket_listener_destructor,
                timeout=timeout
            ),
            serialize=serialize, deserialize=deserialize, lock=lock
        )

    def bind(self):
        """
        Bind the underlying socket duct.
        :return: None
        :rtype: NoneType
        """
        return self.socket_duct.bind()

    def listen(self):
        """
        Listen on the underyling socket duct.
        :return: None
        :rtype: NoneType
        """
        return self.socket_duct.listen()

    def poll(self, timeout=60):
        """
        Poll the underlying socket duct to check for new messages.
        :param timeout: The amount of time to wait for a new message, if none is present. Default: 60 seconds.
        :type timeout: int
        :return: True if a message is waiting, False otherwise.
        :rtype: bool
        """
        return self.socket_duct.poll(timeout)

    def send(self, payload):
        """
        Send a payload to the other end, if connected.

        :param payload: A serializable Python object to send to the other duct.
        :return: None
        :rtype: NoneType
        """
        send_lock = self.lock
        try:
            if send_lock:
                send_lock.acquire()
            serialized_payload = self.serialize(payload)
            payload_len = len(serialized_payload)
            full_message = bytearray(struct.pack('!cL', MAGIC_BYTE, payload_len))
            full_message.extend(serialized_payload)
            full_message_view = memoryview(full_message)
            while full_message_view:
                bytes_sent = self.socket_duct.send(full_message_view)
                full_message_view = full_message_view[bytes_sent:]
        finally:
            if send_lock:
                send_lock.release()

    def recv(self):
        """
        Receive a payload from the other end, if connected and data is present.

        :return: A deserialized Python object from the other end of the duct.
        """
        recv_lock = self.lock
        try:
            if recv_lock:
                recv_lock.acquire()
            leading_byte = self.socket_duct.recv(1)
            if not leading_byte:
                raise RemoteDuctClosed("Remote duct closed.")
            elif leading_byte != MAGIC_BYTE:
                raise MessageProtocolException("Invalid magic byte at message envelope head! Expected: {}, got: {}"
                                               "".format(hexlify(MAGIC_BYTE), hexlify(leading_byte)))
            raw_payload_len = bytearray(struct.calcsize('!L'))
            raw_payload_len_view = memoryview(raw_payload_len)
            while raw_payload_len_view:
                num_bytes_received = self.socket_duct.recv_into(raw_payload_len_view)
                if num_bytes_received == 0:
                    raise RemoteDuctClosed("Remote duct closed mid-message!")
                raw_payload_len_view = raw_payload_len_view[num_bytes_received:]
            incoming_payload_len, = struct.unpack('!L', bytes(raw_payload_len))
            serialized_payload = bytearray(incoming_payload_len)
            serialized_payload_view = memoryview(serialized_payload)
            while serialized_payload_view:
                num_bytes_received = self.socket_duct.recv_into(serialized_payload_view)
                if num_bytes_received == 0:
                    raise RemoteDuctClosed("Remote duct closed mid-message!")
                serialized_payload_view = serialized_payload_view[num_bytes_received:]
            return self.deserialize(serialized_payload)
        finally:
            if recv_lock:
                recv_lock.release()

    def close(self):
        """
        Close the underlying socket duct.
        :return: None
        """
        self.socket_duct.close()

    def __del__(self):
        self.close()


class MessageDuctChild(object):
    """
    The MessageDuctChild is an abstraction over the SocketDuctChild and provides an interface compatible
    with Python's multiprocessing.Connection (created by multiprocessing.Pipe).

    This side must connect to a listening MessageDuctParent in order to begin communication.
    """
    def __init__(self, socket_duct, serialize=default_serializer, deserialize=default_deserializer, lock=None):
        self.socket_duct = socket_duct
        self.serialize = serialize
        self.deserialize = deserialize
        self.lock = lock

    @property
    def connect_address(self):
        return self.socket_duct.connect_address

    def fileno(self):
        """
        Get the file descriptor for the connection socket in the socket duct.
        This is useful for integrating into other event loops.

        :return: The connection file descriptor.
        :rtype: int
        """
        return self.socket_duct.fileno()

    @classmethod
    def psuedo_anonymous_child_duct(cls, connect_address, serialize=default_serializer,
                                    deserialize=default_deserializer, lock=None,
                                    timeout=RawDuctChild.DEFAULT_TIMEOUT):
        """
        Create a new psuedo-anonymous child message duct with Unix Domain sockets. The connect_address parameter
        should be sourced from the parent duct by getting its listener_address property.

        :param connect_address: The filesystem address to connect to. Get this from parent_duct.listener_address.
        :type connect_address: basestring
        :param serialize: The serialization function for sending messages. Default: Encoded JSON.
        :param deserialize: The deserialization function for sending messages. Default: Encoded JSON.
        :param lock: A lock object to lock send/recv calls.
        :param timeout: The number of seconds to block a send/recv call waiting for completion.
        :type timeout: int | float
        :return: A new MessageParentDuct.
        :rtype: ductworks.message_duct.MessageDuctChild
        """
        return cls(
            RawDuctChild(
                connect_address, timeout=timeout
            ),
            serialize=serialize, deserialize=deserialize, lock=lock
        )

    @classmethod
    def psuedo_anonymous_tcp_child_duct(cls, connect_address, connect_port, serialize=default_serializer,
                                        deserialize=default_deserializer, lock=None,
                                        timeout=RawDuctChild.DEFAULT_TIMEOUT):
        """
        Create a new psuedo-anonymous child message duct with Unix Domain sockets. The connect_address and
        connect_port parameters should be sourced from the parent duct by getting its listener_address property.

        :param connect_address: The interface address to connect to. Get this from parent_duct.listener_address[0].
        :type connect_address: basestring
        :param connect_port: The TCP port to connect to. Get this from parent_duct.listener_address[1].
        :type connect_port: int
        :param serialize: The serialization function for sending messages. Default: Encoded JSON.
        :param deserialize: The deserialization function for sending messages. Default: Encoded JSON.
        :param lock: A lock object to lock send/recv calls.
        :param timeout: The number of seconds to block a send/recv call waiting for completion.
        :type timeout: int | float
        :return: A new MessageParentDuct.
        :rtype: ductworks.message_duct.MessageDuctChild
        """

        return cls(
            RawDuctChild(
                connect_address=(connect_address, connect_port),
                socket_constructor=tcp_socket_constructor,
                timeout=timeout
            ),
            serialize=serialize, deserialize=deserialize, lock=lock
        )

    def connect(self):
        """
        Call connect() on the underlying socket duct.
        :return: None
        """
        return self.socket_duct.connect()

    def poll(self, timeout=60):
        """
        Poll the underlying socket duct to check for new messages.
        :param timeout: The amount of time to wait for a new message, if none is present. Default: 60 seconds.
        :type timeout: int
        :return: True if a message is waiting, False otherwise.
        :rtype: bool
        """
        return self.socket_duct.poll(timeout)

    def send(self, payload):
        """
        Send a payload to the other end, if connected.

        :param payload: A serializable Python object to send to the other duct.
        :return: None
        """
        send_lock = self.lock
        try:
            if send_lock:
                send_lock.acquire()
            serialized_payload = self.serialize(payload)
            payload_len = len(serialized_payload)
            full_message = bytearray(struct.pack('!cL', MAGIC_BYTE, payload_len))
            full_message.extend(serialized_payload)
            full_message_view = memoryview(full_message)
            while full_message_view:
                bytes_sent = self.socket_duct.send(full_message_view)
                full_message_view = full_message_view[bytes_sent:]
        finally:
            if send_lock:
                send_lock.release()

    def recv(self):
        """
        Receive a payload from the other end, if connected and data is present.

        :return: A deserialized Python object from the other end of the duct.
        """
        recv_lock = self.lock
        try:
            if recv_lock:
                recv_lock.acquire()
            leading_byte = self.socket_duct.recv(1)
            if not leading_byte:
                raise RemoteDuctClosed("Remote duct closed.")
            elif leading_byte != MAGIC_BYTE:
                raise MessageProtocolException("Invalid magic byte at message envelope head! Expected: {}, got: {}"
                                               "".format(hexlify(MAGIC_BYTE), hexlify(leading_byte)))
            raw_payload_len = bytearray(struct.calcsize('!L'))
            raw_payload_len_view = memoryview(raw_payload_len)
            while raw_payload_len_view:
                num_bytes_received = self.socket_duct.recv_into(raw_payload_len_view)
                if num_bytes_received == 0:
                    raise RemoteDuctClosed("Remote duct closed mid-message!")
                raw_payload_len_view = raw_payload_len_view[num_bytes_received:]
            incoming_payload_len, = struct.unpack('!L', bytes(raw_payload_len))
            serialized_payload = bytearray(incoming_payload_len)
            serialized_payload_view = memoryview(serialized_payload)
            while serialized_payload_view:
                num_bytes_received = self.socket_duct.recv_into(serialized_payload_view)
                if num_bytes_received == 0:
                    raise RemoteDuctClosed("Remote duct closed mid-message!")
                serialized_payload_view = serialized_payload_view[num_bytes_received:]
            return self.deserialize(serialized_payload)
        finally:
            if recv_lock:
                recv_lock.release()

    def close(self):
        """
        Close the underlying socket duct.
        :return: None
        """
        self.socket_duct.close()

    def __del__(self):
        self.close()


def create_psuedo_anonymous_duct_pair(serialize=default_serializer, deserialize=default_deserializer,
                                      parent_lock=None, child_lock=None):
    """
    Create an already connected pair of anonymous ducts. This is very similar to how multiprocess.Pipe(True) functions.

    :param serialize: The serializer function for the pair. Defaults to encoded JSON.
    :param deserialize: The deserializer funtion for the pair. Defaults to encoded JSON.
    :param parent_lock: An optional lock object to give to the "parent" duct.
    :param child_lock: An optional lock object to give to the "child" duct.
    :return: A parent/child pair of ducts.
    :rtype: (ductworks.message_duct.MessageDuctParent, ductworks.message_duct.MesssageDuctChild)
    """

    parent = MessageDuctParent.psuedo_anonymous_parent_duct(
        serialize=serialize, deserialize=deserialize, lock=parent_lock
    )
    parent.bind()
    listener_address = parent.listener_address
    child = MessageDuctChild.psuedo_anonymous_child_duct(
        listener_address, serialize=serialize, deserialize=deserialize, lock=child_lock
    )
    child.connect()
    parent.listen()
    return parent, child
