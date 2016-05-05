try:
    import anyjson as json
except ImportError:
    import json
import struct
import codecs
from tempfile import NamedTemporaryFile

from ductworks.base_duct import RawDuctParent, RawDuctChild


def serializer_with_encoder_constructor(serialization_func, encoder_type='utf-8', encoder_error_mode='strict'):
    encoder = codecs.getencoder(encoder_type)

    def serialize(payload):
        serialized, _ = encoder(serialization_func(payload), encoder_error_mode)
        return serialized
    return serialize


def deserializer_with_encoder_constructor(deserialization_func, decoder_type='utf-8', decoder_error_mode='replace'):
    decoder = codecs.getdecoder(decoder_type)

    def deserialize(payload):
        decoded, _ = decoder(payload, decoder_error_mode)
        return deserialization_func(decoded)
    return deserialize


default_serializer = serializer_with_encoder_constructor(json.dumps)
default_deserializer = deserializer_with_encoder_constructor(json.loads)

MAGIC_BYTE = '\x54'


class MessageProtocolException(Exception):
    pass


class MessageDuctParent(object):
    def __init__(self, socket_duct, serialize=default_serializer, deserialize=default_deserializer, lock=None):
        self.socket_duct = socket_duct
        self.serialize = serialize
        self.deserialize = deserialize
        self.lock = lock

    @property
    def bind_address(self):
        return self.socket_duct.bind_address

    @classmethod
    def psuedo_anonymous_parent_duct(cls, bind_address=None, serialize=default_serializer,
                                     deserialize=default_deserializer, lock=None):
        if bind_address is None:
            tmp = NamedTemporaryFile()
            bind_address = tmp.name
            tmp.close()
        return cls(RawDuctParent(bind_address=bind_address), serialize=serialize, deserialize=deserialize, lock=lock)

    def bind(self):
        return self.socket_duct.bind()

    def listen(self):
        return self.socket_duct.listen()

    def poll(self, timeout=60):
        return self.socket_duct.poll(timeout)

    def send(self, payload):
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
        recv_lock = self.lock
        try:
            if recv_lock:
                recv_lock.acquire()
            leading_byte = self.socket_duct.recv(1)
            if leading_byte != MAGIC_BYTE:
                raise MessageProtocolException("Invalid magic byte at message envelope head! Expected: {}, got: {}"
                                               "".format(hex(ord(MAGIC_BYTE)), hex(ord(leading_byte))))
            raw_payload_len = bytearray(struct.calcsize('!L'))
            raw_payload_len_view = memoryview(raw_payload_len)
            while raw_payload_len_view:
                num_bytes_recieved = self.socket_duct.recv_into(raw_payload_len_view)
                raw_payload_len_view = raw_payload_len_view[num_bytes_recieved:]
            incoming_payload_len, = struct.unpack('!L', buffer(raw_payload_len))
            serialized_payload = bytearray(incoming_payload_len)
            serialized_payload_view = memoryview(serialized_payload)
            while serialized_payload_view:
                num_bytes_recieved = self.socket_duct.recv_into(serialized_payload_view)
                serialized_payload_view = serialized_payload_view[num_bytes_recieved:]
            return self.deserialize(serialized_payload)
        finally:
            if recv_lock:
                recv_lock.release()

    def close(self):
        self.socket_duct.close()

    def __del__(self):
        self.close()


class MessageDuctChild(object):
    def __init__(self, socket_duct, serialize=default_serializer, deserialize=default_deserializer, lock=None):
        self.socket_duct = socket_duct
        self.serialize = serialize
        self.deserialize = deserialize
        self.lock = lock

    @property
    def bind_address(self):
        return self.socket_duct.bind_address

    @classmethod
    def psuedo_anonymous_child_duct(cls, connect_address, serialize=default_serializer,
                                    deserialize=default_deserializer, lock=None):
        return cls(RawDuctChild(connect_address), serialize=serialize, deserialize=deserialize, lock=lock)

    def connect(self):
        return self.socket_duct.connect()

    def poll(self, timeout=60):
        return self.socket_duct.poll(timeout)

    def send(self, payload):
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
        recv_lock = self.lock
        try:
            if recv_lock:
                recv_lock.acquire()
            leading_byte = self.socket_duct.recv(1)
            if leading_byte != MAGIC_BYTE:
                raise MessageProtocolException("Invalid magic byte at message envelope head! Expected: {}, got: {}"
                                               "".format(hex(ord(MAGIC_BYTE)), hex(ord(leading_byte))))
            raw_payload_len = bytearray(struct.calcsize('!L'))
            raw_payload_len_view = memoryview(raw_payload_len)
            while raw_payload_len_view:
                num_bytes_recieved = self.socket_duct.recv_into(raw_payload_len_view)
                raw_payload_len_view = raw_payload_len_view[num_bytes_recieved:]
            incoming_payload_len, = struct.unpack('!L', buffer(raw_payload_len))
            serialized_payload = bytearray(incoming_payload_len)
            serialized_payload_view = memoryview(serialized_payload)
            while serialized_payload_view:
                num_bytes_recieved = self.socket_duct.recv_into(serialized_payload_view)
                serialized_payload_view = serialized_payload_view[num_bytes_recieved:]
            return self.deserialize(serialized_payload)
        finally:
            if recv_lock:
                recv_lock.release()

    def close(self):
        self.socket_duct.close()

    def __del__(self):
        self.close()


def create_psuedo_anonymous_duct_pair(serialize=default_serializer, deserialize=default_deserializer,
                                      parent_lock=None, child_lock=None):
    parent = MessageDuctParent.psuedo_anonymous_parent_duct(
        serialize=serialize, deserialize=deserialize, lock=parent_lock
    )
    parent.bind()
    bind_address = parent.bind_address
    child = MessageDuctChild.psuedo_anonymous_child_duct(
        bind_address, serialize=serialize, deserialize=deserialize, lock=child_lock
    )
    child.connect()
    parent.listen()
    return parent, child
