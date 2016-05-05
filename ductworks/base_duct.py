import select
import socket
import os


class CommunicationFaultException(Exception):
    pass


def psuedo_anon_uds_constructor():
    new_socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    new_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    # TODO: set linger
    return new_socket


def psuedo_anon_uds_listener_destructor(listener_socket):
    socket_address = listener_socket.getsockname()
    listener_socket.shutdown(socket.SHUT_RDWR)
    listener_socket.close()
    try:
        os.unlink(socket_address)
    except OSError:
        pass


def psuedo_anon_uds_client_destructor(client_socket):
    client_socket.shutdown(socket.SHUT_RDWR)
    client_socket.close()


class RawDuctParent(object):
    def __init__(self, bind_address, server_listener_socket_constructor=psuedo_anon_uds_constructor,
                 server_listener_socket_destructor=psuedo_anon_uds_listener_destructor,
                 server_connection_socket_destructor=psuedo_anon_uds_client_destructor):
        self.server_listener_socket_constructor = server_listener_socket_constructor
        self.server_listener_socket_destructor = server_listener_socket_destructor
        self.server_connection_socket_destructor = server_connection_socket_destructor
        self.bind_address = bind_address
        self.listener_socket = None
        self.conn_socket = None

    def bind(self, listen_queue_depth=1):
        assert not self.conn_socket
        if not self.listener_socket:
            self.listener_socket = self.server_listener_socket_constructor()
            self.listener_socket.bind(self.bind_address)
            self.listener_socket.listen(listen_queue_depth)

    def listen(self, timeout=60):
        assert not self.conn_socket
        if not self.listener_socket:
            self.bind()
        listener_fd = self.listener_socket.fileno()
        has_conn, _, is_faulted = map(bool, select.select([listener_fd], [], [listener_fd], timeout))
        if is_faulted:
            self.server_listener_socket_destructor(self.listener_socket)
            raise CommunicationFaultException("Bind socket faulted!")
        elif has_conn:
            self.conn_socket, _ = self.listener_socket.accept()
            self.server_listener_socket_destructor(self.listener_socket)
            self.listener_socket = None
        return bool(self.conn_socket)

    def send(self, byte_array, flags=None):
        assert self.conn_socket
        if flags is None:
            return self.conn_socket.send(byte_array)
        else:
            return self.conn_socket.send(byte_array, flags)

    def recv(self, buff_size, flags=None):
        assert self.conn_socket
        if flags is None:
            return self.conn_socket.recv(buff_size)
        else:
            return self.conn_socket.recv(buff_size, flags)
        
    def recv_into(self, buffer, recv_num_bytes=None, flags=None):
        assert self.conn_socket
        if recv_num_bytes is None and flags is None:
            return self.conn_socket.recv_into(buffer)
        elif flags is None and recv_num_bytes is not None:
            return self.conn_socket.recv_into(buffer, recv_num_bytes)
        elif flags is not None and recv_num_bytes is not None:
            return self.conn_socket.recv_into(buffer, recv_num_bytes, flags)            
        else:
            assert False

    def poll(self, timeout=60):
        assert self.conn_socket
        read_ready_sockets, _, _ = select.select([self.conn_socket], [], [], timeout)
        return bool(read_ready_sockets)

    def close(self):
        if self.listener_socket is not None:
            self.server_listener_socket_destructor(self.listener_socket)
            self.listener_socket = None
        if self.conn_socket is not None:
            self.server_connection_socket_destructor(self.conn_socket)
            self.conn_socket = None

    def __del__(self):
        self.close()


class RawDuctChild(object):
    def __init__(self, connect_address, socket_constructor=psuedo_anon_uds_constructor,
                 socket_destructor=psuedo_anon_uds_client_destructor):
        self.socket_constructor = socket_constructor
        self.socket_desctructor = socket_destructor
        self.connect_address = connect_address
        self.socket = None

    def connect(self):
        assert not self.socket
        self.socket = self.socket_constructor()
        self.socket.connect(self.connect_address)

    def send(self, byte_array, flags=None):
        assert self.socket
        if flags is None:
            return self.socket.send(byte_array)
        else:
            return self.socket.send(byte_array, flags)

    def recv(self, buff_size, flags=None):
        assert self.socket
        if flags is None:
            return self.socket.recv(buff_size)
        else:
            return self.socket.recv(buff_size, flags)

    def recv_into(self, buffer, recv_num_bytes=None, flags=None):
        assert self.socket
        if recv_num_bytes is None and flags is None:
            return self.socket.recv_into(buffer)
        elif flags is None and recv_num_bytes is not None:
            return self.socket.recv_into(buffer, recv_num_bytes)
        elif flags is not None and recv_num_bytes is not None:
            return self.socket.recv_into(buffer, recv_num_bytes, flags)            
        else:
            assert False

    def poll(self, timeout=60):
        assert self.socket
        read_ready_sockets, _, _ = select.select([self.socket], [], [], timeout)
        return bool(read_ready_sockets)

    def close(self):
        if self.socket is not None:
            self.socket_desctructor(self.socket)
            self.socket = None

    def __del__(self):
        self.close()
