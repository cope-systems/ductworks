import select
import socket
import os
import struct
import errno
import time


class DuctworksException(Exception):
    pass


class CommunicationFaultException(DuctworksException):
    pass


class AlreadyConnectedException(DuctworksException):
    pass


class NotConnectedException(DuctworksException):
    pass


class RemoteSocketClosed(DuctworksException):
    pass


def unix_domain_socket_constructor(linger_time=3):
    """
    Create a new UDS streaming socket with reasonable socket options set.

    :param linger_time: The linger time after closing the socket to allow buffers to flush. Default: 3
    :type linger_time: int
    :return: A new Unix Domain socket.
    :rtype: socket.socket
    """
    new_socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    new_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    new_socket.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack('ii', 1, linger_time))
    return new_socket


def tcp_socket_constructor(linger_time=10, tcp_no_delay=1):
    """
    Create a new TCP socket with reasonable socket options set.

    :param linger_time: The linger time after closing the socket to allow buffers to flush. Default: 10
    :type linger_time: int
    :param tcp_no_delay: 1 -> disable Nagle's algorithm on the created socket, 0 -> enable Nagle's algorithm.
        Default: 1
    :type tcp_no_delay: int
    :return: A new TCP socket.
    :rtype socket.socket
    """
    new_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    new_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    new_socket.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack('ii', 1, linger_time))
    # Because everything is sent in a single send call, and we're sending "messages", not really "streams",
    # we turn off Nagle's algorithm to make performance a little better. This might we worth thinking about more though.
    new_socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, tcp_no_delay)
    return new_socket


def unix_domain_socket_listener_destructor(listener_socket, shutdown=False, shutdown_mode=socket.SHUT_RDWR):
    """
    Close and clean up a Unix Domain listener socket.

    :param listener_socket: The socket to close down.
    :type listener_socket: socket.socket
    :param shutdown: Should shutdown be performed on the socket? (Usually no). Default: False
    :type shutdown: bool
    :param shutdown_mode: The parameters to be passed to the socket's shutdown function.
    :type shutdown_mode: int
    :return: None
    """
    socket_address = listener_socket.getsockname()
    if shutdown:
        listener_socket.shutdown(shutdown_mode)
    listener_socket.close()
    try:
        os.unlink(socket_address)
    except OSError:
        pass


def tcp_socket_listener_destructor(listener_socket, shutdown=False, shutdown_mode=socket.SHUT_RDWR):
    """
    Close and clean up a TCP listener socket.

    :param listener_socket: The socket to close down.
    :type listener_socket: socket.socket
    :param shutdown: Should shutdown be performed on the socket? (Usually no). Default: False
    :type shutdown: bool
    :param shutdown_mode: The parameters to be passed to the socket's shutdown function.
    :type shutdown_mode: int
    :return: None
    """
    if shutdown:
        listener_socket.shutdown(shutdown_mode)
    listener_socket.close()


def client_socket_destructor(client_socket, shutdown=False, shutdown_mode=socket.SHUT_RDWR):
    """
    Close and clean up a client socket.

    :param client_socket: The socket to close down.
    :type client_socket: socket.socket
    :param shutdown: Should shutdown be performed on the socket? (Usually no). Default: False
    :type shutdown: bool
    :param shutdown_mode: The parameters to be passed to the socket's shutdown function.
    :type shutdown_mode: int
    :return: None
    """
    if shutdown:
        client_socket.shutdown(shutdown_mode)
    client_socket.close()


class RawDuctParent(object):
    """
    The RawDuctParent is a thin wrapper over top of a "server" socket, that uses the socket in an "anonymous" way.
    Once the first connection is seen, the listener socket is closed, and the "parent" and "child" act like an
    anonymous socket pair (with a few extra niceties, like a poll method).
    """

    DEFAULT_TIMEOUT = 30

    def __init__(self, bind_address, server_listener_socket_constructor=unix_domain_socket_constructor,
                 server_listener_socket_destructor=unix_domain_socket_listener_destructor,
                 server_connection_socket_destructor=client_socket_destructor, timeout=DEFAULT_TIMEOUT):
        self.server_listener_socket_constructor = server_listener_socket_constructor
        self.server_listener_socket_destructor = server_listener_socket_destructor
        self.server_connection_socket_destructor = server_connection_socket_destructor
        self.bind_address = bind_address
        self.listener_address = None
        self.listener_socket = None
        self.conn_socket = None
        self.socket_timeout = timeout

    def bind(self, listen_queue_depth=1):
        """
        Create and bind the listener socket, if this hasn't been done already.

        This will raise AlreadyConnectedException if the steady-state connection socket has already been created.

        :param listen_queue_depth: The queue depth for the listener socket. Default: 1
        :return: None
        """
        if self.conn_socket is not None:
            raise AlreadyConnectedException("Already connected to other end!")
        if self.listener_socket is None:
            self.listener_socket = self.server_listener_socket_constructor()
            self.listener_socket.settimeout(self.socket_timeout)
            self.listener_socket.bind(self.bind_address)
            self.listener_address = self.listener_socket.getsockname()
            self.listener_socket.listen(listen_queue_depth)

    def listen(self, timeout=60):
        """
        Listen for incoming connections from an incoming child duct.
        This will bind if it hasn't been done already.

        This will raise AlreadyConnectedException if the steady-state connection socket has already been created.

        :param timeout: Amount of time to wait for the other end to connect before giving up.
        :param timeout: float | int
        :return: True if a connection was received and connected, False otherwise.
        :rtype: bool
        """
        if self.conn_socket is not None:
            raise AlreadyConnectedException("Already connected to other end!")
        if self.listener_socket is None:
            self.bind()
        listener_fd = self.listener_socket.fileno()
        has_conn, _, is_faulted = map(bool, select.select([listener_fd], [], [listener_fd], timeout))
        if is_faulted:
            self.server_listener_socket_destructor(self.listener_socket, shutdown=True)
            raise CommunicationFaultException("Bind socket faulted!")
        elif has_conn:
            self.conn_socket, _ = self.listener_socket.accept()
            self.conn_socket.settimeout(self.socket_timeout)
            self.server_listener_socket_destructor(self.listener_socket, shutdown=True)
            self.listener_socket = None
        return bool(self.conn_socket)

    def send(self, byte_array, flags=None):
        """
        Send data to the other end of the duct. NOTE: This send call is a thin wrapper over the underlying socket
        send call, and is thus _stream oriented_. A single send call may _not_ send all the data given to the method.
        It is your responsibility to add additional semantics and metadata to get "message-based" send/recv, and to
        check from the return value how many bytes were _actually_ sent.

        A NotConnectedException is raised if the duct hasn't been bound to the other end yet.

        :param byte_array: The bytes-like data to send to the other end.
        :type byte_array: bytearray | buffer | str | bytes
        :param flags: Optional flags to be set on the socket send call.
        :param flags: bytearray | buffer | str | bytes | None
        :return: The number of bytes sent.
        :rtype: int
        """
        if self.conn_socket is None:
            raise NotConnectedException("Must be connected to other end to send data!")
        if flags is None:
            return self.conn_socket.send(byte_array)
        else:
            return self.conn_socket.send(byte_array, flags)

    def recv(self, buff_size, flags=None):
        """
        Receive up to buff_size bytes from the remote host.

        A NotConnectedException is raised if the duct hasn't been bound to the other end yet.

        :param buff_size: The maximum number of bytes to read off the socket buffer from the remote host.
        :type buff_size: int
        :param flags: Optional flags to be set on the socket recv call.
        :param flags: bytearray | buffer | str | bytes | None
        :return: A bytes-like object that contains data from the remote host.
        :rtype: bytearray | buffer | str | bytes
        """
        if self.conn_socket is None:
            raise NotConnectedException("Must be connected to other end to receive data!")
        if flags is None:
            return self.conn_socket.recv(buff_size)
        else:
            return self.conn_socket.recv(buff_size, flags)
        
    def recv_into(self, buffer, recv_num_bytes=None, flags=None):
        """
        Receive up to buff_size bytes from the remote host. A buffer is given to write directly into to reduce the
        memory overhead of recv (one less copy needed).

        A NotConnectedException is raised if the duct hasn't been bound to the other end yet.

        :param buffer: An object that implements the buffer interface and can have data written directly into it.
        :type buffer: buffer
        :param recv_num_bytes: The maximum number of bytes to read off the socket buffer from the remote host.
            If this is not set, it is calculated from the length of the buffer passed in.
        :type recv_num_bytes: int | Nonre
        :param flags: Optional flags to be set on the socket recv call.
        :param flags: bytearray | buffer | str | bytes | None
        :return: The number of bytes read from the socket's recv buffer from the remote host.
        :rtype: int
        """
        if self.conn_socket is None:
            raise NotConnectedException("Must be connected to other end to receive data!")
        if recv_num_bytes is None and flags is None:
            return self.conn_socket.recv_into(buffer)
        elif flags is None and recv_num_bytes is not None:
            return self.conn_socket.recv_into(buffer, recv_num_bytes)
        elif flags is not None and recv_num_bytes is not None:
            return self.conn_socket.recv_into(buffer, recv_num_bytes, flags)            
        else:
            assert False

    def poll(self, timeout=60):
        """
        Poll to see if the socket has any data to read from the remote host.

        A NotConnectedException is raised if the duct hasn't been bound to the other end yet.

        A RemoteSocketClosed exception is raised if the local socket has faulted (we can't talk to the remote any more).

        :param timeout: Time to wait for data to show up. If 0, poll() does not block.
        :type timeout: float | int
        :return: True if this data to read, False otherwise.
        """
        if self.conn_socket is None:
            raise NotConnectedException("Must be connected to other end to poll for data!")
        has_recv_data, _, is_faulted = map(bool, select.select([self.conn_socket], [], [self.conn_socket], timeout))
        if is_faulted:
            self.server_connection_socket_destructor(self.conn_socket, shutdown=True)
            raise RemoteSocketClosed("Remote socket is closed!")
        return has_recv_data

    def get_conn_file_descriptor(self):
        """
        Get the file descriptor of the underlying connection socket. This is useful for integrating into other event
        loops.

        A NotConnectedException is raised if the duct hasn't been bound to the other end yet.

        :return: The connection file descriptor.
        :rtype: int
        """
        if self.conn_socket is None:
            raise NotConnectedException("Must be connected to other end to have a file descriptor!")
        else:
            return self.conn_socket.fileno()

    def close(self, shutdown=False):
        """
        Close the connection or listener sockets, if they're open.
        :param shutdown: Should shutdown be performed on the connection socket? (Usually no). Default: False
        :type shutdown: bool
        :return: None
        """
        if self.listener_socket is not None:
            self.server_listener_socket_destructor(self.listener_socket, shutdown=True)
            self.listener_socket = None
        if self.conn_socket is not None:
            self.server_connection_socket_destructor(self.conn_socket, shutdown=shutdown)
            self.conn_socket = None

    def __del__(self):
        self.close()


class RawDuctChild(object):
    """
    The RawDuctChild is a thin wrapper over top of a "client" socket, and should be at the other end of a listening
    RawDuctParent. Once the first connection is seen, the listener socket is closed, and the "parent" and
    "child" act like an anonymous socket pair (with a few extra niceties, like a poll method).
    """

    DEFAULT_TIMEOUT = 30
    DEFAULT_CONNECT_RETRY_COUNT = 3
    DEFAULT_RETRY_DELAY = 3

    def __init__(self, connect_address, socket_constructor=unix_domain_socket_constructor,
                 socket_destructor=client_socket_destructor, timeout=DEFAULT_TIMEOUT):
        self.socket_constructor = socket_constructor
        self.socket_destructor = socket_destructor
        self.connect_address = connect_address
        self.socket = None
        self.socket_timeout = timeout

    def connect(self, connect_retry_count=DEFAULT_CONNECT_RETRY_COUNT, connect_retry_delay=DEFAULT_RETRY_DELAY):
        """
        Attempt to connect to another duct.

        AlreadyConnectedException is raised if the connection has already been established.

        :param connect_retry_count: The number of times to retry connecting if the connection is refused or if the
            file system entry for the Unix Domain socket does not yet exist on disk. Default: 3
        :type connect_retry_count: int
        :param connect_retry_delay: The amount of time to sleep between successive connect retries in the event of
            failure. Default: 3
        :type connect_retry_delay: int | float
        :return: None
        """
        if self.socket is not None:
            raise AlreadyConnectedException("Already connected to other end!")
        while True:
            try:
                self.socket = self.socket_constructor()
                self.socket.settimeout(self.socket_timeout)
                self.socket.connect(self.connect_address)
                return
            except socket.error as e:
                self.socket = None
                if e.errno == errno.ENOENT or e.errno == errno.ECONNREFUSED and connect_retry_count:
                    connect_retry_count -= 1
                    time.sleep(connect_retry_delay)
                else:
                    raise e

    def send(self, byte_array, flags=None):
        """
        Send data to the other end of the duct. NOTE: This send call is a thin wrapper over the underlying socket
        send call, and is thus _stream oriented_. A single send call may _not_ send all the data given to the method.
        It is your responsibility to add additional semantics and metadata to get "message-based" send/recv, and to
        check from the return value how many bytes were _actually_ sent.

        A NotConnectedException is raised if the duct hasn't been bound to the other end yet.

        :param byte_array: The bytes-like data to send to the other end.
        :type byte_array: bytearray | buffer | str | bytes
        :param flags: Optional flags to be set on the socket send call.
        :param flags: bytearray | buffer | str | bytes | None
        :return: The number of bytes sent.
        :rtype: int
        """
        if self.socket is None:
            raise NotConnectedException("Must be connected to other end to send data!")
        if flags is None:
            return self.socket.send(byte_array)
        else:
            return self.socket.send(byte_array, flags)

    def recv(self, buff_size, flags=None):
        """
        Receive up to buff_size bytes from the remote host.

        A NotConnectedException is raised if the duct hasn't been bound to the other end yet.

        :param buff_size: The maximum number of bytes to read off the socket buffer from the remote host.
        :type buff_size: int
        :param flags: Optional flags to be set on the socket recv call.
        :param flags: bytearray | buffer | str | bytes | None
        :return: A bytes-like object that contains data from the remote host.
        :rtype: bytearray | buffer | str | bytes
        """
        if self.socket is None:
            raise NotConnectedException("Must be connected to other end to receive data!")
        if flags is None:
            return self.socket.recv(buff_size)
        else:
            return self.socket.recv(buff_size, flags)

    def recv_into(self, buffer, recv_num_bytes=None, flags=None):
        """
        Receive up to buff_size bytes from the remote host. A buffer is given to write directly into to reduce the
        memory overhead of recv (one less copy needed).

        A NotConnectedException is raised if the duct hasn't been bound to the other end yet.

        :param buffer: An object that implements the buffer interface and can have data written directly into it.
        :type buffer: buffer
        :param recv_num_bytes: The maximum number of bytes to read off the socket buffer from the remote host.
            If this is not set, it is calculated from the length of the buffer passed in.
        :type recv_num_bytes: int | Nonre
        :param flags: Optional flags to be set on the socket recv call.
        :param flags: bytearray | buffer | str | bytes | None
        :return: The number of bytes read from the socket's recv buffer from the remote host.
        :rtype: int
        """
        if self.socket is None:
            raise NotConnectedException("Must be connected to other end to receive data!")
        if recv_num_bytes is None and flags is None:
            return self.socket.recv_into(buffer)
        elif flags is None and recv_num_bytes is not None:
            return self.socket.recv_into(buffer, recv_num_bytes)
        elif flags is not None and recv_num_bytes is not None:
            return self.socket.recv_into(buffer, recv_num_bytes, flags)
        else:
            assert False

    def poll(self, timeout=60):
        """
        Poll to see if the socket has any data to read from the remote host.

        A NotConnectedException is raised if the duct hasn't been bound to the other end yet.

        A RemoteSocketClosed exception is raised if the local socket has faulted (we can't talk to the remote any more).

        :param timeout: Time to wait for data to show up. If 0, poll() does not block.
        :type timeout: float | int
        :return: True if this data to read, False otherwise.
        """
        if self.socket is None:
            raise NotConnectedException("Must be connected to other end to poll for data!")
        has_recv_data, _, is_faulted = map(bool, select.select([self.socket], [], [self.socket], timeout))
        if is_faulted:
            self.socket_destructor(self.socket, shutdown=True)
            raise RemoteSocketClosed("Remote socket is closed!")
        return has_recv_data

    def get_conn_file_descriptor(self):
        """
        Get the file descriptor of the underlying connection socket. This is useful for integrating into other event
        loops.

        A NotConnectedException is raised if the duct hasn't been bound to the other end yet.

        :return: The connection file descriptor.
        :rtype: int
        """
        if self.socket is None:
            raise NotConnectedException("Must be connected to other end to have a file descriptor!")
        else:
            return self.socket.fileno()

    def close(self, shutdown=False):
        """
        Close the connection socket, if it's open.
        :param shutdown: Should shutdown be performed on the connection socket? (Usually no). Default: False
        :type shutdown: bool
        :return: None
        """
        if self.socket is not None:
            self.socket_destructor(self.socket, shutdown=shutdown)
            self.socket = None

    def __del__(self):
        self.close()
