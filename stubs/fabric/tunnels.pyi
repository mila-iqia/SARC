from _typeshed import Incomplete
from invoke.util import ExceptionHandlingThread

class TunnelManager(ExceptionHandlingThread):
    local_address: Incomplete
    remote_address: Incomplete
    transport: Incomplete
    finished: Incomplete
    def __init__(self, local_host, local_port, remote_host, remote_port, transport, finished) -> None: ...

class Tunnel(ExceptionHandlingThread):
    channel: Incomplete
    sock: Incomplete
    finished: Incomplete
    socket_chunk_size: int
    channel_chunk_size: int
    def __init__(self, channel, sock, finished) -> None: ...
    def read_and_write(self, reader, writer, chunk_size): ...
