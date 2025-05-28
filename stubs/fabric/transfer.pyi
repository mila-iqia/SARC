from _typeshed import Incomplete

from .util import debug as debug

class Transfer:
    connection: Incomplete
    def __init__(self, connection) -> None: ...
    @property
    def sftp(self): ...
    def is_remote_dir(self, path): ...
    def get(
        self, remote, local: Incomplete | None = None, preserve_mode: bool = True
    ): ...
    def put(
        self, local, remote: Incomplete | None = None, preserve_mode: bool = True
    ): ...

class Result:
    local: Incomplete
    orig_local: Incomplete
    remote: Incomplete
    orig_remote: Incomplete
    connection: Incomplete
    def __init__(self, local, orig_local, remote, orig_remote, connection) -> None: ...
