import socket
from datetime import timedelta

import pytest
from invoke.exceptions import CommandTimedOut
from paramiko import AuthenticationException, SSHException

from sarc.config import config
from sarc.errors import FetchTimeout


class FakeConnection:
    def __init__(self, open_error=None, run_error=None):
        self.is_connected = False
        self.connect_timeout = None
        self.connect_kwargs = {"pkey": "dummy"}
        self.open_error = open_error
        self.run_error = run_error
        self.opened = 0
        self.runs = []

    def open(self):
        self.opened += 1
        if self.open_error is not None:
            raise self.open_error
        self.is_connected = True

    def run(self, cmd, hide, timeout):
        self.runs.append((cmd, timeout))
        if self.run_error is not None:
            raise self.run_error
        return type("Result", (), {"stdout": "ok"})()


@pytest.fixture
def cluster(monkeypatch):
    cluster = config.clusters["raisin"]
    monkeypatch.setattr(cluster, "timeout", timedelta(seconds=42))
    return cluster


def _use(monkeypatch, cluster, conn):
    monkeypatch.setitem(cluster.__dict__, "ssh", conn)
    return conn


def test_ssh_run_sets_connection_timeouts(monkeypatch, cluster):
    conn = _use(monkeypatch, cluster, FakeConnection())

    assert cluster.ssh_run("hostname").stdout == "ok"

    assert conn.opened == 1
    assert conn.connect_timeout == 42
    assert conn.connect_kwargs == {
        "pkey": "dummy",
        "banner_timeout": 42,
        "auth_timeout": 42,
        "channel_timeout": 42,
    }
    ((cmd, timeout),) = conn.runs
    assert cmd == "hostname"
    # The command gets what remains of the budget after connecting.
    assert 0 < timeout <= 42


def test_ssh_run_explicit_timeout(monkeypatch, cluster):
    conn = _use(monkeypatch, cluster, FakeConnection())
    cluster.ssh_run("hostname", timeout=5)
    assert conn.connect_timeout == 5
    assert 0 < conn.runs[0][1] <= 5


def test_ssh_run_already_connected(monkeypatch, cluster):
    conn = FakeConnection()
    conn.is_connected = True
    _use(monkeypatch, cluster, conn)
    cluster.ssh_run("hostname")
    assert conn.opened == 0
    assert conn.connect_timeout is None


@pytest.mark.parametrize(
    "error",
    [
        socket.timeout("timed out"),
        TimeoutError(),
        SSHException("Negotiation timed out."),
        SSHException("Error reading SSH protocol banner"),
        AuthenticationException("Authentication timeout."),
    ],
)
def test_ssh_run_connection_timeout(monkeypatch, cluster, error):
    # Banner errors don't always say "timeout": pretend the budget is spent.
    if "banner" in str(error):
        monkeypatch.setattr(cluster, "timeout", timedelta(seconds=0))
    conn = _use(monkeypatch, cluster, FakeConnection(open_error=error))
    with pytest.raises(FetchTimeout, match="SSH connection"):
        cluster.ssh_run("hostname")
    assert conn.runs == []


def test_ssh_run_connection_error_is_not_timeout(monkeypatch, cluster):
    _use(
        monkeypatch,
        cluster,
        FakeConnection(open_error=AuthenticationException("bad key")),
    )
    with pytest.raises(AuthenticationException):
        cluster.ssh_run("hostname")


def test_ssh_run_command_timeout(monkeypatch, cluster):
    _use(
        monkeypatch,
        cluster,
        FakeConnection(run_error=CommandTimedOut(result=None, timeout=42)),
    )
    with pytest.raises(FetchTimeout, match="timed out"):
        cluster.ssh_run("hostname")
