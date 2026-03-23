from __future__ import annotations

import functools
import os
import zoneinfo
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field, fields
from functools import cached_property
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable, Literal, cast, overload

import gifnoc
import tzlocal
from bson import CodecOptions, UuidRepresentation
from easy_oauth import OAuthManager
from hostlist import expand_hostlist
from paramiko import PKey
from serieux.features.encrypt import Secret

from .alerts.common import HealthMonitorConfig

type JSON = list[JSON] | dict[str, JSON] | int | str | float | bool | None

if TYPE_CHECKING:
    from fabric import Connection
    from prometheus_api_client import PrometheusConnect
    from pymongo.database import Database


UTC = zoneinfo.ZoneInfo("UTC")
TZLOCAL = zoneinfo.ZoneInfo(tzlocal.get_localzone_name())


MIG_FLAG = "__MIG_FLAG__"
DEFAULTS_FLAG = "__DEFAULTS__"


class ConfigurationError(Exception):
    pass


@dataclass
class DiskUsageConfig:
    name: str
    params: JSON = field(default_factory=dict)


@dataclass
class PrivateKeyInfo:
    file: Path
    password: Secret[str]


@dataclass
class ClusterConfig:
    # pylint: disable=too-many-instance-attributes
    host: str
    private_key: PrivateKeyInfo
    timezone: zoneinfo.ZoneInfo | None = None
    prometheus_url: str | None = None
    prometheus_headers: dict[str, Secret[str]] = field(default_factory=dict)
    name: str | None = None
    sacct_bin: str = "sacct"
    ignore_tz_utc: bool = False
    accounts: list[str] | None = None
    diskusage: list[DiskUsageConfig] | None = None
    start_date: str = "2022-04-01"
    slurm_conf_host_path: Path = Path("/etc/slurm/slurm.conf")

    # Tell if billing (in job's requested|allocated field) is number of GPUs (True) or RGU (False)
    billing_is_gpu: bool = False
    # Dictionary mapping a node name -> gpu type -> IGUANE gpu name
    gpus_per_nodes: dict[str, dict[str, str]] = field(default_factory=dict)

    def __post_init__(self):
        # Convert node list to node names with `expand_hostlist`
        self.gpus_per_nodes = {
            node: {
                gpu_type.lower().replace(" ", "-"): gpu_desc
                for gpu_type, gpu_desc in gpu_to_desc.items()
            }
            for node_list, gpu_to_desc in self.gpus_per_nodes.items()
            for node in expand_hostlist(node_list)
        }

    def harmonize_gpu(self, nodename: str | None, gpu_type: str) -> str | None:
        """
        Actual utility method to get a GPU name from given node and gpu type.

        Return None if GPU name cannot be inferred.
        """
        gpu_type = gpu_type.lower().replace(" ", "-")
        if gpu_type.startswith("gpu:"):
            gpu_type = gpu_type.split(":")[1]

        # Try to get harmonized GPU from nodename mapping
        harmonized_gpu = self.gpus_per_nodes.get(cast(str, nodename), {}).get(gpu_type)

        # Otherwise, try to get harmonized GPU from default mapping
        if harmonized_gpu is None:
            harmonized_gpu = self.gpus_per_nodes.get(DEFAULTS_FLAG, {}).get(gpu_type)

        # If harmonized name starts with "$", then we must recursively harmonize again.
        if harmonized_gpu and harmonized_gpu.startswith("$"):
            harmonized_gpu = self.harmonize_gpu(nodename, harmonized_gpu[1:])

        # For MIG GPUs, use this method recursively and append MIG name.
        if harmonized_gpu and harmonized_gpu.startswith(MIG_FLAG):
            harmonized_gpu = self.harmonize_gpu(
                nodename, harmonized_gpu[len(MIG_FLAG) :]
            )
            harmonized_gpu = f"{harmonized_gpu} : {gpu_type}"

        return harmonized_gpu

    def harmonize_gpu_from_nodes(self, nodes: list[str], gpu_type: str) -> str | None:
        """
        Get a GPU name from given multiple nodes and GPU type.

        Return None if GPU name cannot be inferred.
        """
        # Collect harmonized names for given nodes
        # NB: If `nodes` is empty, we harmonize using "",
        # so that harmonization function will check __DEFAULTS__
        # harmonized names if available.
        harmonized_gpu_names = {
            self.harmonize_gpu(nodename, gpu_type) for nodename in (nodes or [""])
        }
        # If present, remove None from GPU names
        harmonized_gpu_names.discard(None)
        # If we got 1 GPU name, use it.
        # Otherwise, return None.
        return harmonized_gpu_names.pop() if len(harmonized_gpu_names) == 1 else None

    @cached_property
    def ssh(self) -> Connection:
        from fabric import Config as FabricConfig
        from fabric import Connection

        fconfig = FabricConfig()
        fconfig["run"]["pty"] = False
        fconfig["run"]["in_stream"] = False
        return Connection(
            self.host,
            config=fconfig,
            connect_kwargs={
                "pkey": PKey.from_path(
                    self.private_key.file, self.private_key.password.encode("ascii")
                ),
                "password": "1",
            },
        )

    @cached_property
    def prometheus(self) -> PrometheusConnect:
        from prometheus_api_client import PrometheusConnect

        if self.prometheus_url is None:
            raise ConfigurationError(
                f"No prometheus URL provided for cluster '{self.name}'"
            )
        return PrometheusConnect(
            url=self.prometheus_url, headers=self.prometheus_headers
        )


@dataclass
class MongoConfig:
    connection_string: str
    database_name: str
    auto_upgrade: bool = True

    @cached_property
    def database_instance(self) -> Database:
        from pymongo import MongoClient

        client: MongoClient = MongoClient(self.connection_string)
        db = client.get_database(
            self.database_name,
            codec_options=CodecOptions(
                uuid_representation=UuidRepresentation.STANDARD, tz_aware=True
            ),
        )

        if self.auto_upgrade:
            from sarc.core.db_init import db_upgrade

            db_upgrade(db)

        return db


@dataclass
class LokiConfig:
    uri: str


@dataclass
class TempoConfig:
    uri: str


@dataclass
class SlackConfig:
    description: str
    token: Secret[str]
    channel: str


@dataclass
class LoggingConfig:
    log_level: str
    OTLP_endpoint: str | None = None
    service_name: str | None = None
    slack: SlackConfig | None = None


@dataclass
class UserScrapingConfig:
    scrapers: dict[str, JSON]


@dataclass
class ApiConfig:
    """
    Configuration for Python REST API client

    Used if client is initialized without parameters.
    Currently necessary for high-level Python client functions
    such as `load_job_series()`, which internally initialize
    a client without parameters.
    """

    url: str | None = None  # REST API URL (including port)
    timeout: int = 120
    # Default pagination size
    per_page: int = 100
    # Maximum page size
    max_page_size: int = 5000
    auth: OAuthManager | None = None


@dataclass
class ClientConfig:
    mongo: MongoConfig
    api: ApiConfig = field(default_factory=ApiConfig)
    cache: Path | None = None
    loki: LokiConfig | None = None
    tempo: TempoConfig | None = None
    health_monitor: HealthMonitorConfig | None = None

    @property
    def lock_path(self) -> Path:
        """
        Return a convenient path to be used as lock file for database operations.
        """
        assert self.cache
        return self.cache / "lockfile.lock"

    class SerieuxConfig:
        # Config adds extra fields to ClientConfig, so to be able to read
        # a Config as a ClientConfig we need allow_extras to be True
        allow_extras = True


@dataclass
class Config(ClientConfig):
    users: UserScrapingConfig | None = None
    clusters: dict[str, ClusterConfig] = field(default_factory=dict)
    logging: LoggingConfig | None = None

    def __post_init__(self):
        for name, cluster in self.clusters.items():
            if not cluster.name:
                cluster.name = name


class WhitelistProxy:
    def __init__(self, obj: Any, *whitelist: str):
        self._obj = obj
        self._whitelist = whitelist

    def __getattr__(self, attr: str) -> Any:
        if attr in self._whitelist:
            return getattr(self._obj, attr)
        elif hasattr(self._obj, attr):
            raise AttributeError(
                f"Attribute '{attr}' is only accessible with SARC_MODE=scraping"
            )
        else:
            raise AttributeError(attr)


full_config = gifnoc.define("sarc", Config)


gifnoc.set_sources("${envfile:SARC_CONFIG}")
config_path = Path(os.getenv("SARC_CONFIG", "")).parent

sarc_mode = ContextVar("sarc_mode", default=os.getenv("SARC_MODE", "client"))

Modes = Literal["scraping", "client"]


@contextmanager
def using_sarc_mode(mode: Modes):
    token = sarc_mode.set(mode)
    try:
        yield
    finally:
        sarc_mode.reset(token)


@overload
def config(mode: Literal["scraping"]) -> Config: ...


@overload
def config(mode: Literal["client"]) -> ClientConfig: ...


@overload
def config(mode: None = None) -> Config | ClientConfig: ...


def config(mode: Modes | None = None) -> Config | ClientConfig:
    cur_mode = sarc_mode.get()
    # If we request client mode and we are in scraping mode, that is fine.
    if mode == "scraping" and cur_mode != "scraping":
        raise ScrapingModeRequired()
    if cur_mode == "scraping":
        return full_config
    else:
        accept = [f.name for f in fields(ClientConfig)]
        return cast(ClientConfig, WhitelistProxy(full_config, *accept, "lock_path"))


class ScrapingModeRequired(Exception):
    """Exception raised if a code requiring scraping mode is executed in client mode."""


def scraping_mode_required[**P, R](fn: Callable[P, R]) -> Callable[P, R]:
    """
    Decorator to wrap a function that requires scraping mode to be executed.

    Returns a wrapped function which raises a ScrapingModeRequired exception
    if config is not a ScrapingConfig instance.
    """

    @functools.wraps(fn)
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
        if sarc_mode.get() != "scraping":
            raise ScrapingModeRequired()
        return fn(*args, **kwargs)

    return wrapper
