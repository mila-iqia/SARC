import functools
import json
import os
import zoneinfo
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field, fields
from functools import cached_property
from pathlib import Path
from typing import Optional

import gifnoc
import tzlocal
from hostlist import expand_hostlist

from .alerts.common import HealthMonitorConfig

MTL = zoneinfo.ZoneInfo("America/Montreal")
PST = zoneinfo.ZoneInfo("America/Vancouver")
UTC = zoneinfo.ZoneInfo("UTC")
TZLOCAL = zoneinfo.ZoneInfo(tzlocal.get_localzone_name())


MIG_FLAG = "__MIG_FLAG__"
DEFAULTS_FLAG = "__DEFAULTS__"


class ConfigurationError(Exception):
    pass


@dataclass
class ClusterConfig:
    # pylint: disable=too-many-instance-attributes

    host: str = "localhost"
    timezone: zoneinfo.ZoneInfo | None = None
    prometheus_url: str | None = None
    prometheus_headers_file: str | None = None
    name: str | None = None
    sacct_bin: str = "sacct"
    accounts: list[str] | None = None
    sshconfig: Path | None = None
    duc_inodes_command: str | None = None
    duc_storage_command: str | None = None
    diskusage_report_command: str | None = None
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

    def harmonize_gpu(self, nodename: str, gpu_type: str) -> Optional[str]:
        """
        Actual utility method to get a GPU name from given node and gpu type.

        Return None if GPU name cannot be inferred.
        """

        gpu_type_parts = gpu_type.lower().replace(" ", "-").split(":")
        if gpu_type_parts[0] == "gpu":
            gpu_type_parts.pop(0)
        gpu_type = gpu_type_parts[0]

        # Try to get harmonized GPU from nodename mapping
        harmonized_gpu = self.gpus_per_nodes.get(nodename, {}).get(gpu_type)

        # Otherwise, try to get harmonized GPU from default mapping
        if harmonized_gpu is None:
            harmonized_gpu = self.gpus_per_nodes.get(DEFAULTS_FLAG, {}).get(gpu_type)

        # For MIG GPUs, use this method recursively
        if harmonized_gpu and harmonized_gpu.startswith(MIG_FLAG):
            harmonized_gpu = self.harmonize_gpu(
                nodename, harmonized_gpu[len(MIG_FLAG) :]
            )
            harmonized_gpu = f"{harmonized_gpu} : {gpu_type}"

        return harmonized_gpu

    @cached_property
    def ssh(self):
        from fabric import Config as FabricConfig
        from fabric import Connection
        from paramiko import SSHConfig

        if self.sshconfig is None:
            fconfig = FabricConfig()
        else:
            fconfig = FabricConfig(ssh_config=SSHConfig.from_path(self.sshconfig))
        fconfig["run"]["pty"] = False
        fconfig["run"]["in_stream"] = False
        return Connection(self.host, config=fconfig)

    @cached_property
    def prometheus(self):
        from prometheus_api_client import PrometheusConnect

        if self.prometheus_headers_file is not None:
            headers = json.load(
                open(  # pylint: disable=consider-using-with
                    self.prometheus_headers_file, "r", encoding="utf-8"
                )
            )
        else:
            headers = {}

        if self.prometheus_url is None:
            raise ConfigurationError(
                f"No prometheus URL provided for cluster '{self.name}'"
            )
        return PrometheusConnect(url=self.prometheus_url, headers=headers)


@dataclass
class MongoConfig:
    connection_string: str
    database_name: str

    @cached_property
    def database_instance(self):
        from pymongo import MongoClient

        client = MongoClient(self.connection_string)
        return client.get_database(self.database_name)


@dataclass
class LDAPConfig:
    local_private_key_file: Path
    local_certificate_file: Path
    ldap_service_uri: str
    mongo_collection_name: str
    group_to_prof_json_path: Path | None = None
    exceptions_json_path: Path | None = None


@dataclass
class LokiConfig:
    uri: str


@dataclass
class TempoConfig:
    uri: str


@dataclass
class MyMilaConfig:
    tmp_json_path: Path | None = None


@dataclass
class AccountMatchingConfig:
    drac_members_csv_path: Path
    drac_roles_csv_path: Path
    make_matches_config: Path


@dataclass
class LoggingConfig:
    log_level: str
    OTLP_endpoint: str
    service_name: str


@dataclass
class ClientConfig:
    mongo: MongoConfig
    cache: Path | None = None
    loki: LokiConfig | None = None
    tempo: TempoConfig | None = None
    health_monitor: HealthMonitorConfig | None = None


@dataclass
class Config(ClientConfig):
    ldap: LDAPConfig | None = None
    mymila: MyMilaConfig | None = None
    account_matching: AccountMatchingConfig | None = None
    sshconfig: Path | None = None
    clusters: dict[str, ClusterConfig] | None = None
    logging: LoggingConfig | None = None

    def __post_init__(self):
        if self.clusters:
            for name, cluster in self.clusters.items():
                if not cluster.name:
                    cluster.name = name
                if not cluster.sshconfig:
                    cluster.sshconfig = self.sshconfig


class WhitelistProxy:
    def __init__(self, obj, *whitelist):
        self._obj = obj
        self._whitelist = whitelist

    def __getattr__(self, attr):
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


sarc_mode = ContextVar("sarc_mode", default=os.getenv("SARC_MODE", "client"))


@contextmanager
def using_sarc_mode(mode):
    token = sarc_mode.set(mode)
    try:
        yield
    finally:
        sarc_mode.reset(token)


def config():
    if sarc_mode.get() == "scraping":
        return full_config
    else:
        accept = [f.name for f in fields(ClientConfig)]
        return WhitelistProxy(full_config, *accept)


class ScrapingModeRequired(Exception):
    """Exception raised if a code requiring scraping mode is executed in client mode."""


def scraping_mode_required(fn):
    """
    Decorator to wrap a function that requires scraping mode to be executed.

    Returns a wrapped function which raises a ScrapingModeRequired exception
    if config is not a ScrapingConfig instance.
    """

    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        if sarc_mode.get() != "scraping":
            raise ScrapingModeRequired()
        return fn(*args, **kwargs)

    return wrapper
