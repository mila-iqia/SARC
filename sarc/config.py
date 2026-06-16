import os
import re
import zoneinfo
from dataclasses import dataclass, field
from datetime import date
from functools import cached_property
from pathlib import Path
from typing import TYPE_CHECKING, cast

import gifnoc
from easy_oauth import OAuthManager
from hostlist import expand_hostlist
from paramiko import PKey
from serieux.features.encrypt import Secret
from sqlmodel import Session

from .alerts.common import HealthMonitorConfig

type JSON = list[JSON] | dict[str, JSON] | int | str | float | bool | None

if TYPE_CHECKING:
    from fabric import Connection
    from prometheus_api_client.prometheus_connect import PrometheusConnect
    from sqlalchemy import Engine


UTC = zoneinfo.ZoneInfo("UTC")


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
class OTPInfo:
    otp_secret: Secret[str]


@dataclass
class StaticInfo:
    selector: str


@dataclass
class ClusterConfig:
    # pylint: disable=too-many-instance-attributes
    host: str
    private_key: PrivateKeyInfo
    # Name of user account domain (e.g: "mila", "drac")
    # Used to find user associated account for the cluster in
    # UserData.associated_accounts field
    user_domain: str
    password: OTPInfo | StaticInfo | None = None
    timezone: zoneinfo.ZoneInfo | None = None
    prometheus_url: str | None = None
    prometheus_headers: dict[str, Secret[str]] = field(default_factory=dict)
    prometheus_check_ssl: bool = True
    name: str | None = None
    sacct_bin: str = "sacct"
    ignore_tz_utc: bool = False
    accounts: list[str] | None = None
    diskusage: list[DiskUsageConfig] | None = None
    start_date: date = date(2022, 4, 1)
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
            # We expect a specific MIG name format, like "1g.5gb"
            if not re.fullmatch(r"^(([0-9]+)g\.([0-9]+)gb)$", gpu_type):
                raise ValueError(f"Unrecognized harmonized GPU type: {gpu_type}")

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
        extra_args = {}
        if isinstance(self.password, StaticInfo):
            extra_args["password"] = self.password.selector
        elif isinstance(self.password, OTPInfo):
            import pyotp

            totp = pyotp.TOTP(self.password.otp_secret)
            extra_args["password"] = totp.now()
        else:
            assert self.password is None
        return Connection(
            self.host,
            config=fconfig,
            connect_kwargs={
                "pkey": PKey.from_path(
                    # PKey.from_path has the wrong type annotation, the password must be bytes if provided directly
                    self.private_key.file,
                    self.private_key.password.encode("ascii"),  # ty:ignore[invalid-argument-type]
                ),
                **extra_args,
            },
        )

    @cached_property
    def prometheus(self) -> PrometheusConnect:
        from prometheus_api_client.prometheus_connect import PrometheusConnect

        if self.prometheus_url is None:
            raise ConfigurationError(
                f"No prometheus config provided for cluster '{self.name}'"
            )

        return PrometheusConnect(
            url=self.prometheus_url,
            headers=self.prometheus_headers,
            disable_ssl=not self.prometheus_check_ssl,
        )


def get_db_user() -> str:
    import getpass
    import os

    db_user = os.getenv("PGUSER")
    if db_user is None:
        db_user = getpass.getuser()
    return db_user


@dataclass
class DbConfig:
    host: str
    name: str
    user: str | None = None
    port: int | None = None

    @cached_property
    def engine(self) -> Engine:

        from sqlmodel import create_engine

        if ":" in self.host:
            # NB: `port` is not used here, since Google Cloud connector
            # explicitly drops the port argument and uses its own parameters:
            # https://github.com/GoogleCloudPlatform/cloud-sql-python-connector/blob/v1.20.3/google/cloud/sql/connector/connector.py#L376

            import google.auth
            import google.auth.transport.requests
            from google.cloud.sql.connector import Connector, IPTypes

            if self.user is None:
                credentials, _ = google.auth.default()
                request = google.auth.transport.requests.Request()
                credentials.refresh(request)
                sa_email: str = credentials.service_account_email
                db_user = sa_email.removesuffix(".gserviceaccount.com")
            else:
                db_user = self.user
            connector = Connector(
                ip_type=IPTypes.PRIVATE, refresh_strategy="LAZY", enable_iam_auth=True
            )

            def getconn():
                return connector.connect(
                    self.host, "pg8000", db=self.name, user=db_user
                )

            engine = create_engine("postgresql+pg8000://", creator=getconn)

        else:

            db_user = self.user
            if db_user is None:
                db_user = get_db_user()
            hostname = self.host
            if self.port is not None:
                hostname = f"{hostname}:{self.port}"
            engine = create_engine(
                f"postgresql+pg8000://{db_user}@{hostname}/{self.name}"
            )

        return engine

    def session(self) -> Session:
        return Session(self.engine)


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
class ServerConfig:
    """
    Configuration for Python REST server
    """

    # Authentication manager
    auth: OAuthManager | None = None


@dataclass
class Config:
    db: DbConfig
    patches: Path
    server: ServerConfig = field(default_factory=ServerConfig)
    cache: Path | None = None
    loki: LokiConfig | None = None
    tempo: TempoConfig | None = None
    health_monitor: HealthMonitorConfig | None = None
    users: UserScrapingConfig | None = None
    clusters: dict[str, ClusterConfig] = field(default_factory=dict)
    logging: LoggingConfig | None = None

    def __post_init__(self):
        for name, cluster in self.clusters.items():
            if not cluster.name:
                cluster.name = name

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


config = gifnoc.define("sarc", Config)


gifnoc.set_sources("${envfile:SARC_CONFIG}")
config_path = Path(os.getenv("SARC_CONFIG", "")).parent
