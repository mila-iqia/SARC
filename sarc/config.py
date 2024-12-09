import functools
import json
import os
import zoneinfo
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import date, datetime
from functools import cached_property
from pathlib import Path
from typing import Any, Union

import pydantic
import tzlocal
from bson import ObjectId
from pydantic import BaseModel as _BaseModel
from pydantic import Extra, validator

MTL = zoneinfo.ZoneInfo("America/Montreal")
PST = zoneinfo.ZoneInfo("America/Vancouver")
UTC = zoneinfo.ZoneInfo("UTC")
TZLOCAL = zoneinfo.ZoneInfo(tzlocal.get_localzone_name())


class ConfigurationError(Exception):
    pass


def validate_date(value: Union[str, date, datetime]) -> date:
    if isinstance(value, str):
        if "T" in value:
            return datetime.strptime(value, "%Y-%m-%dT%H:%M:%S").date()

        return datetime.strptime(value, "%Y-%m-%d").date()

    if isinstance(value, datetime):
        return value.date()

    return value


class BaseModel(_BaseModel):
    class Config:
        # Forbid extra fields that are not explicitly defined
        extra = Extra.forbid
        # Ignore cached_property, this avoids errors with serialization
        keep_untouched = (cached_property,)
        # Serializer for mongo's object ids
        json_encoders = {ObjectId: str}
        # Allow types like ZoneInfo
        arbitrary_types_allowed = True

    def dict(self, *args, **kwargs) -> dict[str, Any]:
        d = super().dict(*args, **kwargs)
        for k, v in list(d.items()):
            if isinstance(getattr(type(self), k, None), cached_property):
                del d[k]
                continue

        for k, v in d.items():
            if isinstance(v, date) and not isinstance(v, datetime):
                d[k] = datetime(
                    year=v.year,
                    month=v.month,
                    day=v.day,
                )
        return d

    def replace(self, **replacements):
        new_arguments = {**self.dict(), **replacements}
        return type(self)(**new_arguments)


class ClusterConfig(BaseModel):
    host: str = "localhost"
    timezone: Union[str, zoneinfo.ZoneInfo]  # | does not work with Pydantic's eval
    prometheus_url: str = None
    prometheus_headers_file: str = None
    name: str = None
    sacct_bin: str = "sacct"
    accounts: list[str] = None
    sshconfig: Path = None
    duc_inodes_command: str = None
    duc_storage_command: str = None
    diskusage_report_command: str = None
    start_date: str = "2022-04-01"
    rgu_start_date: str = None
    gpu_to_rgu_billing: Path = None

    @validator("timezone")
    def _timezone(cls, value):
        if isinstance(value, str):
            return zoneinfo.ZoneInfo(value)
        else:
            return value

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


class MongoConfig(BaseModel):
    connection_string: str
    database_name: str

    @cached_property
    def database_instance(self):
        from pymongo import MongoClient

        client = MongoClient(self.connection_string)
        return client.get_database(self.database_name)


class LDAPConfig(BaseModel):
    local_private_key_file: str
    local_certificate_file: str
    ldap_service_uri: str
    mongo_collection_name: str
    group_to_prof_json_path: str = None
    exceptions_json_path: str = None

    @validator("group_to_prof_json_path")
    def _relative_group_to_prof(cls, value):
        return relative_filepath(value)

    @validator("exceptions_json_path")
    def _relative_exception(cls, value):
        return relative_filepath(value)


class LokiConfig(BaseModel):
    uri: str


class TempoConfig(BaseModel):
    uri: str


class MyMilaConfig(BaseModel):
    tmp_json_path: str

    @validator("tmp_json_path")
    def _relative_tmp(cls, value):
        return relative_filepath(value)


class AccountMatchingConfig(BaseModel):
    drac_members_csv_path: Path
    drac_roles_csv_path: Path
    make_matches_config: Path


class LoggingConfig(BaseModel):
    log_level: str
    OTLP_endpoint: str
    service_name: str


# pylint: disable=unused-argument,redefined-outer-name
def _absolute_path(value, values, config, field):
    return value and value.expanduser().absolute()


class Config(BaseModel):
    mongo: MongoConfig
    cache: Path = None
    loki: LokiConfig = None
    tempo: TempoConfig = None

    _abs_path = validator("cache", allow_reuse=True)(_absolute_path)


class ScraperConfig(BaseModel):
    mongo: MongoConfig
    cache: Path = None

    ldap: LDAPConfig = None
    mymila: MyMilaConfig = None
    account_matching: AccountMatchingConfig = None
    sshconfig: Path = None
    clusters: dict[str, ClusterConfig] = None
    logging: LoggingConfig = None
    loki: LokiConfig = None
    tempo: TempoConfig = None

    _abs_path = validator("cache", "sshconfig", allow_reuse=True)(_absolute_path)

    @validator("clusters")
    def _complete_cluster_fields(cls, value, values):
        for name, cluster in value.items():
            if not cluster.name:
                cluster.name = name
            if not cluster.sshconfig and "sshconfig" in values:
                cluster.sshconfig = values["sshconfig"]
        return value


config_var = ContextVar("config", default=None)


_config_folder = None


def relative_filepath(path):
    """Allows files to be relative to the config"""
    if path is None:
        return path

    if "$SELF" in path:
        return path.replace("$SELF", str(_config_folder))

    return path


def parse_config(config_path, config_cls=Config):
    # pylint: disable=global-statement
    global _config_folder
    config_path = Path(config_path)

    _config_folder = str(config_path.parent)

    if not config_path.exists():
        raise ConfigurationError(
            f"Cannot read SARC configuration file: '{config_path}'"
            " Use the $SARC_CONFIG environment variable to choose the config file."
        )

    try:
        cfg = config_cls.parse_file(config_path)
    except json.JSONDecodeError as exc:
        raise ConfigurationError(f"'{config_path}' contains malformed JSON") from exc

    return cfg


def _config_class(mode):
    modes = {
        "scraping": ScraperConfig,
        "client": Config,
    }
    return modes.get(mode, Config)


def config():
    if (current := config_var.get()) is not None:
        return current

    config_path = os.getenv("SARC_CONFIG", "config/sarc-dev.json")
    config_class = _config_class(os.getenv("SARC_MODE", "none"))

    try:
        cfg = parse_config(config_path, config_class)
    except pydantic.error_wrappers.ValidationError as err:
        if config_class is Config:
            raise ConfigurationError(
                "Try `SARC_MODE=scraping sarc ...` if you want admin rights"
            ) from err
        raise

    config_var.set(cfg)
    return cfg


@contextmanager
def using_config(cfg: Union[str, Path, Config], cls=None):
    cls = cls or _config_class(os.getenv("SARC_MODE", "none"))

    if isinstance(cfg, (str, Path)):
        cfg = parse_config(cfg, cls)

    token = config_var.set(cfg)
    yield cfg
    config_var.reset(token)


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
        if not isinstance(config(), ScraperConfig):
            raise ScrapingModeRequired()
        return fn(*args, **kwargs)

    return wrapper
