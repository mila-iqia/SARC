import json
import os
from contextlib import contextmanager
from contextvars import ContextVar
from functools import cached_property
from pathlib import Path

from pydantic import BaseModel, validator
from pymongo import MongoClient


class MongoConfig(BaseModel):
    class Config:
        keep_untouched = (cached_property,)

    url: str
    database: str

    @cached_property
    def instance(self):
        client = MongoClient(self.url)
        return client.get_database(self.database)


class Config(BaseModel):
    mongo: MongoConfig
    cache: Path = None

    @validator("cache")
    def _abspath(cls, value):
        return value and value.expanduser().absolute()


config_var = ContextVar("config", default=None)


def parse_config(config_path):
    config_path = Path(config_path)

    if not config_path.exists():
        raise Exception(f"Cannot read SARC configuration file: '{config_path}'")

    try:
        cfg = Config.parse_file(config_path)
    except json.JSONDecodeError as exc:
        raise Exception(f"'{config_path}' contains malformed JSON") from exc

    return cfg


def config():
    if (current := config_var.get()) is not None:
        return current
    cfg = parse_config(os.environ.get("SARC_CONFIG", "sarc.json"))
    config_var.set(cfg)
    return cfg


@contextmanager
def using_config(cfg):
    token = config_var.set(cfg)
    yield cfg
    config_var.reset(token)
