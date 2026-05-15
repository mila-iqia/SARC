import gifnoc

from .client import SarcClient

default_client = gifnoc.define("sarc.client", SarcClient)

__all__ = ["SarcClient"]
