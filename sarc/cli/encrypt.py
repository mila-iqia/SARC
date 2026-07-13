import json
import logging
import os
from dataclasses import dataclass
from pathlib import Path

from serieux.features.encrypt import EncryptionKey, crypt_prefix
from simple_parsing import subparsers

logger = logging.getLogger(__name__)


@dataclass
class File:
    path: Path

    def execute(self) -> int:
        ek = EncryptionKey(os.environ.get("SERIEUX_PASSWORD", None))
        content = self.path.read_text()
        if content.startswith(crypt_prefix):
            logger.error("File is already encrypted (or looks like it is)")
            return -1
        self.path.write_text(ek.encrypt(json.loads(content)))
        return 0


@dataclass
class Append:
    path: Path
    key: str
    value: str

    def execute(self) -> int:
        ek = EncryptionKey(os.environ.get("SERIEUX_PASSWORD", None))
        val = ek.decrypt(self.path.read_text())
        val[self.key] = self.value
        self.path.write_text(ek.encrypt(val))
        return 0


@dataclass
class Encrypt:
    command: File | Append = subparsers(
        {"file": File, "append": Append}  # ty:ignore[invalid-argument-type]
    )

    def execute(self) -> int:
        return self.command.execute()
