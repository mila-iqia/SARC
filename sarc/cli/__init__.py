from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any, Union

from simple_parsing import ArgumentParser, field, subparsers

from sarc.logging import setupLogging

from .acquire import Acquire
from .db import Db
from .health import Health

colors = SimpleNamespace(
    grey="\033[38;21m",
    blue="\033[38;5;39m",
    yellow="\033[38;5;226m",
    red="\033[38;5;196m",
    bold_red="\033[31;1m",
    reset="\033[0m",
    green="\033[32m",
    cyan="\033[36m",
)


level_colors = {
    "DEBUG": "grey",
    "INFO": "green",
    "WARNING": "yellow",
    "ERROR": "red",
    "CRITICAL": "bold_red",
}


class NiceHandler(logging.StreamHandler):
    def format(self, record):
        color = getattr(colors, level_colors.get(record.levelname, "grey"))
        ts = time.asctime(time.localtime(record.created))
        parts = [
            f"[{ts}] ",
            f"{colors.cyan}{record.name}{colors.reset} ",
            f"{color}{record.levelname:>8}{colors.reset} ",
            record.msg,
        ]
        return "".join(parts)


@dataclass
class CLI:
    command: Union[Acquire, Db, Health] = subparsers(
        {"acquire": Acquire, "db": Db, "health": Health}
    )

    color: bool = False
    verbose: int = field(
        alias=["-v"],
        default=0,
        help="logging levels of information about the process (-v: INFO. -vv: DEBUG)",
        action="count",
    )

    def execute(self) -> int:
        # levels = {0: logging.WARNING, 1: logging.INFO, 2: logging.DEBUG}

        # if self.color:
        #     logging.basicConfig(
        #         handlers=[NiceHandler(), getHandler()],
        #         level=levels.get(self.verbose, logging.DEBUG),
        #     )

        # else:
        #     logging.basicConfig(
        #         handlers=[getHandler()],
        #         format="%(asctime)-15s::%(levelname)s::%(name)s::%(message)s",
        #         level=levels.get(self.verbose, logging.DEBUG),
        #     )

        # logger = logging.getLogger(__name__)

        # # logger.debug("SARC version : %s", sarc.__version__)
        # logger.debug(f"Running command: {self.command}")
        # logger.warning(f"Test warning log")

        return self.command.execute()


def main(argv: list[Any] | None = None) -> int:
    """Main commandline for SARC"""

    setupLogging()

    parser = ArgumentParser()
    parser.add_arguments(CLI, dest="command")
    args = parser.parse_args(argv)
    command: CLI = args.command

    return command.execute()


if __name__ == "__main__":
    returncode = main()
    if returncode > 0:
        raise SystemExit(returncode)
