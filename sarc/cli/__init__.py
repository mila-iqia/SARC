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

        setupLogging(verbose_level=self.verbose)

        return self.command.execute()


def main(argv: list[Any] | None = None) -> int:
    """Main commandline for SARC"""

    parser = ArgumentParser()
    parser.add_arguments(CLI, dest="command")
    args = parser.parse_args(argv)
    command: CLI = args.command

    return command.execute()


if __name__ == "__main__":
    returncode = main()
    if returncode > 0:
        raise SystemExit(returncode)
