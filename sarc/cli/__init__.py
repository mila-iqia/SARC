from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Union

from simple_parsing import ArgumentParser, field, subparsers

from sarc.cli.acquire import Acquire

logger = logging.getLogger(__name__)


@dataclass
class CLI:
    # NOTE: This should use Union[Acquire, OtherCommand] when we have more than one command.
    command: Acquire = subparsers({"acquire": Acquire})

    verbose: int = field(
        alias=["-v"],
        default=0,
        help="logging levels of information about the process (-v: INFO. -vv: DEBUG)",
        action="count",
    )

    def execute(self) -> int:
        levels = {0: logging.WARNING, 1: logging.INFO, 2: logging.DEBUG}

        logging.basicConfig(
            format="%(asctime)-15s::%(levelname)s::%(name)s::%(message)s",
            level=levels.get(self.verbose, logging.DEBUG),
        )

        # logger.debug("SARC version : %s", sarc.__version__)

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
