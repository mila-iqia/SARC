from __future__ import annotations

import logging
from typing import Protocol

from simple_parsing import ArgumentParser

from sarc.cli.acquire import add_acquire_commands
from sarc.cli.db import add_db_commands

logger = logging.getLogger(__name__)


class Command(Protocol):
    def execute(self) -> int:
        ...


def main(argv: list[str] | None = None) -> int:
    parser = ArgumentParser()
    # parser.add_arguments(GlobalArgs, dest="global_args")

    parser.add_argument(
        "-v",
        "--verbose",
        default=0,
        action="count",
        help="logging levels of information about the process (-v: INFO. -vv: DEBUG)",
    )

    command_subparsers = parser.add_subparsers(
        dest="command_name",
        title="Command title",
        description="Description",
        required=True,
    )

    db_parser = command_subparsers.add_parser("db", help="database-related commands")

    add_db_commands(db_parser)

    acquire_parser = command_subparsers.add_parser(
        "acquire", help="commands used acquire different kinds of data"
    )

    add_acquire_commands(acquire_parser)

    args = parser.parse_args(argv)

    verbose: int = args.verbose
    # NOTE: unused, but available in case it's needed:
    # command_name: str = args.command_name
    # subcommand_name: str = args.subcommand_name
    subcommand: Command = args.subcommand

    levels = {0: logging.WARNING, 1: logging.INFO, 2: logging.DEBUG}
    logging.basicConfig(
        format="%(asctime)-15s::%(levelname)s::%(name)s::%(message)s",
        level=levels.get(verbose, logging.DEBUG),
    )

    return subcommand.execute()


if __name__ == "__main__":
    returncode = main()
    if returncode > 0:
        raise SystemExit(returncode)
