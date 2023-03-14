from simple_parsing import ArgumentParser

from sarc.cli.acquire.allocations import AcquireAllocations
from sarc.cli.acquire.jobs import AcquireJobs
from sarc.cli.acquire.storages import AcquireStorages


def add_acquire_commands(parser: ArgumentParser):
    subparsers = parser.add_subparsers(
        title="subcommand",
        description="Acquire subcommand",
        dest="subcommand_name",
        required=True,
    )
    allocations_parser = subparsers.add_parser(
        "allocations", help="Acquire allocations help"
    )
    allocations_parser.add_arguments(AcquireAllocations, dest="subcommand")

    jobs_parser = subparsers.add_parser("jobs", help="Acquire jobs help")
    jobs_parser.add_arguments(AcquireJobs, dest="subcommand")

    storages_parser = subparsers.add_parser("storages", help="Acquire storages help")
    storages_parser.add_arguments(AcquireStorages, dest="subcommand")
