from simple_parsing import ArgumentParser

from sarc.cli.db.init import DbInit


def add_db_commands(parser: ArgumentParser):
    subparsers = parser.add_subparsers(
        title="subcommand",
        description="subcommand description",
        dest="subcommand_name",
        required=True,
    )
    db_init_subparser = subparsers.add_parser("init", help="Initialize the DB")
    db_init_subparser.add_arguments(DbInit, dest="subcommand")
