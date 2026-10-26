from datetime import date

import gifnoc
from pytest import mark, raises
from sqlalchemy.exc import ProgrammingError
from sqlmodel import select

from sarc.db import init_insert
from sarc.db.cluster import SlurmClusterDB
from sarc.db.target_share import TargetShareDB, insert_target_shares

# Real allocations.csv shape: the reader picks its five columns by name and
# ignores the rest, so the fixtures carry the neighbouring columns too.
HEADER = (
    "start,       end,         cluster_name, resource_name,  group_name,"
    "     cpu_year, gpu_year, rgu_year, project_size\n"
)


def write_csv(tmp_path, content):
    path = tmp_path / "allocations.csv"
    path.write_text(content)
    return path


def configured_csv(tmp_path):
    return write_csv(
        tmp_path,
        HEADER
        + "2025-04-01,  2026-04-01,  fromage,    fromage-gpu,    rrg-bonhomme-ad, ,         10,       100000,\n"
        "2026-04-01,  2027-04-01,  fromage,    fromage-gpu,    rrg-bonhomme-ad, ,         10,       110000,\n"
        "2024-04-01,  2025-04-01,  fromage,    fromage-gpu,    def-bonhomme,    ,         2,        5000,\n"
        "2025-04-01,  2026-04-01,  patate,     patate-gpu,     rrg-bonhomme-ad, ,         3,        40000,\n"
        # cpu and storage rows carry no rgu_year: skipped, not budgeted.
        "2025-04-01,  2026-04-01,  fromage,    fromage-compute,rrg-bonhomme-ad, 873,\n"
        "2025-04-01,  2026-04-01,  fromage,    fromage-storage,rrg-bonhomme-ad, ,         ,         ,         150TB,\n",
    )


def table_rows(sess):
    return sorted(
        (
            (cluster_name, row.allocation, row.start_date, row.end_date, row.rgu_year)
            for row, cluster_name in sess.exec(
                select(TargetShareDB, SlurmClusterDB.name).join(SlurmClusterDB)
            ).all()
        ),
        key=lambda t: (t[0], t[1] or "", t[2]),
    )


def test_insert_target_shares(tmp_path, empty_read_write_db):
    with gifnoc.overlay({"sarc.target_shares": str(configured_csv(tmp_path))}):
        insert_target_shares()

    # Consecutive years [2025-04-01, 2026-04-01) + [2026-04-01, 2027-04-01)
    # coexist: the half-open periods only touch, they do not overlap. The same
    # allocation on two clusters does not conflict either. The cpu and storage
    # rows are absent: no rgu_year, no budget.
    assert table_rows(empty_read_write_db) == [
        ("fromage", "def-bonhomme", date(2024, 4, 1), date(2025, 4, 1), 5000),
        ("fromage", "rrg-bonhomme-ad", date(2025, 4, 1), date(2026, 4, 1), 100000),
        ("fromage", "rrg-bonhomme-ad", date(2026, 4, 1), date(2027, 4, 1), 110000),
        ("patate", "rrg-bonhomme-ad", date(2025, 4, 1), date(2026, 4, 1), 40000),
    ]


def test_init_insert_imports_target_shares(tmp_path, empty_read_write_db):
    """init_insert, the deploy path, runs the import; the other tests call
    insert_target_shares directly."""
    with gifnoc.overlay({"sarc.target_shares": str(configured_csv(tmp_path))}):
        init_insert()
    assert len(table_rows(empty_read_write_db)) == 4


def test_full_refresh(tmp_path, empty_read_write_db):
    """The table mirrors the CSV exactly: edits and deletions both apply."""
    with gifnoc.overlay({"sarc.target_shares": str(configured_csv(tmp_path))}):
        insert_target_shares()
    assert len(table_rows(empty_read_write_db)) == 4

    replacement = write_csv(
        tmp_path,
        HEADER
        + "2025-04-01,  2026-04-01,  fromage,    fromage-gpu,    rrg-bonhomme-ad, ,         10,       99999,\n",
    )
    with gifnoc.overlay({"sarc.target_shares": str(replacement)}):
        insert_target_shares()

    rows = table_rows(empty_read_write_db)
    assert len(rows) == 1
    assert rows[0][4] == 99999


def test_missing_column_rejected(tmp_path, empty_read_write_db):
    bad = write_csv(
        tmp_path,
        "start,end,cluster_name,resource_name,cpu_year\n"
        "2025-04-01,2026-04-01,fromage,fromage-gpu,10\n",
    )
    with gifnoc.overlay({"sarc.target_shares": str(bad)}), raises(ValueError) as e:
        insert_target_shares()
    assert "missing column" in str(e.value)


def test_config_null_is_noop(tmp_path, empty_read_write_db):
    write_csv(
        tmp_path,
        HEADER
        + "2025-04-01,  2026-04-01,  fromage,    fromage-gpu,    rrg-bonhomme-ad, ,         10,       100,\n",
    )
    insert_target_shares()  # config.target_shares is null in the test config
    assert table_rows(empty_read_write_db) == []


def test_missing_file_rejected(empty_read_write_db):
    """Only null disables the import: a wrong path fails the deploy."""
    with (
        gifnoc.overlay({"sarc.target_shares": "/nowhere/allocations.csv"}),
        raises(FileNotFoundError),
    ):
        insert_target_shares()


def test_overlap_in_csv_rejected(tmp_path, empty_read_write_db):
    with gifnoc.overlay({"sarc.target_shares": str(configured_csv(tmp_path))}):
        insert_target_shares()
    before = table_rows(empty_read_write_db)

    bad = write_csv(
        tmp_path,
        HEADER
        + "2025-09-01,  2026-09-01,  fromage,    fromage-gpu,    rrg-bonhomme-ad, ,         10,       100000,\n"
        "2026-01-01,  2027-01-01,  fromage,    fromage-gpu,    rrg-bonhomme-ad, ,         10,       110000,\n",
    )
    with gifnoc.overlay({"sarc.target_shares": str(bad)}), raises(ValueError) as e:
        insert_target_shares()
    assert "overlapping target shares" in str(e.value)
    # Failed import leaves the previous content untouched.
    assert table_rows(empty_read_write_db) == before


def test_unknown_cluster_skipped(tmp_path, empty_read_write_db):
    """A budget for a cluster SARC does not know is skipped, not fatal: the
    allocations.csv also lists not-yet-instrumented clusters."""
    csv = write_csv(
        tmp_path,
        HEADER
        + "2025-04-01,  2026-04-01,  patate,     patate-gpu,     rrg-bonhomme-ad, ,         10,       100000,\n"
        "2025-04-01,  2026-04-01,  nowhere,    nowhere-gpu,    rrg-bonhomme-ad, ,         10,       100000,\n",
    )
    with gifnoc.overlay({"sarc.target_shares": str(csv)}):
        insert_target_shares()
    # The known-cluster row lands, the unknown-cluster row is skipped.
    assert table_rows(empty_read_write_db) == [
        ("patate", "rrg-bonhomme-ad", date(2025, 4, 1), date(2026, 4, 1), 100000)
    ]


def test_invalid_row_rejected(tmp_path, empty_read_write_db):
    bad = write_csv(
        tmp_path,
        HEADER
        + "2025-04-01,  2026-04-01,  fromage,    fromage-gpu,    rrg-bonhomme-ad, ,         10,       cent mille,\n",
    )
    with gifnoc.overlay({"sarc.target_shares": str(bad)}), raises(ValueError):
        insert_target_shares()
    assert table_rows(empty_read_write_db) == []


@mark.parametrize(
    ("first", "second"),
    [
        ("rrg-bonhomme-ad", "rrg-bonhomme-ad"),
        ("rrg-bonhomme-ad", "def-bonhomme"),
        ("rrg-bonhomme-ad", None),
        (None, None),
    ],
)
def test_db_constraint_rejects_overlap(empty_read_write_db, first, second):
    """The DB constraint holds even when inserting rows directly: one target
    share per cluster at any instant, whatever the allocations."""
    fromage_id = empty_read_write_db.exec(
        select(SlurmClusterDB.id).where(SlurmClusterDB.name == "fromage")
    ).one()
    empty_read_write_db.add_all(
        [
            TargetShareDB(
                cluster_id=fromage_id,
                allocation=first,
                start_date="2025-04-01",
                end_date="2026-04-01",
                rgu_year=100000,
            ),
            TargetShareDB(
                cluster_id=fromage_id,
                allocation=second,
                start_date="2025-09-01",
                end_date="2026-09-01",
                rgu_year=110000,
            ),
        ]
    )
    # pg8000 maps the exclusion-violation SQLSTATE (23P01) to ProgrammingError,
    # not IntegrityError.
    with raises(ProgrammingError):
        empty_read_write_db.commit()
    empty_read_write_db.rollback()


def test_empty_allocation_is_whole_cluster(tmp_path, empty_read_write_db):
    """A blank group_name column imports as NULL and covers every account."""
    csv = write_csv(
        tmp_path,
        HEADER
        + "2026-04-01,  2027-04-01,  patate,     patate,         ,                    ,         10,       1000,\n",
    )
    with gifnoc.overlay({"sarc.target_shares": str(csv)}):
        insert_target_shares()

    rows = table_rows(empty_read_write_db)
    assert [
        row.allocation for row in empty_read_write_db.exec(select(TargetShareDB))
    ] == [None]
    assert rows[0][4] == 1000


@mark.parametrize("other", ["def-bonhomme", ""])
def test_two_allocations_on_one_cluster_conflict(tmp_path, empty_read_write_db, other):
    """A second allocation, or a whole-cluster budget, overlapping a budget of
    the same cluster is rejected: each job then has one budget at most."""
    csv = write_csv(
        tmp_path,
        HEADER
        + "2026-04-01,  2027-04-01,  fromage,    fromage-gpu,    rrg-bonhomme-ad, ,         10,       100000,\n"
        f"2026-04-01,  2027-04-01,  fromage,    fromage,        {other},    ,         10,       150000,\n",
    )
    with gifnoc.overlay({"sarc.target_shares": str(csv)}), raises(ValueError) as e:
        insert_target_shares()
    assert "overlapping target shares" in str(e.value)
    assert table_rows(empty_read_write_db) == []
