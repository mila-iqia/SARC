from datetime import date

import gifnoc
from pytest import raises
from sqlalchemy.exc import ProgrammingError
from sqlmodel import select

from sarc.db import init_insert
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
        + "2025-04-01,  2026-04-01,  fromage,    fromage-gpu,    rrg-bonhomme-ad_gpu, ,         10,       100000,\n"
        "2026-04-01,  2027-04-01,  fromage,    fromage-gpu,    rrg-bonhomme-ad_gpu, ,         10,       110000,\n"
        "2025-04-01,  2026-04-01,  fromage,    fromage-gpu,    def-bonhomme_gpu,    ,         2,        5000,\n"
        "2025-04-01,  2026-04-01,  patate,     patate-gpu,     rrg-bonhomme-ad_cpu, ,         3,        40000,\n"
        # cpu and storage rows carry no rgu_year: skipped, not budgeted.
        "2025-04-01,  2026-04-01,  fromage,    fromage-compute,rrg-bonhomme-ad,     873,\n"
        "2025-04-01,  2026-04-01,  fromage,    fromage-storage,rrg-bonhomme-ad,     ,         ,         ,         150TB,\n",
    )


def table_rows(sess):
    return sorted(
        (
            (row.cluster_id, row.allocation, row.start, row.end, row.rgu_year)
            for row in sess.exec(select(TargetShareDB)).all()
        ),
        key=lambda t: (t[1] or "", t[2]),
    )


def test_insert_target_shares(tmp_path, empty_read_write_db):
    from sarc.db.cluster import SlurmClusterDB

    fromage_id = empty_read_write_db.exec(
        select(SlurmClusterDB.id).where(SlurmClusterDB.name == "fromage")
    ).one()
    patate_id = empty_read_write_db.exec(
        select(SlurmClusterDB.id).where(SlurmClusterDB.name == "patate")
    ).one()

    with gifnoc.overlay({"sarc.target_shares": str(configured_csv(tmp_path))}):
        insert_target_shares()

    # Consecutive years [2025-04-01, 2026-04-01) + [2026-04-01, 2027-04-01)
    # coexist: the half-open periods only touch, they do not overlap. The cpu
    # and storage rows are absent: no rgu_year, no budget.
    assert table_rows(empty_read_write_db) == [
        (fromage_id, "def-bonhomme_gpu", date(2025, 4, 1), date(2026, 4, 1), 5000),
        (patate_id, "rrg-bonhomme-ad_cpu", date(2025, 4, 1), date(2026, 4, 1), 40000),
        (fromage_id, "rrg-bonhomme-ad_gpu", date(2025, 4, 1), date(2026, 4, 1), 100000),
        (fromage_id, "rrg-bonhomme-ad_gpu", date(2026, 4, 1), date(2027, 4, 1), 110000),
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
        + "2025-04-01,  2026-04-01,  fromage,    fromage-gpu,    rrg-bonhomme-ad_gpu, ,         10,       99999,\n",
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
        + "2025-04-01,  2026-04-01,  fromage,    fromage-gpu,    rrg-bonhomme-ad_gpu, ,         10,       100,\n",
    )
    insert_target_shares()  # config.target_shares is null in the test config
    assert table_rows(empty_read_write_db) == []


def test_missing_file_is_noop(empty_read_write_db):
    # A configured path that is absent (relative default, wrong deploy) warns
    # and leaves the table alone rather than failing the deploy.
    with gifnoc.overlay({"sarc.target_shares": "/nowhere/allocations.csv"}):
        insert_target_shares()
    assert table_rows(empty_read_write_db) == []


def test_overlap_in_csv_rejected(tmp_path, empty_read_write_db):
    with gifnoc.overlay({"sarc.target_shares": str(configured_csv(tmp_path))}):
        insert_target_shares()
    before = table_rows(empty_read_write_db)

    bad = write_csv(
        tmp_path,
        HEADER
        + "2025-09-01,  2026-09-01,  fromage,    fromage-gpu,    rrg-bonhomme-ad_gpu, ,         10,       100000,\n"
        "2026-01-01,  2027-01-01,  fromage,    fromage-gpu,    rrg-bonhomme-ad_gpu, ,         10,       110000,\n",
    )
    with gifnoc.overlay({"sarc.target_shares": str(bad)}), raises(ValueError) as e:
        insert_target_shares()
    assert "overlapping target shares" in str(e.value)
    # Failed import leaves the previous content untouched.
    assert table_rows(empty_read_write_db) == before


def test_unknown_cluster_skipped(tmp_path, empty_read_write_db):
    """A budget for a cluster SARC does not know is skipped, not fatal: the
    allocations.csv also lists not-yet-instrumented clusters."""
    from sarc.db.cluster import SlurmClusterDB

    patate_id = empty_read_write_db.exec(
        select(SlurmClusterDB.id).where(SlurmClusterDB.name == "patate")
    ).one()
    csv = write_csv(
        tmp_path,
        HEADER
        + "2025-04-01,  2026-04-01,  patate,     patate-gpu,     rrg-bonhomme-ad_gpu, ,         10,       100000,\n"
        "2025-04-01,  2026-04-01,  nowhere,    nowhere-gpu,    rrg-bonhomme-ad_gpu, ,         10,       100000,\n",
    )
    with gifnoc.overlay({"sarc.target_shares": str(csv)}):
        insert_target_shares()
    # The known-cluster row lands, the unknown-cluster row is skipped.
    assert table_rows(empty_read_write_db) == [
        (patate_id, "rrg-bonhomme-ad_gpu", date(2025, 4, 1), date(2026, 4, 1), 100000)
    ]


def test_invalid_row_rejected(tmp_path, empty_read_write_db):
    bad = write_csv(
        tmp_path,
        HEADER
        + "2025-04-01,  2026-04-01,  fromage,    fromage-gpu,    rrg-bonhomme-ad_gpu, ,         10,       cent mille,\n",
    )
    with gifnoc.overlay({"sarc.target_shares": str(bad)}), raises(ValueError):
        insert_target_shares()
    assert table_rows(empty_read_write_db) == []


def test_db_exclusion_constraint_rejects_overlap(tmp_path, empty_read_write_db):
    """The DB constraint holds even when inserting rows directly."""
    from sarc.db.cluster import SlurmClusterDB

    fromage_id = empty_read_write_db.exec(
        select(SlurmClusterDB.id).where(SlurmClusterDB.name == "fromage")
    ).one()
    empty_read_write_db.add_all(
        [
            TargetShareDB(
                cluster_id=fromage_id,
                allocation="rrg-bonhomme-ad_gpu",
                start="2025-04-01",
                end="2026-04-01",
                rgu_year=100000,
                timestamp="2026-04-01T00:00:00+00:00",
            ),
            TargetShareDB(
                cluster_id=fromage_id,
                allocation="rrg-bonhomme-ad_gpu",
                start="2025-09-01",
                end="2026-09-01",
                rgu_year=110000,
                timestamp="2026-04-01T00:00:00+00:00",
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


def test_cluster_wide_row_conflicts_with_concrete_allocation(
    tmp_path, empty_read_write_db
):
    """An all-accounts budget overlaps any account budget of that cluster."""
    csv = write_csv(
        tmp_path,
        HEADER
        + "2026-04-01,  2027-04-01,  fromage,    fromage-gpu,    rrg-bonhomme-ad_gpu, ,         10,       100000,\n"
        "2026-04-01,  2027-04-01,  fromage,    fromage,        ,                    ,         10,       150000,\n",
    )
    with gifnoc.overlay({"sarc.target_shares": str(csv)}), raises(ValueError) as e:
        insert_target_shares()
    assert "all accounts" in str(e.value)
    assert table_rows(empty_read_write_db) == []


def test_db_constraint_rejects_two_cluster_wide_rows(tmp_path, empty_read_write_db):
    """The coalesce() in the exclusion constraint makes NULL allocations
    conflict with each other instead of escaping the check."""
    from sarc.db.cluster import SlurmClusterDB

    fromage_id = empty_read_write_db.exec(
        select(SlurmClusterDB.id).where(SlurmClusterDB.name == "fromage")
    ).one()
    empty_read_write_db.add_all(
        [
            TargetShareDB(
                cluster_id=fromage_id,
                allocation=None,
                start="2025-04-01",
                end="2026-04-01",
                rgu_year=100000,
                timestamp="2026-04-01T00:00:00+00:00",
            ),
            TargetShareDB(
                cluster_id=fromage_id,
                allocation=None,
                start="2025-09-01",
                end="2026-09-01",
                rgu_year=110000,
                timestamp="2026-04-01T00:00:00+00:00",
            ),
        ]
    )
    with raises(ProgrammingError):
        empty_read_write_db.commit()
    empty_read_write_db.rollback()
