from datetime import date

import gifnoc
from pytest import raises
from sqlalchemy.exc import ProgrammingError
from sqlmodel import select

from sarc.db.target_share import TargetShareDB, insert_target_shares


def write_csv(tmp_path, content):
    path = tmp_path / "target_shares.csv"
    path.write_text(content)
    return path


def configured_csv(tmp_path):
    return write_csv(
        tmp_path,
        "start,end,cluster_name,allocation,rgu_hours\n"
        "2025-04-01,2026-04-01,fromage,rrg-bonhomme-ad_gpu,100000\n"
        "2026-04-01,2027-04-01,fromage,rrg-bonhomme-ad_gpu,110000\n"
        "2025-04-01,2026-04-01,fromage,def-bonhomme_gpu,5000\n"
        "2025-04-01,2026-04-01,patate,rrg-bonhomme-ad_cpu,40000\n",
    )


def table_rows(sess):
    return sorted(
        (
            (row.cluster_id, row.allocation, row.start, row.end, row.rgu_hours)
            for row in sess.exec(select(TargetShareDB)).all()
        ),
        key=lambda t: (t[1], t[2]),
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
    # coexist: the half-open periods only touch, they do not overlap.
    assert table_rows(empty_read_write_db) == [
        (fromage_id, "def-bonhomme_gpu", date(2025, 4, 1), date(2026, 4, 1), 5000.0),
        (patate_id, "rrg-bonhomme-ad_cpu", date(2025, 4, 1), date(2026, 4, 1), 40000.0),
        (
            fromage_id,
            "rrg-bonhomme-ad_gpu",
            date(2025, 4, 1),
            date(2026, 4, 1),
            100000.0,
        ),
        (
            fromage_id,
            "rrg-bonhomme-ad_gpu",
            date(2026, 4, 1),
            date(2027, 4, 1),
            110000.0,
        ),
    ]


def test_full_refresh(tmp_path, empty_read_write_db):
    """The table mirrors the CSV exactly: edits and deletions both apply."""
    with gifnoc.overlay({"sarc.target_shares": str(configured_csv(tmp_path))}):
        insert_target_shares()
    assert len(table_rows(empty_read_write_db)) == 4

    replacement = write_csv(
        tmp_path,
        "start,end,cluster_name,allocation,rgu_hours\n"
        "2025-04-01,2026-04-01,fromage,rrg-bonhomme-ad_gpu,99999\n",
    )
    with gifnoc.overlay({"sarc.target_shares": str(replacement)}):
        insert_target_shares()

    rows = table_rows(empty_read_write_db)
    assert len(rows) == 1
    assert rows[0][4] == 99999.0


def test_config_unset_is_noop(tmp_path, empty_read_write_db):
    write_csv(
        tmp_path,
        "start,end,cluster_name,allocation,rgu_hours\n"
        "2025-04-01,2026-04-01,fromage,rrg-bonhomme-ad_gpu,100\n",
    )
    insert_target_shares()  # config.target_shares is None in the test config
    assert table_rows(empty_read_write_db) == []


def test_overlap_in_csv_rejected(tmp_path, empty_read_write_db):
    with gifnoc.overlay({"sarc.target_shares": str(configured_csv(tmp_path))}):
        insert_target_shares()
    before = table_rows(empty_read_write_db)

    bad = write_csv(
        tmp_path,
        "start,end,cluster_name,allocation,rgu_hours\n"
        "2025-09-01,2026-09-01,fromage,rrg-bonhomme-ad_gpu,100000\n"
        "2026-01-01,2027-01-01,fromage,rrg-bonhomme-ad_gpu,110000\n",
    )
    with gifnoc.overlay({"sarc.target_shares": str(bad)}), raises(ValueError) as e:
        insert_target_shares()
    assert "overlapping target shares" in str(e.value)
    # Failed import leaves the previous content untouched.
    assert table_rows(empty_read_write_db) == before


def test_unknown_cluster_rejected(tmp_path, empty_read_write_db):
    bad = write_csv(
        tmp_path,
        "start,end,cluster_name,allocation,rgu_hours\n"
        "2025-04-01,2026-04-01,patate,rrg-bonhomme-ad_gpu,100000\n"
        "2025-04-01,2026-04-01,nowhere,rrg-bonhomme-ad_gpu,100000\n",
    )
    with gifnoc.overlay({"sarc.target_shares": str(bad)}), raises(ValueError) as e:
        insert_target_shares()
    assert "nowhere" in str(e.value)
    # Partial import rolled back: the first row is not in either.
    assert table_rows(empty_read_write_db) == []


def test_allocation_not_a_configured_account_rejected(tmp_path, empty_read_write_db):
    bad = write_csv(
        tmp_path,
        "start,end,cluster_name,allocation,rgu_hours\n"
        "2025-04-01,2026-04-01,fromage,rrg-bengioy-ad_gpu,100000\n",
    )
    with gifnoc.overlay({"sarc.target_shares": str(bad)}), raises(ValueError) as e:
        insert_target_shares()
    assert "rrg-bengioy-ad_gpu" in str(e.value)
    assert table_rows(empty_read_write_db) == []


def test_cluster_without_accounts_list_accepts_any_allocation(
    tmp_path, empty_read_write_db
):
    """`local` has accounts: null — every allocation is accepted."""
    csv = write_csv(
        tmp_path,
        "start,end,cluster_name,allocation,rgu_hours\n"
        "2025-04-01,2026-04-01,local,rrg-importante_gpu,100000\n",
    )
    with gifnoc.overlay({"sarc.target_shares": str(csv)}):
        insert_target_shares()
    assert len(table_rows(empty_read_write_db)) == 1


def test_invalid_row_rejected(tmp_path, empty_read_write_db):
    bad = write_csv(
        tmp_path,
        "start,end,cluster_name,allocation,rgu_hours\n"
        "2025-04-01,2026-04-01,fromage,rrg-bonhomme-ad_gpu,cent mille\n",
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
                rgu_hours=100000,
                timestamp="2026-04-01T00:00:00+00:00",
            ),
            TargetShareDB(
                cluster_id=fromage_id,
                allocation="rrg-bonhomme-ad_gpu",
                start="2025-09-01",
                end="2026-09-01",
                rgu_hours=110000,
                timestamp="2026-04-01T00:00:00+00:00",
            ),
        ]
    )
    # pg8000 maps the exclusion-violation SQLSTATE (23P01) to ProgrammingError,
    # not IntegrityError.
    with raises(ProgrammingError):
        empty_read_write_db.commit()
    empty_read_write_db.rollback()
