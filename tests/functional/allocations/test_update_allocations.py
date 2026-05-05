import os
import re

import pytest
from sqlmodel import Session, select

from sarc.cli import main
from sarc.db.allocation import AllocationDB
from sarc.db.cluster import SlurmClusterDB

FOLDER = os.path.dirname(os.path.abspath(__file__))


@pytest.mark.freeze_time("2023-02-15")
@pytest.mark.usefixtures("enabled_cache")
def test_update_allocations(data_regression, empty_read_write_db: Session):
    assert (
        len(
            empty_read_write_db.exec(
                select(AllocationDB)
                .join(SlurmClusterDB, SlurmClusterDB.id == AllocationDB.cluster_id)
                .where(SlurmClusterDB.name.in_(["fromage", "patate"]))
            ).all()
        )
        == 0
    )
    main(["fetch", "allocations", "--file", os.path.join(FOLDER, "allocations.csv")])
    main(["parse", "allocations", "--since", "2023-02-14"])
    data = empty_read_write_db.exec(
        select(AllocationDB)
        .join(SlurmClusterDB, SlurmClusterDB.id == AllocationDB.cluster_id)
        .where(SlurmClusterDB.name.in_(["fromage", "patate"]))
    ).all()
    assert len(data) == 11
    data_regression.check(
        [allocation.model_dump(exclude={"id"}) for allocation in data]
    )


@pytest.mark.freeze_time("2023-02-15")
@pytest.mark.usefixtures("enabled_cache")
def test_update_allocations_no_duplicates(
    data_regression, empty_read_write_db: Session
):
    assert (
        len(
            empty_read_write_db.exec(
                select(AllocationDB)
                .join(SlurmClusterDB, SlurmClusterDB.id == AllocationDB.cluster_id)
                .where(SlurmClusterDB.name.in_(["fromage", "patate"]))
            ).all()
        )
        == 0
    )
    main(["fetch", "allocations", "--file", os.path.join(FOLDER, "allocations.csv")])
    main(["parse", "allocations", "--since", "2023-02-14"])
    data = empty_read_write_db.exec(
        select(AllocationDB)
        .join(SlurmClusterDB, SlurmClusterDB.id == AllocationDB.cluster_id)
        .where(SlurmClusterDB.name.in_(["fromage", "patate"]))
    ).all()
    assert len(data) == 11
    main(["parse", "allocations", "--since", "2023-02-14"])
    data = empty_read_write_db.exec(
        select(AllocationDB)
        .join(SlurmClusterDB, SlurmClusterDB.id == AllocationDB.cluster_id)
        .where(SlurmClusterDB.name.in_(["fromage", "patate"]))
    ).all()
    assert len(data) == 11
    data_regression.check(
        [allocation.model_dump(exclude={"id"}) for allocation in data]
    )


@pytest.mark.freeze_time("2023-02-15")
@pytest.mark.usefixtures("enabled_cache")
def test_update_allocations_invalid_with_some_valid(
    data_regression, empty_read_write_db
):
    assert (
        len(
            empty_read_write_db.exec(
                select(AllocationDB)
                .join(SlurmClusterDB, SlurmClusterDB.id == AllocationDB.cluster_id)
                .where(SlurmClusterDB.name.in_(["fromage", "patate"]))
            ).all()
        )
        == 0
    )
    main(
        [
            "fetch",
            "allocations",
            "--file",
            os.path.join(FOLDER, "invalid_allocations.csv"),
        ]
    )
    main(["parse", "allocations", "--since", "2023-02-14"])
    data = empty_read_write_db.exec(
        select(AllocationDB)
        .join(SlurmClusterDB, SlurmClusterDB.id == AllocationDB.cluster_id)
        .where(SlurmClusterDB.name.in_(["fromage", "patate"]))
    ).all()
    assert len(data) == 2
    data_regression.check(
        [allocation.model_dump(exclude={"id"}) for allocation in data]
    )


@pytest.mark.freeze_time("2023-02-14")
@pytest.mark.usefixtures("enabled_cache")
def test_update_allocations_invalid_error_msg(caplog, empty_read_write_db):
    assert (
        len(
            empty_read_write_db.exec(
                select(AllocationDB)
                .join(SlurmClusterDB, SlurmClusterDB.id == AllocationDB.cluster_id)
                .where(SlurmClusterDB.name.in_(["fromage", "patate"]))
            ).all()
        )
        == 0
    )
    main(
        [
            "fetch",
            "allocations",
            "--file",
            os.path.join(FOLDER, "invalid_allocations.csv"),
        ]
    )
    main(["parse", "allocations", "--since", "2023-02-13"])
    data = empty_read_write_db.exec(
        select(AllocationDB)
        .join(SlurmClusterDB, SlurmClusterDB.id == AllocationDB.cluster_id)
        .where(SlurmClusterDB.name.in_(["fromage", "patate"]))
    ).all()
    assert len(data) == 2
    print(caplog.text)
    assert bool(
        re.search(
            r"pydantic_core._pydantic_core.ValidationError: 1 validation error for AllocationDB\ncpu_year\n  Input should be a valid integer, unable to parse string as an integer \[type=int_parsing, input_value='allo', input_type=str\]",
            caplog.text,
            flags=re.MULTILINE,
        )
    )
    assert bool(
        re.search(
            r"pydantic_core._pydantic_core.ValidationError: 1 validation error for AllocationDB\ngpu_year\n  Input should be a valid integer, unable to parse string as an integer \[type=int_parsing, input_value='coucou', input_type=str\]",
            caplog.text,
            flags=re.MULTILINE,
        )
    )
    assert bool(
        re.search(
            r"pydantic_core._pydantic_core.ValidationError: 1 validation error for AllocationDB\nvcpu_year\n  Input should be a valid integer, unable to parse string as an integer \[type=int_parsing, input_value='oups', input_type=str\]",
            caplog.text,
            flags=re.MULTILINE,
        )
    )
    assert bool(
        re.search(
            r"pydantic_core._pydantic_core.ValidationError: 1 validation error for AllocationDB\nvgpu_year\n  Input should be a valid integer, unable to parse string as an integer \[type=int_parsing, input_value='marche-pas', input_type=str\]",
            caplog.text,
            flags=re.MULTILINE,
        )
    )
    assert bool(
        re.search(
            r"pydantic_core._pydantic_core.ValidationError: 1 validation error for AllocationDB\nproject_size\n  could not parse value and unit from byte string \[type=byte_size, input_value='mille', input_type=str\]",
            caplog.text,
            flags=re.MULTILINE,
        )
    )
    assert bool(
        re.search(
            r"pydantic_core._pydantic_core.ValidationError: 1 validation error for AllocationDB\nproject_inodes\n  Input should be a valid number, unable to parse string as a number \[type=float_parsing, input_value='cinq', input_type=str\]",
            caplog.text,
            flags=re.MULTILINE,
        )
    )
    assert bool(
        re.search(
            r"pydantic_core._pydantic_core.ValidationError: 1 validation error for AllocationDB\nnearline_size\n  could not parse value and unit from byte string \[type=byte_size, input_value='patate', input_type=str\]",
            caplog.text,
            flags=re.MULTILINE,
        )
    )
    assert bool(
        re.search(
            r"pydantic_core._pydantic_core.ValidationError: 1 validation error for AllocationDB\nstart\n  Value error, time data '2020/04/01' does not match format '%Y-%m-%d' \[type=value_error, input_value='2020/04/01', input_type=str\]",
            caplog.text,
            flags=re.MULTILINE,
        )
    )
    assert bool(
        re.search(
            r"pydantic_core._pydantic_core.ValidationError: 1 validation error for AllocationDB\nend\n  Value error, time data '2021/04/01' does not match format '%Y-%m-%d' \[type=value_error, input_value='2021/04/01', input_type=str\]",
            caplog.text,
            flags=re.MULTILINE,
        )
    )
