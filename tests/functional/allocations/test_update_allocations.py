import os

import pytest

from sarc.allocations import get_allocations
from sarc.cli import main

FOLDER = os.path.dirname(os.path.abspath(__file__))


@pytest.mark.freeze_time("2023-02-15")
@pytest.mark.usefixtures("init_empty_db")
def test_update_allocations(data_regression):
    assert get_allocations(cluster_name=["fromage", "patate"]) == []
    main(["acquire", "allocations", "--file", os.path.join(FOLDER, "allocations.csv")])
    data = get_allocations(cluster_name=["fromage", "patate"])
    assert len(data) == 11
    data_regression.check(
        [allocation.json(exclude={"id": True}) for allocation in data]
    )


@pytest.mark.freeze_time("2023-02-15")
@pytest.mark.usefixtures("init_empty_db")
def test_update_allocations_no_duplicates(data_regression):
    assert get_allocations(cluster_name=["fromage", "patate"]) == []
    main(["acquire", "allocations", "--file", os.path.join(FOLDER, "allocations.csv")])
    data = get_allocations(cluster_name=["fromage", "patate"])
    assert len(data) == 11
    main(["acquire", "allocations", "--file", os.path.join(FOLDER, "allocations.csv")])
    data = get_allocations(cluster_name=["fromage", "patate"])
    assert len(data) == 11
    data_regression.check(
        [allocation.json(exclude={"id": True}) for allocation in data]
    )


@pytest.mark.freeze_time("2023-02-15")
@pytest.mark.usefixtures("init_empty_db")
def test_update_allocations_invalid_with_some_valid(data_regression):
    assert get_allocations(cluster_name=["fromage", "patate"]) == []
    main(
        [
            "acquire",
            "allocations",
            "--file",
            os.path.join(FOLDER, "invalid_allocations.csv"),
        ]
    )
    data = get_allocations(cluster_name=["fromage", "patate"])
    assert len(data) == 2
    data_regression.check(
        [allocation.json(exclude={"id": True}) for allocation in data]
    )


@pytest.mark.freeze_time("2023-02-14")
@pytest.mark.usefixtures("init_empty_db")
def test_update_allocations_invalid_error_msg(data_regression, capsys):
    assert get_allocations(cluster_name=["fromage", "patate"]) == []
    main(
        [
            "acquire",
            "allocations",
            "--file",
            os.path.join(FOLDER, "invalid_allocations.csv"),
        ]
    )
    data = get_allocations(cluster_name=["fromage", "patate"])
    assert len(data) == 2
    data_regression.check(capsys.readouterr().out)
