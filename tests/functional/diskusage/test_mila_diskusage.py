from pathlib import Path

import pytest

from sarc.storage.diskusage import get_diskusages
import sarc.storage.mila
from sarc.ldap.api import User, Credentials





def mock_get_user():
    return [
        User(name="user1", mila=Credentials(username="user1", email="@", active=True), drac=None, mila_ldap=dict(), drac_members=None, drac_roles=None),
        User(name="user3", mila=Credentials(username="user3", email="@", active=False), drac=None, mila_ldap=dict(), drac_members=None, drac_roles=None),
    ]

def mock_get_users():
    return [
        User(name="user1", mila=Credentials(username="user1", email="@", active=True), drac=None, mila_ldap=dict(), drac_members=None, drac_roles=None),
        User(name="user2", mila=Credentials(username="user2", email="@", active=True), drac=None, mila_ldap=dict(), drac_members=None, drac_roles=None),
        User(name="user3", mila=Credentials(username="user3", email="@", active=False), drac=None, mila_ldap=dict(), drac_members=None, drac_roles=None),
    ]

@pytest.mark.parametrize(
    "test_config", [{"clusters": {"mila": {"host": "mila"}}}], indirect=True
)
def test_mila_fetch_diskusage_single(test_config, monkeypatch, cli_main, file_regression):
    
    count = 0
    def mock_get_report(*args):
        nonlocal count
        count += 1
        
        file = Path(__file__).parent / "mila_reports/report_single_user.txt"
        with open(file, "r", encoding="utf-8") as f:
            raw_report = f.read()    
        return raw_report, None
    
    monkeypatch.setattr(sarc.storage.mila, 'get_users', mock_get_users)
    monkeypatch.setattr(sarc.storage.mila, '_fetch_diskusage_report', mock_get_report)

    cli_main(
        [
            "acquire",
            "storages",
            "-c",
            "mila",
        ]
    )

    assert count == 2
    data = get_diskusages(cluster_name=["mila"])
    assert len(data) == 1
    # report = sarc.storage.mila.fetch_diskusage_report(cluster=test_config.clusters["mila"])
    file_regression.check(data[0].json(exclude={"id": True}, indent=4))


@pytest.mark.parametrize(
    "test_config", [{"clusters": {"mila": {"host": "mila"}}}], indirect=True
)
def test_mila_fetch_diskusage_multi(test_config, monkeypatch, cli_main, file_regression):
    
    count = 0
    def mock_get_report(*args):
        nonlocal count
        count += 1
        
        file = Path(__file__).parent / "mila_reports/report_multiple_user.txt"
        with open(file, "r", encoding="utf-8") as f:
            raw_report = f.read()    
        return raw_report, None
    
    monkeypatch.setattr(sarc.storage.mila, 'get_users', mock_get_user)
    monkeypatch.setattr(sarc.storage.mila, '_fetch_diskusage_report', mock_get_report)

    cli_main(
        [
            "acquire",
            "storages",
            "-c",
            "mila",
        ]
    )

    assert count == 1
    data = get_diskusages(cluster_name=["mila"])
    assert len(data) == 1
    # report = sarc.storage.mila.fetch_diskusage_report(cluster=test_config.clusters["mila"])
    file_regression.check(data[0].json(exclude={"id": True}, indent=4))

