import pytest
import sqlmodel

from sarc.db.cluster import SlurmClusterDB
from sarc.db.support import GpuRguDB
from sarc.db.users import UserDB
from tests.db.factory import create_gpu_billings


@pytest.fixture
def rgu_db(empty_read_write_db):
    """
    Dedicated testing database, based on empty_read_write_db to check RGUs.
    Add GPU billings, fake RGUs and one user.
    Each test will still have to add custom jobs.
    """
    clusters = empty_read_write_db.exec(sqlmodel.select(SlurmClusterDB)).all()
    empty_read_write_db.add_all(create_gpu_billings(clusters=clusters))

    # Clear pre-existing GpuRguDB entries (inserted by db_upgrade from IGUANE)
    # so tests only see the fake ones below.
    empty_read_write_db.exec(sqlmodel.delete(GpuRguDB))
    # Fake GPUs with RGU values
    empty_read_write_db.add_all(
        [
            GpuRguDB(name=name, rgu=rgu, drac_rgu=rgu)
            for name, rgu in {
                "A100": 3.21,
                "raisin_gpu_with_rgu_no_billing": 1.5,
                "raisin_gpu_with_rgu_with_billing": 2.5,
                "patate_gpu_with_rgu_no_billing": 3.5,
                "patate_gpu_with_rgu_with_billing": 4.5,
                "mila_gpu_no_rgu_no_billing": 7,
                "mila_gpu_no_rgu_with_billing": 2 * 7,
                "mila_gpu_with_rgu_no_billing": 3 * 7,
                "mila_gpu_with_rgu_with_billing": 4 * 7,
            }.items()
        ]
    )

    # Insert a test user, required as foreign key target for any SlurmJobDB.sarc_user_id.
    empty_read_write_db.add(UserDB(display_name="test", email="test@mila.quebec"))
    empty_read_write_db.commit()
    yield empty_read_write_db
