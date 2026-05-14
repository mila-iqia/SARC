import pytest
import sqlmodel

from sarc.db.cluster import SlurmClusterDB
from sarc.db.support import GpuRguDB
from sarc.db.users import UserDB
from tests.db.factory import create_gpu_billings

from .rgu_utils import _gen_fake_rgus


@pytest.fixture
def rgu_db(empty_read_write_db):
    """
    Dedicated testing database, based on empty_read_write_db to check RGUs.
    Add GPU billings, fake RGUs and one user.
    Each test will still have to add custom jobs.
    """
    clusters = empty_read_write_db.exec(sqlmodel.select(SlurmClusterDB)).all()
    empty_read_write_db.add_all(create_gpu_billings(clusters=clusters))
    empty_read_write_db.add_all(
        [GpuRguDB(name=name, rgu=rgu) for name, rgu in _gen_fake_rgus().items()]
    )
    # Insert a test user, required as foreign key target for any SlurmJobDB.sarc_user_id.
    empty_read_write_db.add(UserDB(display_name="test", email="test@mila.quebec"))
    empty_read_write_db.commit()
    return empty_read_write_db
