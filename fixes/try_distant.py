import logging

import sqlmodel

from sarc.config import config
from sarc.db.job import SlurmJobDB

logger = logging.getLogger(__name__)


def main():
    with config.db.session() as sess:
        count = sess.exec(sqlmodel.select(sqlmodel.func.count(SlurmJobDB))).one()
        logger.info(count)


if __name__ == "__main__":
    main()
