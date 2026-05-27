import sqlmodel

from sarc.config import config
from sarc.db.job import SlurmJobDB


def main():
    with config("scraping").db.session() as sess:
        count = sess.exec(sqlmodel.select(sqlmodel.func.count(SlurmJobDB))).one()
        print(count)


if __name__ == "__main__":
    main()
