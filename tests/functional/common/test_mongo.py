import pymongo
import pytest

from sarc.config import config


# test connection to test mongo database
@pytest.mark.usefixtures("standard_config")
def test_mongo_connection():
    uri = config().mongo.connection_string

    myclient = pymongo.MongoClient(uri)
    assert myclient != None
    assert myclient.list_database_names() != None


@pytest.mark.usefixtures("standard_config")
def test_mongo_version():
    uri = config().mongo.connection_string

    myclient = pymongo.MongoClient(uri)
    assert myclient != None
    info = myclient.server_info()
    assert info["versionArray"][0] >= 6  # check if MongoDb >= v6.0
