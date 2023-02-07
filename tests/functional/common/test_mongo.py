import pymongo

# test connection to mongo database
def test_mongo_connection():
    myclient = pymongo.MongoClient("mongodb://localhost:27017/")
    assert myclient!=None        
    assert myclient.list_database_names()!=None

def test_mongo_version():
    myclient = pymongo.MongoClient("mongodb://localhost:27017/")
    assert myclient!=None        
    info = myclient.server_info()
    assert info['versionArray'][0]>=6    # check if MongoDb >= v6.0