"""
Connect to the local MongoDB database.
Get the information from the "matches_done.json" file.
Commit the new information to the database.

"""

import pymongo
from sarc.common.config import get_config

def run():

    collection_name = "users"
    

    connection_string = "mongodb://%s:%s" % (get_config()["mongodb"]["host"], get_config()["mongodb"]["port"])
    mc = pymongo.MongoClient(connection_string)

    mc[collection_name]


if __name__ == '__main__':
    run()