# /// script
# requires-python = ">=3.9"
# dependencies = [
#     "pymongo",
# ]
# ///
import json

from pymongo import MongoClient

connection_string = "mongodb://readuser:readpwd@localhost:27017/sarc"
database_name = "sarc"

client = MongoClient(connection_string)
db = client.get_database(database_name)
users = list(db.users.find())

for u in users:
    del u["_id"]

with open("old_users.json", "w") as f:
    json.dump(users, f, default=str)
