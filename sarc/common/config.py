"""
This is a placeholder.

We need to have some kind of system that allows
for a configuration file to be specified so that
we have the information about the servers, and
at the same time we need to have some kind of "test"
configuration file that we can use to test the code.

We need to have some design discussion at the SARC meeting
to pick something.

"""


def get_config():
    return {
        "mongodb": {
            "host": "localhost",
            "port": 27017,
        }
    }
