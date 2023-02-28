"""
This script is basically a wrapper around the "read_mila_ldap.py" script.
Instead of taking arguments from the command line, it takes them from 
the SARC configuration file.

This is possible because the "read_mila_ldap.py" script has a `run` function
that takes the arguments as parameters, so the argparse step comes earlier.

As a result of running this script, the values in the collection 
referenced by "cfg.ldap.mongodb_collection_name" will be updated.
"""

import tempfile
from sarc.config import config
import sarc.ldap.read_mila_ldap  # for the `run` function


def run():
    cfg = config()

    # Make a temporary file with python using the tempfile module.
    # The `with` statement is a way to make sure that the file is
    # deleted when you're done with it.
    with tempfile.NamedTemporaryFile() as tmp_file:
        tmp_file_path = tmp_file.name

        sarc.ldap.read_mila_ldap.run(
            local_private_key_file=cfg.ldap.local_private_key_file,
            local_certificate_file=cfg.ldap.local_certificate_file,
            ldap_service_uri=cfg.ldap.ldap_service_uri,

            # write results to here
            output_json_file=tmp_file_path)
        
        sarc.ldap.read_mila_ldap.run(
            # read results from here
            input_json_file=tmp_file_path,
            # write results in database
            mongodb_connection_string=cfg.mongo.connection_string,
            mongodb_database=cfg.mongo.database_name,
            mongodb_collection=cfg.ldap.mongodb_collection_name,
        )

if __name__ == "__main__":
    run()