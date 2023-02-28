import os
import tempfile
import json

from unittest.mock import patch

import sarc.ldap.read_mila_ldap # will monkeypatch "query_ldap"
from sarc.config import config




def fake_raw_ldap_data(nbr_users=10):
    """
    Return a deterministically-generated list of fake LDAP users just as
    they would be returned by the function `query_ldap`.
    This is used for mocking the LDAP server.
    """
    return [
    {
        "apple-generateduid": [
            "AF54098F-29AE-990A-B1AC-F63F5A89B89"
        ],
        "cn": [
            f"john.smith{i}",
            f"John Smith{i}"
        ],
        "departmentNumber": [],
        "displayName": [
            f"John Smith the {i}rd"
        ],
        "employeeNumber": [],
        "employeeType": [],
        "gecos": [
            ""
        ],
        "gidNumber": [
            str(1500000001+i)
        ],
        "givenName": [
            "John"
        ],
        "googleUid": [
            f"john.smith{i}"
        ],
        "homeDirectory": [
            f"/home/john.smith{i}"
        ],
        "loginShell": [
            "/bin/bash"
        ],
        "mail": [
            f"john.smith{i}@mila.quebec"
        ],
        "memberOf": [],
        "objectClass": [
            "top",
            "person",
            "organizationalPerson",
            "inetOrgPerson",
            "posixAccount"
        ],
        "physicalDeliveryOfficeName": [],
        "posixUid": [
            f"smithj{i}"
        ],
        "sn": [
            f"Smith {i}"
        ],
        "suspended": [
            "false"
        ],
        "telephoneNumber": [],
        "title": [],
        "uid": [
            f"john.smith{i}"
        ],
        "uidNumber": [
            str(1500000001 + i)
        ]
    } for i in range(nbr_users)]



def test_query_to_ldap_server_and_writing_to_output_json(monkeypatch):

    cfg = config()

    nbr_users = 10
    fake_raw_ldap_data(nbr_users)

    def mock_query_ldap(self, local_private_key_file, local_certificate_file, ldap_service_uri):
        assert os.path.exists(local_private_key_file)
        assert os.path.exists(local_certificate_file)
        assert ldap_service_uri.startswith("ldaps://")
        return fake_raw_ldap_data(nbr_users)

    monkeypatch.setattr(sarc.ldap.read_mila_ldap, "query_ldap", mock_query_ldap)

    with tempfile.NamedTemporaryFile() as tmp_file:
        tmp_file_path = tmp_file.name

        sarc.ldap.read_mila_ldap.run(
            local_private_key_file=cfg.ldap.local_private_key_file,
            local_certificate_file=cfg.ldap.local_certificate_file,
            ldap_service_uri=cfg.ldap.ldap_service_uri,

            # write results to here
            output_json_file=tmp_file_path)

        E = json.load(tmp_file)

        # TODO: Figure out what the format of the output should be.
        # It's been a while. Also, is it possible to read the json
        # contents like that for a NamedTemporaryFile?

        """
        {
            "mila_email_username": "john.smith0@mila.quebec",
            "mila_cluster_username": "john.smith0",
            "mila_cluster_uid": "1500000001",
            "mila_cluster_gid": "1500000001",
            "display_name": "John Smith the 0rd",
            "status": "enabled"
        }
        """