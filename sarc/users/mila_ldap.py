"""
This is a plugin to read user data from the mila LDAP

When it comes to the LDAP entries that we get (and need to process),
they are structured as follows:

::

    {
        "attributes": {
            "apple-generateduid": [
                "AF54098F-29AE-990A-B1AC-F63F5A89B89"
            ],
            "cn": [
                "john.smith",
                "John Smith"
            ],
            "departmentNumber": [],
            "displayName": [
                "John Smith"
            ],
            "employeeNumber": [],
            "employeeType": [],
            "gecos": [
                ""
            ],
            "gidNumber": [
                "1500000001"
            ],
            "givenName": [
                "John"
            ],
            "googleUid": [
                "john.smith"
            ],
            "homeDirectory": [
                "/home/john.smith"
            ],
            "loginShell": [
                "/bin/bash"
            ],
            "mail": [
                "john.smith@mila.quebec"
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
                "smithj"
            ],
            "sn": [
                "Smith"
            ],
            "suspended": [
                "false"
            ],
            "telephoneNumber": [],
            "title": [],
            "uid": [
                "john.smith"
            ],
            "uidNumber": [
                "1500000001"
            ]
        },
        "dn": "uid=john.smith,ou=IDT,ou=STAFF,ou=Users,dc=mila,dc=quebec"
    }

"""

import json
import logging
import os
import ssl
from collections.abc import Iterable
from pathlib import Path

# Requirements
# - pip install ldap3
from attr import dataclass
from ldap3 import ALL_ATTRIBUTES, SUBTREE, Connection, Server, Tls

from sarc.core.models.users import Credentials
from sarc.core.scraping.users import UserMatch, UserScraper, _builtin_scrapers

logger = logging.getLogger(__name__)


@dataclass
class MilaLDAPConfig:
    service_uri: str
    private_key_file: Path
    certificate_file: Path


class MilaLDAPScraper(UserScraper[MilaLDAPConfig]):
    config_type = MilaLDAPConfig

    def get_user_data(self, config: MilaLDAPConfig) -> str:
        return json.dumps(
            _query_ldap(
                config.private_key_file, config.certificate_file, config.service_uri
            )
        )

    def parse_user_data(
        self, _config: MilaLDAPConfig, data: str
    ) -> Iterable[UserMatch]:
        """
        mail[0]        -> mila_email_username  (includes the "@mila.quebec")
        posixUid[0]    -> mila_cluster_username
        uidNumber[0]   -> mila_cluster_uid
        gidNumber[0]   -> mila_cluster_gid
        displayName[0] -> display_name
        suspended[0]   -> status  (as string "enabled" or "disabled")
        """

        for user_raw in json.loads(data):
            creds = Credentials()
            creds.insert(user_raw["posixUid"][0])
            yield UserMatch(
                display_name=user_raw["displayName"][0],
                email=user_raw["mail"][0],
                original_plugin="mila_ldap",
                matching_id=user_raw["mail"][0],
                associated_accounts={"mila": creds},
            )


_builtin_scrapers["mila_ldap"] = MilaLDAPScraper()


def _query_ldap(
    local_private_key_file: Path, local_certificate_file: Path, ldap_service_uri: str
) -> list[dict[str, list[str]]]:
    """
    Since we don't always query the LDAP (i.e. omitted when --input_json_file is given),
    we'll make this a separate function.
    """

    assert os.path.exists(local_private_key_file), (
        f"Missing local_private_key_file {local_private_key_file}."
    )
    assert os.path.exists(local_certificate_file), (
        f"Missing local_certificate_file {local_certificate_file}."
    )

    # Prepare TLS Settings
    tls_conf = Tls(
        local_private_key_file=local_private_key_file,
        local_certificate_file=local_certificate_file,
        validate=ssl.CERT_REQUIRED,
        version=ssl.PROTOCOL_TLSv1_2,
    )
    # Connect to LDAP
    server = Server(ldap_service_uri, use_ssl=True, tls=tls_conf)
    conn = Connection(server)
    conn.open()
    # Extract all the data
    conn.search(
        "dc=mila,dc=quebec",
        "(objectClass=inetOrgPerson)",
        search_scope=SUBTREE,
        attributes=ALL_ATTRIBUTES,
    )
    # We make the decision here to return only the "attributes"
    # and leave out the "dn" field.
    return [json.loads(entry.entry_to_json())["attributes"] for entry in conn.entries]
