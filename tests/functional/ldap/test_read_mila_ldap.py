import copy
import json
import tempfile
from datetime import date
from unittest.mock import patch

import pandas as pd
import pytest
from sarc_mocks import fake_raw_ldap_data

import sarc.ldap.read_mila_ldap  # will monkeypatch "query_ldap"
from sarc.config import config


def test_query_to_ldap_server_and_writing_to_output_json(
    patch_return_values, mock_file
):
    cfg = config()
    nbr_users = 10

    patch_return_values(
        {
            "sarc.ldap.read_mila_ldap.query_ldap": fake_raw_ldap_data(nbr_users),
        }
    )

    with tempfile.NamedTemporaryFile() as tmp_file:
        tmp_file_path = tmp_file.name

        with patch("builtins.open", side_effect=mock_file):
            sarc.ldap.read_mila_ldap.run(
                cfg.ldap,
                # write results to here
                output_json_file=tmp_file_path,
            )

        E = json.load(tmp_file)

        # We're going to compare the two, and assume that
        # sarc.ldap.read_mila_ldap.process_user() is correct.
        # This means that we are not testing much.
        assert len(E) == nbr_users
        for e, raw_user in zip(E, fake_raw_ldap_data(nbr_users)):
            processed_user = sarc.ldap.read_mila_ldap.process_user(raw_user)

            # resolve_supervisors is not called here
            e["supervisor"] = None

            assert e == processed_user

        # note that the elements being compared are of the form
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


@pytest.mark.usefixtures("empty_read_write_db")
def test_query_to_ldap_server_and_commit_to_db(patch_return_values, mock_file):
    """
    This test is going to use the database and it will make
    two queries to the LDAP server. The second query will have
    all the same users, plus another batch of new users.
    We will not test special cases such as deleted users
    or problematic cases.
    """

    cfg = config()
    db = cfg.mongo.database_instance

    nbr_users = 10

    L = fake_raw_ldap_data(2 * nbr_users)
    L_first_batch_users = L[0:nbr_users]
    L_second_batch_users = L[nbr_users : 2 * nbr_users]
    del L

    # avoid copy/paste of the same code
    def helper_function(L_users_to_add):
        patch_return_values(
            {
                "sarc.ldap.read_mila_ldap.query_ldap": L_users_to_add,
            }
        )

        with patch("builtins.open", side_effect=mock_file):
            sarc.ldap.read_mila_ldap.run(
                cfg.ldap,
                # write results to here
                mongodb_collection=sarc.ldap.read_mila_ldap.get_ldap_collection(cfg),
            )
        L_users = list(db[cfg.ldap.mongo_collection_name].find({}, {"_id": False}))
        return L_users

    # Remember that the entries coming out of the database are of the form
    # {'mila_ldap':
    #     {'mila_email_username': 'john.smith7@mila.quebec',
    #     'mila_cluster_username': 'smithj7',
    #     'mila_cluster_uid': '1500000008',
    #     'mila_cluster_gid': '1500000008',
    #     'display_name': 'John Smith the 7rd',
    #     'status': 'enabled'}
    # }
    # in anticipation of the fact that there will be other keys besides "mila_ldap"
    # that will be added later.
    #
    # In order to compare the results with `L_first_batch_users` and `L_second_batch_users`,
    # we need to be able to transform the latter slightly in order to match the structure.
    # This involves add the "mila_ldap" wrapper, but also `sarc.ldap.read_mila_ldap.process_user`.

    def transform_user_list(L_u):
        return [{"mila_ldap": sarc.ldap.read_mila_ldap.process_user(u)} for u in L_u]

    sorted_order_func = lambda u: u["mila_ldap"]["mila_email_username"]

    def remove_newkeys(obj):
        obj.pop("record_start", None)
        obj.pop("record_end", None)
        return obj

    # Make query 1. Find users. Add them to database. Then check that database contents is correct.
    L_users = helper_function(L_first_batch_users)
    for u1, u2 in zip(
        sorted(L_users, key=sorted_order_func),
        sorted(transform_user_list(L_first_batch_users), key=sorted_order_func),
    ):
        assert remove_newkeys(u1) == u2

    # Make query 2. Find users. Add them to database. Then check that database contents is correct.
    # Keep in mind that the first batch of users are still there.
    # There is something that changes, though, which is that the first batch of users
    # should now have the status "archived" because they are not in the second batch.
    # That's the LDAP parser's way of saying that when a user is in the database
    # but it's not reported in the most current LDAP query, then it means that
    # it's been "archived". We do that instead of deleting the users.

    # Does the LDAP query and adds the results to the database.
    # Note that L_uA containts users from `L_first_batch_users`
    # with a status of "archived", and users from `L_second_batch_users`
    # with their normal status.
    L_users = helper_function(L_second_batch_users)
    L_uA = sorted(L_users, key=sorted_order_func)

    L1 = transform_user_list(L_first_batch_users)
    L2 = transform_user_list(L_second_batch_users)

    for u in L1:
        u["mila_ldap"]["status"] = "archived"

    L_uB = sorted(
        L1 + L2,
        key=sorted_order_func,
    )

    assert len(L_uA) == len(L_uB)
    for uA, uB in zip(L_uA, L_uB):
        assert remove_newkeys(uA) == uB
