"""
Implements the API as it was defined in
https://mila-iqia.atlassian.net/wiki/spaces/IDT/pages/2190737548/Planification

"""
from sarc.config import config


def get_user(
    mila_email_username=None, mila_cluster_username=None, drac_account_username=None
):
    # note that this `cfg = config()` is cached inside the `config` call
    # so we don't need to caching it here to avoid
    # paying a price each time we call `get_user`
    cfg = config()

    if mila_email_username is not None:
        query = {"mila_ldap.mila_email_username": mila_email_username}
    elif mila_cluster_username is not None:
        query = {"mila_ldap.mila_cluster_username": mila_cluster_username}
    elif drac_account_username is not None:
        query = {
            "$or": [
                {"cc_roles.username": drac_account_username},
                {"cc_members.username": drac_account_username},
            ]
        }
    else:
        raise ValueError("At least one of the arguments must be provided.")

    L = list(
        cfg.mongo.database_instance[cfg.ldap.mongo_collection_name].find(
            query, {"_id": False}
        )
    )
    assert len(L) <= 1
    if len(L) == 1:
        return L[0]
    else:
        return None
