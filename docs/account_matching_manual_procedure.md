# Account Matching procedure

## In a nutshell

The manual procedure steps are:

**Once a month** (or each time we have new users files from DRAC):

- Copy the two DRAC files (`users` and `roles`) in the `secrets/account_matching` folder
- Adjust the `account_matching` section of the config file to point to these two files 
- run the `acquire users` command
- if any errors, adjust the exceptions in the `secrets/make_matches_config.json` file, and re-run the account matching script.

## In details

This procedure consists in making matches between the DRAC user account and the Mila acocunts.

- Mila accounts are gathered directly by connecting to Mila LDAP. 
- On the other hand, DRAC accounts must be provided manually.

After that, the `users` collection of mongoDB contains the aggregated users database.

### Access rights

The operator executing the account matching procedure must have wite access to the DRAC folder.

Two possible scenarios : 

- have write access to the running SARC server (production server)
- use SARC from a local machine, with a SSH connection to the production server (see below). **This is the prefered method.**

#### Remote access to MongoDB (via SSH tunneling) 

##### SSH config
Refer to `remote_mongo_access.md` for ssh connection with port redirection, to connect to mongoDB form the local machine.

##### SARC config file
To use the remote mongoDB connection, tunneled from localhost:27018, the `mongo` section in the config file like this:

```
    "mongo": {
        "connection_string": "127.0.0.1:27018",
        "database_name": "sarc"
    },
```

### data source 1: Mila LDAP credentials

The credentials for the Mila LDAP are in the `secrets/ldap` folder.

They are refered to in the ldap section of the sarc config file :
```
    "ldap": {
        "local_private_key_file": "secrets/ldap/Google_2026_01_26_66827.key",
        "local_certificate_file": "secrets/ldap/Google_2026_01_26_66827.crt",
        "ldap_service_uri": "ldaps://ldap.google.com",
        "mongo_collection_name": "users"
    },

```

### data source 2: DRAC account files

Compute Canada must provide 2 CSV files:
- One "members" file
- One "roles" file 

#### copy the files in the right directory

The two file must be copied to the `secrets/account_matching/` folder of SARC, on the server or the local machine, depending on the scenario. 

#### Configuration file



### Exceptions handling

The exception are manually handled in the `secrets/make_matches_config.json` file.

```
{
    "L_phantom_mila_emails_to_ignore":
        [
            "ignoreme@mila.quebec",
            "idontexistanymore@mila.quebec"
        ],
    "D_override_matches_mila_to_cc_account_username":
        {
            "john_doe@mila.quebec": "jdoe01",
            "janedoe@mila.quebec": "unguessableusername"
        }
}
```
The `L_phantom_mila_emails_to_ignore` list contains the Mila emails present in the DRAC users listings that do not exist in the Mila LDAP.

The `D_override_matches_mila_to_cc_account_username` dictionnary is used to bypass the automatic matching algorythm, when no link can be made with the name or the email address.

The procedure is:
- run the matching script
- if there are mathcing errors, modify `make_matches_config.json` accordingly and re-run the matching script.

### Run the matching script

From the SARC folder:
```
$ SARC_CONFIG=<path_to_config_file> poetry run sarc acquire users
```
