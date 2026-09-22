# SARC dev overview documentation

This documentation is a global overview of the SARC codebase, how SARC works globally.
For more precise details, always refer to the code;

Some specific topics are more detailed in the `docs/` folder, this section is only meant to have an overview of the project.

- [Connections diagram](connections_diagram.md)
- [Connection to DB](db_connection.md)
- [Database tables](db_tables.md)
- [Configuration file](config_file.md)
- [Database initialisation](cli_db.md)
- [DRAC user import](drac_user_import.md)
- [Manual user matching](user_matching.md)
- [Merging changed emails](merge_emails.md)
- [Health Monitor](health_monitor.md)
- [Underusage notifications](usage_notifications.md)

<!-- The list above is what GitHub renders; this toctree is what puts the same
     pages in the Sphinx sidebar, and keeps them out of the orphan warnings. -->

```{toctree}
:hidden:
:maxdepth: 1

connections_diagram
db_connection
db_tables
config_file
cli_db
drac_user_import
user_matching
merge_emails
health_monitor
usage_notifications
```
