# procédure import CSV

Après l'ajout de nouveaux utilisateurs à l'allocation DRAC, il faut aller récupérer cette liste depuis le site CCDB pour l'injecter dans SARC.

> [!NOTE]
> Dans notre exemple, nous utiliserons la date du 2026-08-20

- aller cherche le CSV dans [le dashboard ccdb d'un utilisateur ayant les droits sur l'allocation benjioy](https://ccdb.alliancecan.ca/) (rubique "Mes projets/Gérer l'hadhésion au projet" > bouton "télécharger le CSV")
- le placer/renommer en `drac_members.csv`
- utiliser un fichier de configuration tel que celui-ci que nous nommerons `import_drac_csv.yaml`:

```yaml
sarc:
  db:
    host: whocares
    name: whocares
  patches: whocares
  cache: sarc-cache
  users:
    scrapers:
      drac_member:
        csv:
          $include:
            path: drac_members.csv
            format: txt
        csv_date: "2026-08-20"
```

- utiliser ensuite ce fichier pour fetcher (localement) les users, cela créera un fichier de cache incluant le CSV

```bash
SARC_MODE=scraping SARC_CONFIG=import_drac_csv.yaml uv run sarc fetch users
```

_Une entrée de cache est ainsi créée; dans notre cas: `sarc-cache/users/2026/08/20/18:51:07.207`_

- verser ce fichier dans le bucket de cache du projet SARC sur GCP

_dans notre cas, le 2026-08-20, verser le fichier dans [ce répertoire](https://console.cloud.google.com/storage/browser/sarc-cache/users/2026/08/20) ; adapter le répertoire au besoin._

> [!WARNING]
> le fichier de cache doit être généré et uploadé entre deux fetchs automatiques sur GCP, si on veut qu'il soit parsé. S'il est plus vieux que le dernier fichier de cache parsé au moment de son téléversement, il ne sera pas pris en compte par SARC!
