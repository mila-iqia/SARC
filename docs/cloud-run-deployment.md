# Déploiement du dashboard SARC sur Google Cloud Run

Ce document décrit étape par étape comment déployer le dashboard SARC
(`sarc/api/main.py`) sur **Google Cloud Run**, en se branchant sur une base
PostgreSQL hébergée ailleurs (par exemple une VM GCE déjà existante,
accessible publiquement par IP).

Le résultat : une URL publique du type
`https://sarc-dashboard-<n>.<region>.run.app/dash/metrics` qui reste vivante
sans serveur à maintenir activement.

> **Public visé** : la procédure suppose que tu sais utiliser un terminal
> Linux et que tu as déjà du Python. Aucune connaissance préalable de Google
> Cloud n'est requise.

---

## 1. Vue d'ensemble

```
┌──────────────┐   HTTPS   ┌─────────────┐   PG/SSL   ┌──────────────────┐
│   Browser    │─────────▶│  Cloud Run  │──────────▶│  PostgreSQL VM   │
│              │           │ sarc-dash.. │            │  (34.152.3.253)  │
└──────────────┘           └─────────────┘            └──────────────────┘
                                  ▲
                                  │
                          ┌───────┴───────┐
                          │ Secret Manager│ (DB password)
                          └───────────────┘
```

- **Cloud Run** : exécute le container du dashboard. Scale-to-zero (gratuit
  tant que personne ne l'utilise), démarrage automatique au premier hit
  (~5–10 s de cold start).
- **PostgreSQL** : déjà hébergé ailleurs. Doit être accessible depuis
  l'Internet (la VM utilisée accepte `0.0.0.0/0` dans `pg_hba.conf`).
- **Secret Manager** : stocke le password Postgres ; injecté dans le
  container comme variable d'environnement à l'exécution.

---

## 2. Prérequis locaux

### 2.1 Installer le SDK Google Cloud (`gcloud`)

Sur Ubuntu/Debian :

```bash
sudo apt update
sudo apt install apt-transport-https ca-certificates gnupg curl
curl https://packages.cloud.google.com/apt/doc/apt-key.gpg \
    | sudo gpg --dearmor -o /etc/apt/keyrings/cloud.google.gpg
echo "deb [signed-by=/etc/apt/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" \
    | sudo tee /etc/apt/sources.list.d/google-cloud-sdk.list
sudo apt update
sudo apt install google-cloud-cli
gcloud --version   # confirme l'installation
```

### 2.2 Installer le client PostgreSQL

Utile pour tester la connexion à la DB et faire des manipulations :

```bash
sudo apt install postgresql-client
```

Cela installe `psql`, `pg_dump`, etc. Ne déploie **pas** un serveur Postgres
local — aucun conflit avec d'autres installations.

### 2.3 Avoir un compte Google avec accès facturation

Cloud Run est techniquement gratuit dans la limite du *free tier* (2 M
requêtes/mois, 360 K vCPU-secondes/mois), mais GCP exige que tu lies un
**billing account** à ton projet, ce qui demande une carte de crédit.
La facture reste à 0 € tant que tu restes dans le free tier.

---

## 3. Setup initial du projet GCP

### 3.1 Authentification

```bash
gcloud init
```

Cela ouvre un navigateur pour t'authentifier avec ton compte Google, puis
te demande :

- de créer ou choisir un projet GCP
- une région par défaut (choisis `northamerica-northeast1` pour Montréal)

> Si tu sautes la création du projet à ce stade, fais-le ensuite :
> `gcloud projects create sarc-dashboard-XXX --set-as-default`

Vérifie ensuite l'ID du projet actif :

```bash
gcloud config get-value project   # ex: sarc-dashboard-notoraptor
```

Note l'ID retourné, on s'en sert partout.

Authentifie aussi les "Application Default Credentials" (utilisé par les
SDK Python, scripts, etc.) :

```bash
gcloud auth application-default login
```

### 3.2 Activer le billing

C'est obligatoire pour utiliser Cloud Run, **même dans le free tier**.

1. Ouvre <https://console.cloud.google.com/billing>
2. Crée un *billing account* (carte de crédit demandée)
3. Lie le billing account au projet :
   - Sélectionne le projet → **"Link a billing account"**

Vérifier en CLI :

```bash
gcloud beta billing projects describe $(gcloud config get-value project)
# doit afficher: billingEnabled: true
```

### 3.3 Fixer la région par défaut

```bash
gcloud config set compute/region northamerica-northeast1   # Montréal
gcloud config set run/region northamerica-northeast1
```

Choix possibles au Canada :
- `northamerica-northeast1` (Montréal)
- `northamerica-northeast2` (Toronto)

### 3.4 Activer les APIs nécessaires

```bash
gcloud services enable \
    run.googleapis.com \
    cloudbuild.googleapis.com \
    artifactregistry.googleapis.com \
    secretmanager.googleapis.com
```

Cela prend ~30 secondes. À faire une fois par projet.

---

## 4. Préparer le code

Trois fichiers à créer à la racine du repo SARC, et un patch dans
`sarc/config.py`.

### 4.1 `Dockerfile`

```dockerfile
FROM python:3.14-slim

# `iguane` is pulled from a git repo via uv → we need git in the build image.
# `libpq5` is the PostgreSQL client runtime library; psycopg in "python"
# mode (pure-Python wrapper) loads libpq at import time and crashes
# without it.
RUN apt-get update \
    && apt-get install -y --no-install-recommends git libpq5 \
    && rm -rf /var/lib/apt/lists/*

# Install uv (project's dep manager).
RUN pip install --no-cache-dir uv

WORKDIR /app

# Install Python deps first (without the project itself) so this layer is
# cached when only sarc/ changes. README.md is required by hatchling to
# validate the project metadata even when we skip installing the project.
COPY pyproject.toml uv.lock README.md ./
RUN uv sync --frozen --no-dev --no-install-project

# Now add the app code + demo config and install the project itself.
COPY sarc/ ./sarc/
COPY config-cloud-run.yaml ./config-cloud-run.yaml
RUN uv sync --frozen --no-dev

# Scraping mode so db_upgrade() runs at startup: it creates tables, indexes
# (idx_jobstats_job_id, idx_slurm_jobs_submit_time), and the job_series_view.
# With clusters: {} in the config, insert_clusters/insert_rgu are no-ops, so
# this is safe even though the service only reads.
ENV SARC_MODE=scraping \
    SARC_CONFIG=/app/config-cloud-run.yaml

# Cloud Run injects $PORT (defaults to 8080) and expects the app to listen on it.
CMD ["sh", "-c", "uv run uvicorn sarc.api.main:app --host 0.0.0.0 --port ${PORT:-8080}"]
```

### 4.2 `.gcloudignore`

Sans cela, `gcloud run deploy --source .` upload **tout** le répertoire, y
compris des dossiers énormes comme `sarc-cache/` (typiquement plusieurs GB).

```
# Exclude from Cloud Build context (gcloud run deploy --source .).
# Keeps the build fast and avoids leaking secrets.

.git/
.gitignore
.venv/
__pycache__/
*.pyc
.pytest_cache/
.ruff_cache/
.mypy_cache/
.tox/
.cache/
.coverage
.claude/

# Local SARC scraping cache (often huge, GBs).
sarc-cache/

# Local secrets, never upload.
secrets/

# Throwaway data + helper scripts not needed at runtime.
fixes/
tests/
docs/
data/
examples/
scripts/
config/

# Large local dumps.
*.csv
*.json

# Common editor / OS noise.
.idea/
.vscode/
.DS_Store

# Garbage filenames sometimes created by shell mistakes.
dbconfig.txt
Procfile
```

### 4.3 `config-cloud-run.yaml`

C'est la config SARC embarquée dans le container. **Le password n'y est pas**
— il est injecté par Cloud Run via Secret Manager.

```yaml
sarc:
  db:
    host: 34.152.3.253
    name: sarc-demo
    user: postgres
    password: "${env:SARC_DB_PASSWORD}"
    auto_upgrade: true
  clusters: {}
  cache: /tmp/cache
  server:
    auth: null
```

> **Note importante sur la syntaxe** : c'est `${env:VAR}`, pas
> `${envvar:VAR}`. Serieux (le moteur de désérialisation utilisé par
> gifnoc) supporte les resolvers `""`, `env`, `envfile`.

> **Note sur `clusters: {}`** : un dict vide. `insert_clusters()` et
> `insert_rgu()` (appelés par `db_upgrade()`) itèrent dessus → no-op. Pas
> besoin de répliquer la config des clusters dans le container puisque
> celle-ci est utilisée pour la population (script jobs_csv_to_sql.py),
> pas pour la lecture.

### 4.4 Patcher `sarc/config.py` pour supporter user/password/SSL

Le `DbConfig` original ne supporte pas la connection authentifiée. Patch :

```python
# sarc/config.py
@dataclass
class DbConfig:
    host: str
    name: str
    auto_upgrade: bool = True
    user: str | None = None
    password: Secret[str] | None = None
    sslmode: str | None = None

    @cached_property
    def engine(self) -> Engine:
        from urllib.parse import quote

        from sqlmodel import create_engine

        userinfo = ""
        if self.user:
            userinfo = quote(self.user, safe="")
            if self.password:
                userinfo += ":" + quote(str(self.password), safe="")
            userinfo += "@"
        url = f"postgresql+psycopg://{userinfo}{self.host}/{self.name}"

        connect_args: dict[str, str] = {"options": "-c timezone=utc"}
        if self.sslmode:
            connect_args["sslmode"] = self.sslmode

        engine = create_engine(url, connect_args=connect_args)

        if self.auto_upgrade:
            from sarc.db import db_upgrade
            db_upgrade(engine)

        return engine

    def session(self) -> Session:
        return Session(self.engine)
```

---

## 5. Préparer la base Postgres

### 5.1 Connexion test

Depuis ta machine :

```bash
psql "postgresql://postgres@34.152.3.253:5432/postgres"
# Saisis le password Postgres quand demandé
```

### 5.2 Créer la base de démo

Dans la session psql :

```sql
CREATE DATABASE "sarc-demo";
\q
```

> Optionnel mais recommandé pour démo publique : créer un user read-only à
> la place d'utiliser `postgres` :
> ```sql
> CREATE ROLE demo_reader LOGIN PASSWORD 'un-mot-de-passe-fort';
> GRANT CONNECT ON DATABASE "sarc-demo" TO demo_reader;
> \c sarc-demo
> GRANT USAGE ON SCHEMA public TO demo_reader;
> ALTER DEFAULT PRIVILEGES IN SCHEMA public
>     GRANT SELECT ON TABLES TO demo_reader;
> ```
> Puis utilise `demo_reader` dans `config-cloud-run.yaml`. À noter : le user
> doit avoir les droits `CREATE` au premier démarrage pour que
> `db_upgrade()` crée les tables. Soit tu lui donnes temporairement
> (`GRANT ALL PRIVILEGES ON DATABASE "sarc-demo" TO demo_reader`), soit tu
> garde `postgres` jusqu'à la première initialisation puis tu passes à
> read-only.

---

## 6. Stocker les passwords dans Secret Manager

Deux secrets indépendants :
- **`sarc-db-password`** : password Postgres pour la connexion à la DB
- **`dash-password`** : password HTTP Basic Auth pour protéger le dashboard
  et l'API REST (cf. §6 bis ci-dessous)

```bash
# Secret 1 — password Postgres
echo -n "TON_PASSWORD_POSTGRES" | gcloud secrets create sarc-db-password --data-file=-

# Secret 2 — password Basic Auth dashboard (différent du précédent)
echo -n "MOT_DE_PASSE_DASHBOARD_FORT" | gcloud secrets create dash-password --data-file=-

# Donne accès au service account par défaut de Cloud Run aux deux secrets
PROJECT_NUMBER=$(gcloud projects describe $(gcloud config get-value project) --format='value(projectNumber)')
for s in sarc-db-password dash-password; do
    gcloud secrets add-iam-policy-binding "$s" \
        --member="serviceAccount:${PROJECT_NUMBER}-compute@developer.gserviceaccount.com" \
        --role="roles/secretmanager.secretAccessor"
done
```

> `${PROJECT_NUMBER}-compute@developer.gserviceaccount.com` est le service
> account par défaut généré pour le projet ; c'est lui qui exécute le
> container Cloud Run et qui a besoin d'accéder aux secrets.

---

## 6 bis. Protéger l'API derrière un Basic Auth

`sarc/api/auth.py` fournit une dependency `require_basic_auth` activée
quand les env vars `DASH_BASIC_AUTH_USER` et `DASH_BASIC_AUTH_PASSWORD`
sont toutes deux définies. Sans ces vars, l'API est ouverte (compatibilité
dev local / podman).

La dependency est appliquée au niveau **router** dans deux fichiers :

- `sarc/api/metrics.py` :
  ```python
  router = APIRouter(prefix="/dash", dependencies=[Depends(require_basic_auth)])
  ```
- `sarc/api/v0.py` :
  ```python
  router = APIRouter(prefix="/v0", dependencies=[Depends(require_basic_auth)])
  ```

Effet en prod (Cloud Run avec les env vars) :
- Toute requête vers `/dash/*` ou `/v0/*` sans `Authorization: Basic ...`
  → HTTP 401 avec un header `WWW-Authenticate: Basic`
- Le navigateur affiche son dialog natif "Sign in" avec
  username/password
- Une fois saisi, le navigateur cache les creds et les renvoie
  automatiquement sur les requêtes suivantes

**Sécurité** :
- Cloud Run sert toujours en HTTPS, donc le password (encodé en
  Base64 dans l'header) ne passe jamais en clair sur le réseau
- `secrets.compare_digest` est utilisé pour la comparaison
  constant-time (pas de leak par timing attack)

**Limites du Basic Auth** :
- Un seul user/password, partagé. Pour de la vraie auth multi-user, il
  faut OAuth (Cloud IAP, ou easy_oauth dans `server.auth`)
- Pas de logout HTTP-level — pour se "déconnecter", il faut fermer
  toutes les fenêtres du navigateur sur ce domaine
- /docs et /openapi.json (générés par FastAPI au niveau racine) ne sont
  pas protégés par cette mécanique car ils ne sont pas dans un router.
  Suffisant pour démo (la liste des endpoints n'est pas un secret), mais
  à considérer pour de la prod.

---

## 7. Déploiement initial

```bash
gcloud run deploy sarc-dashboard \
    --source . \
    --region northamerica-northeast1 \
    --set-secrets SARC_DB_PASSWORD=sarc-db-password:latest,DASH_BASIC_AUTH_PASSWORD=dash-password:latest \
    --set-env-vars DASH_BASIC_AUTH_USER=demo \
    --allow-unauthenticated \
    --max-instances 1 \
    --memory 1Gi \
    --timeout 300s
```

**Significations** :
- `--source .` : Cloud Build tar le répertoire courant (en respectant
  `.gcloudignore`), build l'image via le `Dockerfile`, la pousse dans
  Artifact Registry, puis déploie sur Cloud Run. Tout en une commande.
- `--set-secrets ...` : déclare deux env vars injectées depuis Secret
  Manager. **Attention** : ce flag *remplace* toute la liste des secrets
  configurés sur le service. Toujours mentionner les deux, sinon le
  secret omis est retiré.
- `--set-env-vars DASH_BASIC_AUTH_USER=demo` : le username en clair (ce
  n'est pas un secret). C'est l'identifiant que tu communiqueras aux
  utilisateurs autorisés, avec le password de `dash-password`.
- `--allow-unauthenticated` : accepte les requêtes anonymes **au niveau
  Cloud Run**. L'auth est gérée dans le code applicatif (Basic Auth).
  Si tu retires ce flag, Cloud Run lui-même refuse tout sauf les
  identités Google IAM autorisées — ça empêche le Basic Auth d'être
  testable par un user externe.
- `--max-instances 1` : limite le scaling automatique à 1 instance.
  Sécurité de coût pour démo (sinon une avalanche de requêtes peut faire
  exploser la facture).
- `--memory 1Gi` : mémoire allouée. Python + SQLAlchemy a besoin d'un peu.
  Au-dessous de 512Mi, l'app peut crasher.
- `--timeout 300s` : durée max d'une requête HTTP avant que Cloud Run la
  tue (default 60s). Les endpoints lents (`/density`, `/jobs`) peuvent
  dépasser sur 1 an de données.

À la fin de la commande, tu vois :
```
Service [sarc-dashboard] revision [sarc-dashboard-00001-xxx] has been deployed
Service URL: https://sarc-dashboard-XXXXX.northamerica-northeast1.run.app
```

Accède au dashboard via `<Service URL>/dash/metrics`.

---

## 8. Peupler la base

Depuis ta machine (la VM hébergeant Postgres est accessible publiquement).
Prépare un fichier de config local `secrets/sarc-dev-distant-sql.yaml`
(à NE PAS commit) :

```yaml
sarc:
  db:
    host: 34.152.3.253
    name: sarc-demo
    user: postgres
    password: "${env:SARC_DB_PASSWORD}"
    auto_upgrade: true
  # Copier la section `clusters: ...` depuis sarc-dev-local-sql.yaml — c'est
  # ce qui permet à insert_clusters / insert_rgu / l'harmonisation GPU de
  # fonctionner.
  clusters:
    mila: ...
    narval: ...
    # etc.
  cache: ../sarc-cache
```

Lance :

```bash
export SARC_DB_PASSWORD="le-password-postgres"
SARC_MODE=scraping \
SARC_CONFIG=secrets/sarc-dev-distant-sql.yaml \
    uv run python fixes/jobs_csv_to_sql.py \
        secrets/metrics-results/raw/<DATE>/jobs-*.csv \
        --gpu-billing secrets/metrics-results/gpu_billing.json \
        --batch-size 40000
```

L'import prend ~20 minutes via Internet pour ~7 M jobs (vs ~10 minutes en
local).

**Pendant que ça tourne, monitorer depuis un autre terminal** :
```bash
watch -n 30 'psql "postgresql://postgres@34.152.3.253:5432/sarc-demo" -c "SELECT count(*) FROM slurm_jobs;"'
```

---

## 9. Opérations courantes

### 9.1 Redéployer après modification du code

Une fois le setup initial fait, tu peux re-déployer aussi souvent que
nécessaire avec la même commande qu'à l'étape 7 :

```bash
gcloud run deploy sarc-dashboard \
    --source . \
    --region northamerica-northeast1 \
    --set-secrets SARC_DB_PASSWORD=sarc-db-password:latest \
    --allow-unauthenticated \
    --max-instances 1 \
    --memory 1Gi \
    --timeout 300s
```

Build typique après modif `sarc/` : ~1–2 min (les deps sont cachées par
Docker, seule la couche du code est rebâtie).

### 9.2 Consulter les logs

**Logs récents** (text) :

```bash
gcloud run services logs read sarc-dashboard \
    --region=northamerica-northeast1 --limit=100
```

**Logs en temps réel** (équivalent `tail -f`) :

```bash
gcloud run services logs tail sarc-dashboard \
    --region=northamerica-northeast1
```

**Console web** (filtres, recherche) :

<https://console.cloud.google.com/run/detail/northamerica-northeast1/sarc-dashboard/logs>

### 9.3 Mettre à jour un secret (rotation password)

Deux secrets distincts à connaître :

**Password Postgres** :
```bash
echo -n "NOUVEAU_PASSWORD_PG" | gcloud secrets versions add sarc-db-password --data-file=-
```

**Password Basic Auth dashboard** :
```bash
echo -n "NOUVEAU_PASSWORD_DASH" | gcloud secrets versions add dash-password --data-file=-
```

Cloud Run pointe vers `:latest` pour les deux, donc le prochain cold start
utilisera la nouvelle valeur automatiquement. Pour forcer l'actualisation
immédiate sans modifier le code :

```bash
gcloud run services update sarc-dashboard \
    --region=northamerica-northeast1 \
    --set-secrets SARC_DB_PASSWORD=sarc-db-password:latest,DASH_BASIC_AUTH_PASSWORD=dash-password:latest
```

> Toujours réécrire les deux mappings dans `--set-secrets`, sinon celui
> omis est retiré du service.

### 9.3 bis Changer l'utilisateur Basic Auth (pas le password)

`DASH_BASIC_AUTH_USER` est en clair via `--set-env-vars`, pas un secret.
Pour le changer :

```bash
gcloud run services update sarc-dashboard \
    --region=northamerica-northeast1 \
    --set-env-vars DASH_BASIC_AUTH_USER=nouveau_user
```

### 9.3 ter Désactiver complètement l'auth (rendre le dashboard public)

Supprime les env vars/secrets liés à Basic Auth :

```bash
gcloud run services update sarc-dashboard \
    --region=northamerica-northeast1 \
    --remove-env-vars DASH_BASIC_AUTH_USER \
    --remove-secrets DASH_BASIC_AUTH_PASSWORD
```

Le code détecte l'absence des deux et passe en mode "no auth" sans
redéploiement de l'image.

### 9.4 Voir la version courante / l'historique des revisions

```bash
gcloud run services describe sarc-dashboard \
    --region=northamerica-northeast1

gcloud run revisions list \
    --service=sarc-dashboard \
    --region=northamerica-northeast1
```

Chaque déploiement crée une nouvelle révision (`sarc-dashboard-00001-xxx`,
`-00002-xxx`, ...). Tu peux router le trafic vers une révision précédente
si une nouvelle version casse :

```bash
gcloud run services update-traffic sarc-dashboard \
    --region=northamerica-northeast1 \
    --to-revisions=sarc-dashboard-00001-xxx=100
```

### 9.5 Mettre le service en pause (réduire à zéro)

Cloud Run scale déjà à zéro automatiquement. Mais pour bloquer toutes les
requêtes :

```bash
# Refuser tout trafic public (l'URL répond 403)
gcloud run services update sarc-dashboard \
    --region=northamerica-northeast1 \
    --no-allow-unauthenticated

# Pour ré-ouvrir
gcloud run services update sarc-dashboard \
    --region=northamerica-northeast1 \
    --allow-unauthenticated
```

### 9.6 Supprimer le service

```bash
gcloud run services delete sarc-dashboard \
    --region=northamerica-northeast1
```

Note : ça ne supprime pas l'image dans Artifact Registry, ni le secret, ni
les builds. Pour nettoyer entièrement :

```bash
gcloud secrets delete sarc-db-password
gcloud artifacts repositories delete cloud-run-source-deploy \
    --location=northamerica-northeast1
```

### 9.7 Tester localement avec la même config

```bash
docker build -t sarc-dashboard-local .
docker run --rm -p 8080:8080 \
    -e SARC_DB_PASSWORD="le-password-pg" \
    -e DASH_BASIC_AUTH_USER="demo" \
    -e DASH_BASIC_AUTH_PASSWORD="le-password-dash" \
    sarc-dashboard-local
# Puis http://localhost:8080/dash/metrics (le navigateur demande user/password)
```

Sans les variables `DASH_BASIC_AUTH_*`, l'app tourne ouverte (utile pour
itérer localement sans avoir à saisir les creds à chaque rechargement).

---

## 10. Troubleshooting (erreurs rencontrées)

### 10.1 "Git executable not found" pendant le build

Cause : `iguane` est une dépendance git mais l'image `python:3.14-slim`
n'a pas `git`.
Fix : déjà appliqué dans le Dockerfile (`apt-get install git`).

### 10.2 "Readme file does not exist: README.md" pendant le build

Cause : hatchling exige `README.md` pour builder le package `sarc`.
Fix : déjà appliqué (`COPY ... README.md ./` avant le premier `uv sync`).

### 10.3 "no pq wrapper available" / "libpq library not found" au démarrage

Cause : psycopg en mode pure-Python charge `libpq` au runtime, absente de
`python:slim`.
Fix : déjà appliqué (`apt-get install libpq5`).

### 10.4 "Cannot resolve 'envvar:...' because the 'envvar' resolver is not defined"

Cause : la bonne syntaxe pour serieux est `${env:VAR}`, pas `${envvar:VAR}`.
Fix : utiliser `${env:SARC_DB_PASSWORD}` dans `config-cloud-run.yaml`.

### 10.5 "Upload sources" très lent (plusieurs minutes / GB)

Cause : `.gcloudignore` ne ferme pas un gros dossier (sarc-cache, données,
.tox, etc.). Cloud Build tar **tout** ce qui n'est pas dans
`.gcloudignore`.
Fix : ajouter les dossiers concernés à `.gcloudignore`.
Pour vérifier la taille uploadée, au début du build tu vois la ligne :
```
Creating temporary tarball archive of N file(s) totalling X.X MB ...
```
Si X est > 100 MB, il y a probablement un dossier à exclure.

### 10.6 Le container démarre mais HTTP 500 sur les endpoints

Probable : erreur de connexion à la DB. Vérifier les logs :
```bash
gcloud run services logs read sarc-dashboard --region=northamerica-northeast1
```

Causes possibles :
- Password incorrect dans le secret (résolu par étape 9.3)
- IP de la VM Postgres pas accessible depuis Cloud Run (vérifier
  `pg_hba.conf` et le firewall GCP de la VM)
- `auto_upgrade: true` + user sans droits `CREATE` → le `db_upgrade()`
  échoue. Donner les droits ou pré-créer les tables.

### 10.7 Le dashboard charge mais montre 0 jobs / 0 clusters

C'est normal si la DB est vide : il faut lancer
`fixes/jobs_csv_to_sql.py` (étape 8).

---

## 11. Coûts et free tier

Cloud Run free tier (mensuel, par compte) :

- 2 000 000 requêtes
- 360 000 vCPU-secondes
- 180 000 GiB-secondes (mémoire)

Pour une démo qui reçoit < 1000 hits/jour, on est largement dans le
free tier. La facture reste à 0 €.

Coûts hors free tier :
- Cloud Build : ~120 minutes gratuites/jour, puis $0.003/build-min
- Artifact Registry : 0.5 GB gratuit, puis $0.10/GB/mois
- Secret Manager : 6 secrets gratuits + 10 K accès/mois, puis $0.06/secret/mois + $0.03/10K accès

**Pour éviter les mauvaises surprises** : `--max-instances 1` est essentiel.
Active aussi un budget alert :

<https://console.cloud.google.com/billing/budgets>

---

## 12. Liens utiles

Tous les liens supposent que tu as ton projet actif (`gcloud config get-value project`).

| Page | URL |
|---|---|
| Console Cloud Run (services) | <https://console.cloud.google.com/run> |
| Détails du service `sarc-dashboard` | <https://console.cloud.google.com/run/detail/northamerica-northeast1/sarc-dashboard/metrics> |
| Logs du service | <https://console.cloud.google.com/run/detail/northamerica-northeast1/sarc-dashboard/logs> |
| Cloud Build (builds en cours / historique) | <https://console.cloud.google.com/cloud-build/builds;region=northamerica-northeast1> |
| Secret Manager (gérer les secrets) | <https://console.cloud.google.com/security/secret-manager> |
| Artifact Registry (images Docker) | <https://console.cloud.google.com/artifacts> |
| Billing | <https://console.cloud.google.com/billing> |
| Budgets et alertes | <https://console.cloud.google.com/billing/budgets> |
| APIs activées | <https://console.cloud.google.com/apis/dashboard> |
| IAM (service accounts) | <https://console.cloud.google.com/iam-admin/iam> |

---

## 13. Reproduire dans un nouveau projet (résumé express)

Une fois ce document compris, voici la séquence minimale pour reproduire
le déploiement dans un nouveau projet GCP :

```bash
# 0. Prérequis : gcloud installé + auth fait + projet actif + billing lié

# 1. Region + APIs
gcloud config set compute/region northamerica-northeast1
gcloud config set run/region northamerica-northeast1
gcloud services enable run.googleapis.com cloudbuild.googleapis.com \
    artifactregistry.googleapis.com secretmanager.googleapis.com

# 2. Secrets (password Postgres + password Basic Auth dashboard)
echo -n "PASSWORD_PG"   | gcloud secrets create sarc-db-password --data-file=-
echo -n "PASSWORD_DASH" | gcloud secrets create dash-password    --data-file=-
PROJECT_NUMBER=$(gcloud projects describe $(gcloud config get-value project) --format='value(projectNumber)')
for s in sarc-db-password dash-password; do
    gcloud secrets add-iam-policy-binding "$s" \
        --member="serviceAccount:${PROJECT_NUMBER}-compute@developer.gserviceaccount.com" \
        --role="roles/secretmanager.secretAccessor"
done

# 3. Adapter config-cloud-run.yaml (host, name, user)
# 4. Build + deploy
gcloud run deploy sarc-dashboard \
    --source . \
    --region northamerica-northeast1 \
    --set-secrets SARC_DB_PASSWORD=sarc-db-password:latest,DASH_BASIC_AUTH_PASSWORD=dash-password:latest \
    --set-env-vars DASH_BASIC_AUTH_USER=demo \
    --allow-unauthenticated \
    --max-instances 1 \
    --memory 1Gi \
    --timeout 300s

# 5. Peupler la DB (depuis la machine locale)
export SARC_DB_PASSWORD="..."
SARC_MODE=scraping SARC_CONFIG=secrets/sarc-dev-distant-sql.yaml \
    uv run python fixes/jobs_csv_to_sql.py \
        secrets/metrics-results/raw/.../jobs-*.csv \
        --gpu-billing secrets/metrics-results/gpu_billing.json
```

Total : ~30 minutes pour un setup à zéro, ~3 minutes pour un redéploiement.
