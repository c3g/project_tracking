# Tracking the C3G projects
[![Test suite](https://github.com/c3g/project_tracking/actions/workflows/PyTest.yml/badge.svg?branch=main)](https://github.com/c3g/project_tracking/actions/workflows/PyTest.yml) on main branch
[![Test suite](https://github.com/c3g/project_tracking/actions/workflows/PyTest.yml/badge.svg?branch=dev)](https://github.com/c3g/project_tracking/actions/workflows/PyTest.yml) on dev branch

This is an API to access and modify the C3G data processing tracking database.

## Install
We recommend using PostgreSQL in production. SQLite works for local development, except for
regex filters (see [Query filtering syntax](#query-filtering-syntax)).
We also publish containers on the GitHub registry (ghcr.io) and test our system using podman.

Python 3.11 or newer is required. The default development version is 3.14 (`.python-version`);
CI tests 3.11 to 3.14 and the container image is based on Fedora 44 (Python 3.14).

### From GitHub with sqlite (best for developer):
Sqlite needs to be installed on your machine.
Here, you will deploy a development instance of the app and be able to modify the code in the repo with auto-reload 
```bash
git clone git@github.com:c3g/project_tracking.git
cd project_tracking
git checkout dev # If you are developing from the dev branch!
python3.14 -m venv .venv
source ./.venv/bin/activate
pip install --upgrade pip
pip install -e .
# Setting the db url is optional, the default will be in the app installation folder
export C3G_SQLALCHEMY_DATABASE_URI="sqlite:////tmp/my_test_db.sql"
# initialise the db (run flask commands from the repo root: they read alembic.ini)
flask --app project_tracking init-db
# run the app
flask --app project_tracking --debug run
```

By default, flask will run the server on http://127.0.0.1:5000. You can test that everything 
is fine with curl in a terminal:
```bash
$ curl http://127.0.0.1:5000/
Welcome to the TechDev tracking API!
# The help api is also available. It lists all the server urls.
$ curl http://127.0.0.1:5000/help
----------
URL:
        /
DOC:
        Welcome page

[...]
```



`init-db` only creates tables on an empty database (see [init-db](#init-db-and---flush-in-multi-database-setups)).
To wipe a database and recreate it from scratch, use `--flush`:

```bash
# WARNING this will erase all entry to you Database!
flask --app project_tracking init-db --flush --db-uri "sqlite:////tmp/my_test_db.sql"
```

### Using podman and sqlite:
We have a [ghcr.io repo for the project](https://github.com/c3g/project_tracking/pkgs/container/project_tracking)
There are version releases and `latest` tag relates to the latest release.
```bash
SQLITE_DB_FOLDER=<folder on host with WR access>
podman run -v $SQLITE_DB_FOLDER:/sqlite:Z -it --rm -p 8000:8000 -e C3G_INIT_DB=1 ghcr.io/c3g/project_tracking:latest
```
The app runs on port 8000 inside the container, the `-e C3G_INIT_DB=1` option will create the db in
`$SQLITE_DB_FOLDER/tracking_db.sql` (and leave it untouched on later starts if it already exists).



### From GitHub using PostgreSQL:
PostgreSQL needs to be installed with a database named <DB_NAME>,
accessible by user <POSTGRES_USER>, with a password <POSTGRES_PW>

```bash
git clone git@github.com:c3g/project_tracking.git
cd project_tracking
python3.14 -m venv .venv
source ./.venv/bin/activate
pip install .[postgres]
export C3G_SQLALCHEMY_DATABASE_URI="postgresql+psycopg2://<POSTGRES_USER>:<POSTGRES_PW>@<POSTGRES_HOST>/<DB_NAME>?client_encoding=utf8"
flask --app project_tracking init-db
gunicorn -w 4 'project_tracking:create_app()'
```

### Using podman and PostgreSQL:
Here we expect postgres to be listening to the localhost (127.0.0.1) interface. 
The podman option `--network slirp4netns:allow_host_loopback=true` 
options makes it so that the host `127.0.0.1` interface is
reachable with the `10.0.2.2` adress inside the container. That is why the C3G_SQLALCHEMY_DATABASE_URI
is set to that value.
```bash
podman pull ghcr.io/c3g/project_tracking:latest
export C3G_SQLALCHEMY_DATABASE_URI="postgresql+psycopg2://<POSTGRES_USER>:<POSTGRES_PW>@10.0.2.2/<POSTGRES_DB_NAME>?client_encoding=utf8"
podman secret create --env C3G_SQLALCHEMY_DATABASE_URI C3G_SQLALCHEMY_DATABASE_URI
podman run --secret C3G_SQLALCHEMY_DATABASE_URI,type=env -p 8000:8000 -e C3G_INIT_DB=1 --network slirp4netns:allow_host_loopback=true ghcr.io/c3g/project_tracking:latest
```

## Install from pypi:
```
No release yet
```

## Configuration (single DB and multi DB)

The app reads config in this order:
1. built-in defaults from `create_app()`
2. environment variables with `C3G_` prefix
3. `instance/config.py`

Because `instance/config.py` is loaded last, values in that file override `C3G_` env vars.

**Role of `SQLALCHEMY_DATABASE_URI` in multi-db mode:**
`SQLALCHEMY_DATABASE_URI` is a required base URI — it is always present and is used in single-db mode (e.g. local dev with SQLite) where all projects share one database. In a multi-db production deployment, every project should have an explicit entry in `PROJECT_DATABASES`. If a project name has no matching entry, the request silently falls through to `SQLALCHEMY_DATABASE_URI` as a catch-all, which is not the intended behavior in production. Do not rely on this fallback: always map every project explicitly.

### Local development

`instance/` is already ignored by git (`.gitignore` contains `instance/`), so local config can safely live there.

Create `instance/config.py`:

```python
SQLALCHEMY_DATABASE_URI = "postgresql+psycopg2://<USER>:<PW>@<HOST>/<DEFAULT_DB>?client_encoding=utf8"

# PostgreSQL identifiers are easiest to operate if you keep DB/user names in
# lowercase snake_case (avoids quoting surprises).
# Each DB has its own dedicated role, named to match: project_tracking_<name>
# (see "Adding a new database and rotating secrets" below). PROJ-B and PROJ-C
# below share one DB/role (project_tracking_grp) on purpose, to illustrate
# that routing is per project name, not necessarily one DB per project.
# Routing map used by the API:
# - exact project-name match only (case-insensitive)
PROJECT_DATABASES = {
        "PROJ-A": "postgresql+psycopg2://project_tracking_prja:<PW>@<HOST>/project_tracking_prja?client_encoding=utf8",
        "PROJ-B": "postgresql+psycopg2://project_tracking_grp:<PW>@<HOST>/project_tracking_grp?client_encoding=utf8",
        "PROJ-C": "postgresql+psycopg2://project_tracking_grp:<PW>@<HOST>/project_tracking_grp?client_encoding=utf8",
}
```

You can also use env vars only:

```bash
export C3G_SQLALCHEMY_DATABASE_URI="postgresql+psycopg2://<USER>:<PW>@<HOST>/<DEFAULT_DB>?client_encoding=utf8"
export C3G_PROJECT_DATABASES='{"PROJ-A":"postgresql+psycopg2://project_tracking_prja:<PW>@<HOST>/project_tracking_prja?client_encoding=utf8","PROJ-B":"postgresql+psycopg2://project_tracking_grp:<PW>@<HOST>/project_tracking_grp?client_encoding=utf8","PROJ-C":"postgresql+psycopg2://project_tracking_grp:<PW>@<HOST>/project_tracking_grp?client_encoding=utf8"}'
```

### init-db and --flush in multi-database setups

`init-db` always targets one database URI at a time.

What `init-db` does to a database:
- **Empty database:** creates every table from the models and stamps it at the latest
  Alembic revision (`head`), so later `upgrade-all-dbs` runs have nothing to replay on it.
- **Database that already has tables:** left completely untouched (no new tables, no stamp) and a
  message says so. Apply pending schema changes with `upgrade-all-dbs`. This makes `init-db` /
  `init-all-dbs` safe to run on every start.
- **With `--flush`:** drops the tables, recreates them and stamps `head`. **All data is lost.**

Commands:
1. `flask --app project_tracking init-db` initializes only `SQLALCHEMY_DATABASE_URI` (base URI).
2. `flask --app project_tracking init-db --db-uri <URI>` initializes only that explicit DB.
3. Adding `--flush` drops and recreates tables only on the targeted DB URI.
4. `flask --app project_tracking init-all-dbs` initializes base URI + all DB URIs from `PROJECT_DATABASES`
   (databases that already have tables are skipped).
5. `flask --app project_tracking init-all-dbs --flush --yes-i-really-mean-flush` flushes and initializes all configured DBs.

`init-all-dbs --flush` is intentionally guarded and requires `--yes-i-really-mean-flush`.

`init-db`, `init-all-dbs` and `upgrade-all-dbs` read `alembic.ini`: run them from the repo root
(the container already does).

**Why a new database must go through `init-db` and not `upgrade-all-dbs`:** the migration history
starts from a legacy schema (its first revision renames an old `patient` table), so it cannot build
a database from scratch. Migrations are only for databases that already exist.

Example:

```bash
flask --app project_tracking init-db --db-uri "postgresql+psycopg2://USER:PW@DBHOST/default_db?client_encoding=utf8"
flask --app project_tracking init-db --db-uri "postgresql+psycopg2://project_tracking_prja:PW@DBHOST/project_tracking_prja?client_encoding=utf8"
flask --app project_tracking init-db --db-uri "postgresql+psycopg2://project_tracking_grp:PW@DBHOST/project_tracking_grp?client_encoding=utf8"
flask --app project_tracking init-all-dbs
```

Use `--flush` only with an explicit `--db-uri` to avoid wiping the wrong database.

### Creating projects through the Admin API

Admin endpoint:

```http
POST /admin/create_project/<project_name>
```

Optional metadata must be provided in the POST JSON body:

- `ext_id` (integer)
- `ext_src` (string)

Important behavior with multi-database routing:
1. Project-to-database routing is exact-name only and case-insensitive.
2. The API stores project names exactly as provided in the route argument.
3. If a project name has no entry in `PROJECT_DATABASES`, the request falls through to `SQLALCHEMY_DATABASE_URI` as a catch-all. In a properly configured deployment, every project should be explicitly mapped — do not rely on this fallback.

Recommended workflow when adding a new project:
1. Decide which DB should own that project.
2. Add or update the exact project-name key in `PROJECT_DATABASES` (env/secret/config).
3. Create the schema on any new, empty database, then bring every database to the latest revision:

```bash
flask --app project_tracking init-all-dbs     # creates + stamps empty DBs, skips the others
flask --app project_tracking upgrade-all-dbs  # applies pending migrations everywhere
```

4. Restart/redeploy so the API reads updated config.
5. Create the project row with the admin endpoint.

Example:

```bash
curl -X POST "http://localhost:8000/admin/create_project/PROJ-B"

curl -X POST "http://localhost:8000/admin/create_project/PROJ-B" \
        -H "Content-Type: application/json" \
        -d '{"ext_id":123,"ext_src":"lims"}'
```

If `PROJ-B` is mapped to `project_tracking_grp` in `PROJECT_DATABASES`, it is created in `project_tracking_grp`.
If `PROJ-B` has no `PROJECT_DATABASES` entry, the request falls through to `SQLALCHEMY_DATABASE_URI`. Always add the mapping before creating a project.

### Project routing policy (name-based on public routes)

Public `/project/...` routes require a **project name** in the URL path.
Numeric project IDs are intentionally rejected on public routes to avoid
ambiguous cross-database routing.

Example (supported):

```http
GET /project/PROJ-A/
```

Example (rejected with 400):

```http
GET /project/123/
```

If you need numeric ID lookup, use the admin-only explicit route with a required
database selector:

```http
GET /admin/projects/by-id/<project_id>?database=<db_label_or_uri>
```

The database label is the database name from its URI (e.g. `project_tracking_prja`).

To list the projects of every configured database (each row gets a `_database` field):

```http
GET /admin/projects/across-databases
```

The project in the URL path selects the database. GET routes also accept a `project_name` filter
to query several projects **stored in that same database** at once:

```http
GET /project/PROJ-B/samples?json={"project_name": "PROJ-B,PROJ-C"}
```

Database errors come back in the usual envelope:
- **503** when the database cannot be reached (connection refused, authentication failure, missing
  database or role), with a hint on which database the project resolved to.
- **500** for other database errors (e.g. a lock or statement timeout), with the database message.

## Production deployment with Podman secrets

Use environment/secret injection from your orchestrator (podman secrets, Kubernetes secrets, etc.) and keep DB credentials out of the repository.

Recommended pattern:
1. Set `C3G_SQLALCHEMY_DATABASE_URI` to the base URI. Every project must also have an explicit entry in `PROJECT_DATABASES` — do not rely on the catch-all fallback in production.
2. Set `C3G_PROJECT_DATABASES` (JSON) with all routing entries.
3. Run schema upgrades on all configured DBs during deployment:

```bash
flask --app project_tracking upgrade-all-dbs
```

This keeps one migration history while ensuring every database is upgraded to the same revision.

For secret-file based deployments, the app also supports `C3G_*_FILE` variables.
When `C3G_SQLALCHEMY_DATABASE_URI_FILE` is set, the app reads the secret file and uses its value,
**even if `C3G_SQLALCHEMY_DATABASE_URI` is also set** (the image sets a SQLite default for it).
Same behavior for `C3G_PROJECT_DATABASES_FILE`. The app, the container entrypoint, `podman exec`
commands and one-off containers therefore all use the same databases.

Example with podman secrets:

```bash
podman secret create C3G_SQLALCHEMY_DATABASE_URI /path/to/C3G_SQLALCHEMY_DATABASE_URI.txt
podman secret create C3G_PROJECT_DATABASES /path/to/C3G_PROJECT_DATABASES.json

podman run \
        --secret C3G_SQLALCHEMY_DATABASE_URI,type=mount \
        --secret C3G_PROJECT_DATABASES,type=mount \
        -e C3G_SQLALCHEMY_DATABASE_URI_FILE=/run/secrets/C3G_SQLALCHEMY_DATABASE_URI \
        -e C3G_PROJECT_DATABASES_FILE=/run/secrets/C3G_PROJECT_DATABASES \
        -e C3G_ALEMBIC_UPGRADE=1 \
        -p 8000:8000 ghcr.io/c3g/project_tracking:latest
```

### One-time VM setup

Create a secure directory on the VM to hold the secret source files.
These never enter the container directly; Podman reads them when creating secrets.

```bash
sudo mkdir -p /etc/project_tracking/secrets
sudo chmod 700 /etc/project_tracking/secrets
```

Create the secret files (replace placeholders with real values):

```bash
# Single-line plain text
sudo tee /etc/project_tracking/secrets/C3G_SQLALCHEMY_DATABASE_URI > /dev/null <<'EOF'
postgresql+psycopg2://USER:PW@DBHOST/default_db?client_encoding=utf8
EOF

# Compact single-line JSON
sudo tee /etc/project_tracking/secrets/C3G_PROJECT_DATABASES > /dev/null <<'EOF'
{"PROJ-A":"postgresql+psycopg2://project_tracking_prja:PW@DBHOST/project_tracking_prja?client_encoding=utf8","PROJ-B":"postgresql+psycopg2://project_tracking_grp:PW@DBHOST/project_tracking_grp?client_encoding=utf8","PROJ-C":"postgresql+psycopg2://project_tracking_grp:PW@DBHOST/project_tracking_grp?client_encoding=utf8"}
EOF

sudo chmod 600 /etc/project_tracking/secrets/C3G_*
```

Load the files into Podman's secret store (**run as the same user that owns the systemd unit**):

```bash
podman secret create C3G_SQLALCHEMY_DATABASE_URI /etc/project_tracking/secrets/C3G_SQLALCHEMY_DATABASE_URI
podman secret create C3G_PROJECT_DATABASES /etc/project_tracking/secrets/C3G_PROJECT_DATABASES
```

Update the `ExecStart` block of your systemd unit file to mount secrets and point the app at them via `*_FILE` env vars:

```ini
ExecStart=/usr/bin/podman run \
        --cidfile=%t/%n.ctr-id \
        --cgroups=no-conmon \
        --rm \
        --sdnotify=conmon \
        -d \
        --replace \
        --name project_tracking-api \
        --label io.containers.autoupdate=registry \
        --secret C3G_SQLALCHEMY_DATABASE_URI,type=mount \
        --secret C3G_PROJECT_DATABASES,type=mount \
        -e C3G_SQLALCHEMY_DATABASE_URI_FILE=/run/secrets/C3G_SQLALCHEMY_DATABASE_URI \
        -e C3G_PROJECT_DATABASES_FILE=/run/secrets/C3G_PROJECT_DATABASES \
        --network slirp4netns:allow_host_loopback=true \
        -e C3G_ALEMBIC_UPGRADE=1 \
        -p 8000:8000 ghcr.io/c3g/project_tracking:latest -w 1 -t 180
```

Reload and start:

```bash
systemctl --user daemon-reload
systemctl --user restart container-project_tracking-api.service
systemctl --user status container-project_tracking-api.service
```

---

### Adding a new database and rotating secrets

#### 1 — Create the PostgreSQL database and user

Generate a strong random password first (pick one method):

```bash
# Option A: OpenSSL
openssl rand -base64 36

# Option B: Python
python -c 'import secrets; print(secrets.token_urlsafe(36))'
```

Store this value in your secret manager/file and use it when creating the DB user.

Connect to PostgreSQL as a superuser and run:

```sql
-- Create a dedicated user PER DATABASE, not one shared user for all DBs.
-- Each project's DB gets its own role/credential so a leaked or compromised
-- credential is scoped to that one project, not every project's data.
--
-- Use the SAME name, prefixed project_tracking_<name>, for both the role
-- and the database it owns. This makes every project_tracking DB/role
-- instantly identifiable and greppable on a shared Postgres instance
-- (`\l`, `\du`, pg_stat_activity, or `pg_database.datname LIKE
-- 'project_tracking_%'` for backup/monitoring/cross-DB tooling), without
-- needing to look anything up.
CREATE USER project_tracking_newdb WITH PASSWORD 'strong_random_password';

-- Create the new database, owned by that dedicated role
CREATE DATABASE project_tracking_newdb OWNER project_tracking_newdb ENCODING 'UTF8';

-- Connect to the new database and grant minimum required permissions
\c project_tracking_newdb
GRANT CONNECT ON DATABASE project_tracking_newdb TO project_tracking_newdb;
GRANT USAGE ON SCHEMA public TO project_tracking_newdb;
-- After first migration these broader grants are needed:
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO project_tracking_newdb;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT USAGE, SELECT ON SEQUENCES TO project_tracking_newdb;
```

#### 2 — Create the schema on the new database

Run this before restarting the API so the new DB has the correct schema. Migrations cannot build an
empty database, so use `init-db`: it creates the tables and stamps the database at the latest revision.

```bash
# From the project repo root with virtualenv active
flask --app project_tracking init-db --db-uri "postgresql+psycopg2://project_tracking_newdb:PW@DBHOST/project_tracking_newdb?client_encoding=utf8"
```

Equivalent once the new URI is in `PROJECT_DATABASES` (existing databases are skipped):

```bash
flask --app project_tracking init-all-dbs
```

#### 3 — Update secret files on the VM

Edit the JSON secret file to add the new mapping:

```bash
sudo vi /etc/project_tracking/secrets/C3G_PROJECT_DATABASES
# Add the new entry, e.g.:
# {"PROJ-A":"...","PROJ-B":"...","PROJ-C":"...","NEWPROJECT":"postgresql+psycopg2://project_tracking_newdb:PW@DBHOST/project_tracking_newdb?client_encoding=utf8"}
```

#### 4 — Rotate Podman secrets

Podman secrets are immutable; to update one you delete and recreate it:

```bash
podman secret rm C3G_PROJECT_DATABASES
podman secret create C3G_PROJECT_DATABASES /etc/project_tracking/secrets/C3G_PROJECT_DATABASES
```

#### 5 — Restart the service

```bash
systemctl --user restart container-project_tracking-api.service
systemctl --user status container-project_tracking-api.service
```

The new routing entry is now live. 
Any request for project name `NEWPROJECT` will be routed to `project_tracking_newdb`.

---

### Production maintenance operations (runbook)

Use this table to decide which operation to run in production.

1. Normal deploy or restart: use migrations only.
2. Brand new empty DB (tables do not exist): initialize once with `init-db`/`init-all-dbs` (creates
   and stamps it), then migrate.
3. Existing DB that already has tables/data: never run `--flush`.

Recommended defaults in the systemd unit:
1. Keep `C3G_ALEMBIC_UPGRADE=1` enabled.
2. Keep `C3G_INIT_DB` unset. Leaving it set is harmless (an existing DB is left untouched), but it
   only covers the default URI, not the databases in `PROJECT_DATABASES`.

**The container refuses to start on a failed migration.** If reading a secret, `init-db` or
`upgrade-all-dbs` fails at startup, the container exits with an error instead of serving the API on a
database with the wrong schema. `systemctl --user status` shows the failure; the cause is in
`journalctl --user -u container-project_tracking-api.service`. Database URIs are logged by label only
(e.g. `project_tracking_prja`), never with credentials.

#### Running maintenance commands in production

The image contains everything needed (Flask commands, Alembic, `alembic.ini`, migrations), and the
`*_FILE` secrets take precedence over the image's SQLite default, so commands see the same databases
as the service. Two ways to run them:

```bash
# In the running container (read-only checks, quick operations)
podman exec project_tracking-api flask --app project_tracking db-revisions

# In a one-off container with the same secrets: does not depend on the live container, works even
# when it is stopped or fails to start. Use it for anything that changes a schema or a revision.
PT_SECRETS=(--network slirp4netns:allow_host_loopback=true
  --secret C3G_SQLALCHEMY_DATABASE_URI,type=mount --secret C3G_PROJECT_DATABASES,type=mount
  -e C3G_SQLALCHEMY_DATABASE_URI_FILE=/run/secrets/C3G_SQLALCHEMY_DATABASE_URI
  -e C3G_PROJECT_DATABASES_FILE=/run/secrets/C3G_PROJECT_DATABASES)
podman run --rm "${PT_SECRETS[@]}" --entrypoint flask ghcr.io/c3g/project_tracking:latest \
  --app project_tracking db-revisions
```

Use the same image tag as the running service, so the migrations match the deployed code.

`flask db-revisions` lists every configured database with its Alembic revision and state
(`up to date`, `BEHIND`, `NOT STAMPED` or `UNREACHABLE`), and exits with an error if any database is not
at the latest revision.

#### Use case A — Regular restart/deploy

No special action. Just restart:

```bash
systemctl --user restart container-project_tracking-api.service
```

At startup the container runs `upgrade-all-dbs` and migrates all configured DBs. If the service does
not come back, run `db-revisions` in a one-off container to see which database is the problem
(often a database that was never stamped, see use case F).

#### Use case B — You added a new database URI to PROJECT_DATABASES

A new database is empty, and migrations cannot create a schema from scratch: initialize it first
(see [step 2 above](#2--create-the-schema-on-the-new-database)). `C3G_INIT_DB` does not help here,
it only targets the default URI.

Preferred sequence:

```bash
# 1) Update C3G_PROJECT_DATABASES secret and recreate the podman secret
podman secret rm C3G_PROJECT_DATABASES
podman secret create C3G_PROJECT_DATABASES /etc/project_tracking/secrets/C3G_PROJECT_DATABASES

# 2) Create the schema on the new database (existing databases are skipped)
podman run --rm "${PT_SECRETS[@]}" --entrypoint flask ghcr.io/c3g/project_tracking:latest \
  --app project_tracking init-all-dbs

# 3) Restart service (startup runs upgrade-all-dbs, a no-op on the freshly stamped DB)
systemctl --user restart container-project_tracking-api.service
```

#### Use case C — Brand new empty default DB and you want explicit init first

Run `C3G_INIT_DB` as a one-off, then remove it:

```bash
systemctl --user set-environment C3G_INIT_DB=1
systemctl --user restart container-project_tracking-api.service
systemctl --user unset-environment C3G_INIT_DB
```

This initializes only the default DB URI used by `init-db`, and only if it is empty.

#### Use case D — Initialize all configured DBs explicitly

Use a one-off container (or `flask --app project_tracking init-all-dbs` from the repo root):

```bash
podman run --rm "${PT_SECRETS[@]}" --entrypoint flask ghcr.io/c3g/project_tracking:latest \
  --app project_tracking init-all-dbs
```

Destructive reset of all configured DBs is possible but guarded:

```bash
flask --app project_tracking init-all-dbs --flush --yes-i-really-mean-flush
```

Only use the flush form in disposable/test environments.

#### Use case E — Create a new project row after mapping is added

After DB mapping is in place and service restarted, create the project:

```bash
curl -X POST "http://localhost:8000/admin/create_project/NEWPROJECT"
```

Because routing is exact-name based, `NEWPROJECT` must exist as an exact key in `PROJECT_DATABASES`.

#### Use case F — Check that every database is tracked by Alembic

`upgrade-all-dbs` needs each database to record its current revision. Databases created by older
versions of `init-db`, which did not stamp them, have no revision recorded, and an upgrade would
then replay the whole history and fail, and the container would not start. Check all databases:

```bash
podman run --rm "${PT_SECRETS[@]}" --entrypoint flask ghcr.io/c3g/project_tracking:latest \
  --app project_tracking db-revisions
```

For each database reported `NOT STAMPED`, record once the revision matching its schema. Stamping is
deliberately manual: it tells Alembic which migrations the database already has, so a wrong revision
makes migrations be skipped or replayed. The URI is read from the mounted secret, so no password is
typed or kept in the shell history:

```bash
# Database of project PROJ-A (key in C3G_PROJECT_DATABASES)
podman run --rm "${PT_SECRETS[@]}" --entrypoint sh ghcr.io/c3g/project_tracking:latest -c \
  'URI=$(python3 -c "import json,sys; print(json.load(open(sys.argv[1]))[sys.argv[2]])" /run/secrets/C3G_PROJECT_DATABASES PROJ-A) && alembic -x db_url="$URI" stamp <revision>'

# Default database
podman run --rm "${PT_SECRETS[@]}" --entrypoint sh ghcr.io/c3g/project_tracking:latest -c \
  'alembic -x db_url="$(cat /run/secrets/C3G_SQLALCHEMY_DATABASE_URI)" stamp <revision>'
```

For a database created from the models at revision `64a560017524` (before the `sample_match` and
`oncotree_code` migrations), stamp `64a560017524`; the next restart then applies the pending
migrations. Run `db-revisions` again to confirm.

---

## Sample metadata at ingestion

Two optional keys can be set on each sample of a `run_processing` JSON.

### `sample_matched_sample_name`: samples analysed together

A list of sample names this sample must be analysed with, e.g. a tumour and the patient's normal(s):

```json
{"sample_name": "MoHQ-CM-201-70-M10-1DT", "sample_tumour": true,
 "sample_matched_sample_name": ["MoHQ-CM-201-70-M10-1DN", "MoHQ-CM-201-70-M10-2DN"], "readset": [...]}
```

- A match works in both directions: declaring it on one sample is enough.
- Samples of the same payload are linked whatever their order. A named sample that does not exist
  yet only gives a warning; the link is made when that sample is ingested and names this one back.
  **Fill the key on both tumours and normals** so later runs still create the link.
- Links are stored by sample id, so renaming a sample keeps them. Curating a sample removes its links.

`digest_pair_file` uses these links: one `Specimen,Sample_N,Sample_T` row per tumour/normal match,
with physical sample names. A tumour matched with two normals gives two rows. Samples without any
declared match fall back to the previous behaviour: tumours are paired with the unmatched normals of
the same specimen. `digest_readset_file` is unchanged.

### `sample_oncotree_code`: tumour type

The [OncoTree](https://oncotree.mskcc.org/) code of the tumour, e.g. `LUAD`, left empty for normals.

- It is trimmed and uppercased; it must be letters and digits only, otherwise the whole payload is
  rejected before anything is written. Checking the code against the OncoTree list is up to the sender.
- Re-ingesting a sample that already has a **different** code keeps the stored code and returns a
  warning. Use `force` to replace it, or `pt-cli edit` on the `sample.oncotree_code` column.

## Enum values

Enum fields only accept their exact values: no trimming, no case change, and a lane is the string
`"1"`, not the number `1`.

| Payload | Key | Values | Missing or `null` |
|---|---|---|---|
| `run_processing` | `readset_lane` | `"1"` to `"8"` | error |
| `run_processing` | `readset_sequencing_type` | `SINGLE_END`, `PAIRED_END` | error |
| `run_processing` | `readset_state` | `VALID`, `ON_HOLD`, `INVALID`, `DELIVERED` | `VALID` |
| `run_processing`, `analysis` | `metric_flag` | `PASS`, `WARNING`, `FAILED`, `MISSING`, `NOT_APPLICABLE` | no flag |
| `analysis` | `job_status` | `PENDING`, `RUNNING`, `COMPLETED`, `FAILED`, `OUT_OF_MEMORY`, `CANCELLED` | job skipped |

- An ingest payload with invalid values is rejected before anything is written. The error lists every
  invalid value with its readset, job or metric and the valid values.
- `edit` on an enum column takes the exact value or the member name (lane `"1"` or `ONE`). Anything
  else is an error and nothing changes.

### Readset state

`digest_readset_file` leaves out `ON_HOLD` and `INVALID` readsets, with a warning for each one. To
include a readset again, change its state with `pt-cli edit` on the `readset.state` column.

## Analysis ingestion

`POST /project/<project>/ingest_analysis` (`pt-cli ingest analysis`) ingests an analysis operation,
e.g. a GenPipes run. It replaces `ingest_genpipes`. The payload names the tool, the pipeline and the
protocol:

```json
{"operation_name": "genpipes", "operation_pipeline": "RnaSeq", "operation_protocol": "cancer", ...}
{"operation_name": "oncoanalyser", "operation_pipeline": "wgts", ...}
{"operation_name": "dragen", "operation_pipeline": "somatic", "operation_protocol": "tumor_normal", ...}
```

- The three values must come from the analysis operations vocabulary, with exact case. Otherwise the
  whole payload is rejected before anything is written, with the allowed values in the error.
  `run_processing`, `transfer` and `delivery` are reserved for the other ingest routes.
- The stored operation has `analysis = true`. `digest_unanalyzed` uses this flag, not operation names.
- Re-ingesting reuses the existing operation, matched as before on its name, command line, platform,
  config, status and project. An empty pipeline or protocol is filled in. A **different** stored value
  is kept, with a warning: use `force` to replace it, or `pt-cli edit` on the `operation` columns.
- Analyses ingested before this change are named `genpipes` with an empty pipeline and protocol: the
  migration sets their `analysis` flag. Re-ingesting one with a payload in the new format fills its
  pipeline and protocol in. Payloads still sending `"operation_name": "GenPipes_<Pipeline>.<protocol>"`
  are rejected.

### Analysis operations vocabulary

The default vocabulary is `project_tracking/analysis_operations.json`. It lists:

- `genpipes`: the GenPipes 6.2.0 pipelines (their class names) and protocols, with `default` for
  pipelines that have a single protocol. Update it with each GenPipes release that adds a pipeline or
  a protocol.
- `oncoanalyser` (nf-core/oncoanalyser): its `--mode` as pipeline, `wgts` or `targeted`, and no
  protocol.
- `dragen` (Illumina DRAGEN run outside GenPipes): its secondary analysis pipelines as pipeline, with
  `tumor_only` or `tumor_normal` as protocol for `somatic` and no protocol for the others. DRAGEN run
  by GenPipes stays `genpipes` / `MethylSeq` / `dragen`. Demultiplexing, ORA compression and the
  reference builder are left out: they aren't analyses of a readset. Put the DRAGEN version in the
  operation config, not in the protocol.

To change it without a release, copy the file, edit it and set it as `ANALYSIS_OPERATIONS` in the
config, or as `C3G_ANALYSIS_OPERATIONS` / `C3G_ANALYSIS_OPERATIONS_FILE`. It replaces the whole
default, so later changes to the default don't reach a deployment that overrides it. For example:

```json
{
  "genpipes": {"RnaSeq": ["stringtie", "variants", "cancer"], "RnaSeqLight": ["default"]},
  "dragen": {"germline": [], "somatic": ["tumor_normal"]}
}
```

- `operation_name` → `operation_pipeline` → allowed `operation_protocol` values.
- `{}`: the tool takes no `operation_pipeline`. `[]`: the pipeline takes no `operation_protocol`.
- Names use letters, digits, `_`, `.` and `-`, 64 characters max. An invalid vocabulary stops the app
  at startup with the list of problems.

---

## Query filtering syntax

Most GET routes accept a `json=` query parameter to filter results by name (or URI for locations).

### Exact match

Pass one or more comma-separated values:
```
/project/<project>/files?json={"file_name": "my_file.tsv,other_file.bam"}
```

### Regex match (`r:` prefix)

Prefix any value with `r:` to treat it as a PostgreSQL POSIX regular expression (`~` operator):
```
/project/<project>/files?json={"file_name": "r:^MoHQ.*\.tsv$"}
```

Simple patterns made only of a literal (letters, digits, `-`, `_`, `/`, `:`…) with optional `^`, `$`
and leading/trailing `.*` are run as a faster `LIKE` (exact, prefix, suffix or contains), which gives
the same result on PostgreSQL (on SQLite, `LIKE` ignores case). Any other pattern needs PostgreSQL:
SQLite has no `~` operator and the request fails.

Locations are filtered by URI with the same syntax, e.g. all BAM locations of a sample:
```
/project/<project>/locations?json={"location_uri": "r:.*MoHQ-CM-1-3-15000863775-19933DT.*\.bam$"}
```

#### Important regex caveats

- **`.` matches any character**, not a literal dot. Use `\.` for a literal dot.
  - `r:.*output.tsv` also matches `outputXtsv`, `output_tsv`, etc.
  - `r:.*output\.tsv` matches only strings containing `output.tsv`

- **No end anchor by default.** Without `$`, the pattern matches anywhere inside the string.
  - `r:.*output\.tsv` matches both `output.tsv` **and** `output.tsv.gz`
  - `r:.*output\.tsv$` matches only strings ending exactly with `output.tsv`

- **Combine both** for an unambiguous suffix match on a filename:
  ```
  r:.*sample_name\.tsv$
  ```

Common patterns:

| Intent | Pattern |
|---|---|
| Starts with `foo` | `r:^foo.*` |
| Ends with `.tsv` (literal dot) | `r:.*\.tsv$` |
| Contains `sample_01` | `r:.*sample_01.*` |
| Exact match | `r:^exact_name$` |

## Development

### Run tests
Once you have modified the code, you can run the test to make sure you have not broken anything. In the git repo:
```bash
pip install -e .[tests]
pytest -v
```

Each test runs on its own temporary SQLite database, so PostgreSQL-only behaviour (regex filters,
JSONB) is not covered by the suite: check it against a PostgreSQL database.

### Schema changes

Change the models in `project_tracking/model.py`, then add an Alembic revision in
`migration/alembic/versions/` so existing databases get the change with `upgrade-all-dbs`
(new databases get it directly from `init-db`). On a database at the latest revision,
`alembic -x db_url=<URI> check` reports any difference left between the models and the migrations.
