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

For a database created from the models at revision `64a560017524` (release 3.1.6, before the
`3c7f1a2d9e64` migration adding sample metadata, analysis operations and sample groups), stamp
`64a560017524`; the next restart then applies the pending migration. Run `db-revisions` again to
confirm.

---

## Sample metadata at ingestion

Four optional keys can be set on each sample of a `run_processing` JSON.

### `sample_group_name`: samples analysed together

The groups this sample belongs to, e.g. a tumour/normal duo, or a trio with the tumour RNA. One analysis
runs per group. Give a name or a list of names:

```json
{"sample_name": "SAMPLE-1-1DT", "sample_tumour": true, "sample_group_name": "SPECIMEN-1_G1", "readset": [...]}
{"sample_name": "SAMPLE-1-1DN", "sample_tumour": false, "sample_group_name": ["SPECIMEN-1_G1", "SPECIMEN-1_G2"], "readset": [...]}
```

- The first sample naming a group creates it, for the sample's specimen. Later samples, in the same
  payload or in a later run, join it: when the RNA arrives with `"sample_group_name": "SPECIMEN-1_G1"`,
  the duo becomes a trio under the same name. Each group created is reported in `DB_ACTION_WARNING`, so a
  misspelled name doesn't go unnoticed.
- A sample can be in several groups, e.g. a normal shared by two tumours.
- A group holds samples of one specimen only. Names use letters, digits, `_`, `.` and `-` (128 characters
  max): the name becomes oncoanalyser's `group_id`. Otherwise the whole payload is rejected before
  anything is written.
- Ingestion only adds: re-ingesting a sample without the key, or with other names, keeps its groups.
  Use [`/sample_group`](#editing-sample-groups) to remove or move a sample, and `edit` to rename a group.
  Curating a sample removes it from its groups.

`digest_pair_file` uses the groups: one `Specimen,Sample_N,Sample_T` row per tumour/normal pair of a
group, with physical sample names. A tumour grouped with two normals gives two rows. Tumours in no group
fall back to the previous behaviour: they're paired with the normals of the same specimen that are in no
group either. `digest_readset_file` is unchanged.

### Editing sample groups

- `GET /project/<project>/sample_groups` (`pt-cli sample_group list`) lists the groups with their id,
  specimen and sample names. Filter with `specimen_name`, `sample_group_name` and `sample_name` lists:
  `sample_name` gives the groups holding any of these samples.
- `POST /project/<project>/sample_group` (`pt-cli sample_group edit`) changes the samples of one group:

```json
{"sample_group_name": "SPECIMEN-1_G1", "add": ["SAMPLE-1-1RT"], "remove": ["SAMPLE-1-1DN"]}
```

- `add` on a group that doesn't exist creates it. The samples must exist in the project and belong to the
  group's specimen. A group left without samples is deleted.
- Problems are all listed in `DB_ACTION_ERROR`, and nothing changes then.
- A group is renamed, deprecated, deleted or curated like any other entry, with the modification routes
  on the `sample_group` table. `edit` keeps the name format and uniqueness, and refuses to change a
  group's `specimen_id`:

```json
{"modification": [{"table": "sample_group", "id": [1], "column": "name", "new": "SPECIMEN-1_G3"}]}
```

- A deprecated or deleted group can't be joined at ingestion, and is left out of the samplesheets.

### `sample_oncotree_code`: tumour type

The [OncoTree](https://oncotree.mskcc.org/) code of the tumour, e.g. `LUAD`, left empty for normals.

- It is trimmed and uppercased; it must be letters and digits only, otherwise the whole payload is
  rejected before anything is written. Checking the code against the OncoTree list is up to the sender.
- Re-ingesting a sample that already has a **different** code keeps the stored code and returns a
  warning. Use `force` to replace it, or `pt-cli edit` on the `sample.oncotree_code` column.

### `sample_cancer_type`: cancer type as text

The cancer type in words, e.g. `Lung_Adenocarcinoma`, alongside the OncoTree code. GenPipes has the
same pair in its cancer ini files (`oncotree_code`, `cancer_type`).

```json
{"sample_name": "SAMPLE-2-1DT", "sample_tumour": true,
 "sample_oncotree_code": "LUAD", "sample_cancer_type": "Lung Adenocarcinoma", "readset": [...]}
```

is stored as `Lung_Adenocarcinoma`.

- It is free text with its case kept. Spaces become underscores, runs of spaces become one, and leading
  and trailing ones are dropped, so tools such as oncoanalyser take it as one word on their command lines.
  An empty string is ignored. A value that isn't a string rejects the whole payload before anything is
  written.
- Re-ingesting a sample that already has a **different** type keeps the stored type and returns a
  warning. Use `force` to replace it, or `pt-cli edit` on the `sample.cancer_type` column.

## Controlled vocabularies

There are two kinds of controlled vocabularies. Pick the kind by what the code does with the values:

| Kind | Use it when | Defined in | Changing it |
|---|---|---|---|
| Enum | The code acts on specific values, e.g. skips `ON_HOLD` readsets or filters on `VALID` | `*Enum` classes in `project_tracking/model.py`, PostgreSQL enum types | Code change, Alembic migration and release |
| Analysis vocabulary | The code only checks that an external name is known, e.g. GenPipes pipelines and protocols | `project_tracking/analysis_operations.json`, overridable in the config | Edit the JSON file, or the config override without a release |

Both kinds are checked before anything is written, accept exact values only, and list the allowed values
in their errors. `GET /vocabulary` (`pt-cli vocabulary`) returns the values in use: every enum, the
active analysis vocabulary (the config override when set) and the reserved operation names.

### Enum values

Enum fields only accept their exact values: no trimming, no case change, and a lane is the string
`"1"`, not the number `1`.

| Payload | Key | Values | Missing or `null` |
|---|---|---|---|
| `run_processing` | `readset_lane` | `"1"` to `"8"` | error |
| `run_processing` | `readset_sequencing_type` | `SINGLE_END`, `PAIRED_END` | error |
| `run_processing` | `readset_state` | `VALID`, `ON_HOLD`, `INVALID`, `DELIVERED` | `VALID` |
| `transfer` | `readset_state` | same as `run_processing` | state unchanged |
| `run_processing`, `analysis` | `metric_flag` | `PASS`, `WARNING`, `FAILED`, `MISSING`, `NOT_APPLICABLE` | no flag |
| `analysis` | `job_status` | `PENDING`, `RUNNING`, `COMPLETED`, `FAILED`, `OUT_OF_MEMORY`, `CANCELLED` | job skipped |
| `analysis` | `operation_status` | same as `job_status` | deduced from the jobs |

- An ingest payload with invalid values is rejected before anything is written. The error lists every
  invalid value with its readset, job or metric and the valid values.
- `edit` on an enum column takes the exact value or the member name (lane `"1"` or `ONE`). Anything
  else is an error and nothing changes.

### Readset state

`digest_readset_file` and `digest_oncoanalyser_samplesheet` leave out `ON_HOLD` and `INVALID` readsets,
with a warning for each one. To
include a readset again, change its state with `pt-cli edit` on the `readset.state` column.

A transfer can also change the state of its readsets: give `readset_state` on a readset of the
`transfer` JSON.

```json
{"readset": [{"readset_name": "SAMPLE-1-1DT.A00000_0001_1", "readset_state": "ON_HOLD", "file": [...]}]}
```

- Each change is reported in `DB_ACTION_WARNING`. Readsets without the key keep their state.
- An invalid value rejects the whole payload before anything is written.
- Sending the same transfer again with another state changes it again.

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
- The operation status is `operation_status` when given. Otherwise it's `COMPLETED` when every job of
  the payload is `COMPLETED`, and `RUNNING` when any job isn't (failed, pending or without status): the
  run isn't finished.
- Re-ingesting reuses the existing operation, matched on its name, command line, platform, config and
  project, and updates its status with a warning. A run ingested while `RUNNING`, then once
  `COMPLETED`, stays one operation. An empty pipeline or protocol is filled in. A **different** stored value
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
default, so later changes to the default don't reach a deployment that overrides it: compare with
`GET /vocabulary`. For example:

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

## oncoanalyser samplesheet

`GET /project/<project>/digest_oncoanalyser_samplesheet` (`pt-cli digest oncoanalyser_samplesheet`) gives
the [nf-core/oncoanalyser samplesheet](https://nf-co.re/oncoanalyser/latest/docs/usage/#samplesheet) of
[sample groups](#sample_group_name-samples-analysed-together): every group of the given specimens
(`specimen_name` or `specimen_id`) and the groups named in `sample_group_name`, with the readsets at
`location_endpoint`. `experiment_nucleic_acid_type` is optional: DNA and RNA readsets go together when
it's missing. pt-cli writes one `<group>.samplesheet.csv` per group.

```bash
pt-cli --project MY-PROJECT digest oncoanalyser_samplesheet --specimen_name SPECIMEN-1 --endpoint abacus --output-dir samplesheets/
```

```csv
group_id,subject_id,sample_id,sample_type,sequence_type,filetype,info,filepath
SPECIMEN-1_G1,SPECIMEN-1,SAMPLE-1-1DT,tumor,dna,fastq,library_id:SAMPLE-1-1DT.A00000_0001_1;lane:001;flowcell:H0000AAXX;cancer_type:Lung,/path/R1.fastq.gz;/path/R2.fastq.gz
```

| Column | Value |
|---|---|
| `group_id` | Sample group name |
| `subject_id` | Specimen name |
| `sample_id` | Sample name |
| `sample_type` | `tumor` or `normal`, from `sample.tumour` |
| `sequence_type` | `dna` or `rna`, from the experiment |
| `filetype` | `fastq` |
| `info` | `library_id:<readset name>;lane:<lane on 3 digits>;flowcell:<flowcell>`, plus `;cancer_type:<sample.cancer_type>` on tumour rows |
| `filepath` | `<R1>;<R2>`: the readset's `run_processing` fastq files at `location_endpoint` |

- One row per readset, by group, then DNA before RNA, tumour before normal, sample and lane.
- Samples in no group, and specimens without groups, are left out with a warning.
- The readset name is the library id, so every readset is its own read group. Lanes of one library are
  therefore deduplicated separately.
- The flowcell comes from the run name, an Illumina run folder: `250101_A00000_0001_BH0000AAXX_…` gives
  `H0000AAXX`, MiSeq `…_000000000-ABCDE` gives `000000000-ABCDE`. Another run name is used as is.
- oncoanalyser passes `cancer_type` unquoted on command lines and splits `info` on `;` and `:`. Ingestion
  already turns spaces into underscores; a cancer type with `;`, `:` or `,` is left out with a warning.
- Readsets without both fastq files at the endpoint (e.g. BAM-only readsets), `ON_HOLD` and `INVALID`
  readsets are left out with a warning.
- oncoanalyser takes one sample per `sample_type` and `sequence_type` in a group. A group with, e.g., two
  DNA tumours is rejected with the group and sample names: remove the extra sample from the group, or set
  its readsets to `ON_HOLD`.

## API documentation (OpenAPI)

The API describes itself in the [OpenAPI 3](https://spec.openapis.org/oas/v3.0.3) format:

- `GET /openapi.json`: the specification. A copy is committed in [docs/openapi.json](docs/openapi.json).
- `GET /docs`: [Swagger UI](https://swagger.io/tools/swagger-ui/), to browse the routes and send requests
  with "Try it out". The requests run with your login session, so they read and change real data.

Both are behind the same login as the rest of the API. Swagger UI is vendored in
`project_tracking/static/swagger-ui` (version and checksums in its `VERSION` file): `/docs` loads no third
party script, and its `Content-Security-Policy` header only allows these files.

The specification is generated from the code, in `project_tracking/openapi.py`:

- Paths and methods come from the routes. The summary and description come from the view's docstring:
  a GET is summarized by its `return:` line, a POST by its first line.
- The `json` query keys of a GET and the body keys of a POST come from `parse_json_get` and
  `parse_json_input`.
- The entities returned come from the marshmallow schemas in `project_tracking/schema.py`.
- Anything else goes in `@openapi_doc(...)` under the route, e.g. the payload of the modification routes.
- The ingest routes show an example payload from `project_tracking/ingest_examples.json`, with generic
  names (`MY-PROJECT`, `SPECIMEN-1`, `SAMPLE-1-1DT`, ...). `tests/test_openapi.py` ingests the four in
  order, so an example that stops being a valid payload fails the tests. Don't send them with "Try it
  out" on a real project: they would be ingested.

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
/project/<project>/files?json={"file_name": "r:^SAMPLE.*\.tsv$"}
```

Simple patterns made only of a literal (letters, digits, `-`, `_`, `/`, `:`…) with optional `^`, `$`
and leading/trailing `.*` are run as a faster `LIKE` (exact, prefix, suffix or contains), which gives
the same result on PostgreSQL (on SQLite, `LIKE` ignores case). Any other pattern needs PostgreSQL:
SQLite has no `~` operator and the request fails.

Locations are filtered by URI with the same syntax, e.g. all BAM locations of a sample:
```
/project/<project>/locations?json={"location_uri": "r:.*SAMPLE-1-1DT.*\.bam$"}
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

Then regenerate the database diagram (`docs/database.dbml` and `docs/data/database.svg`, see
[docs/README.md](docs/README.md)), and add any new table to a stage of `STAGES` in
`project_tracking/dbml.py`:

```bash
utils/update_database_diagram.sh
```

`tests/test_dbml.py` and the Database Diagram workflow fail while the diagram is out of date.

### Route changes

After adding or changing a route, regenerate the committed specification:

```bash
flask --app project_tracking openapi -o docs/openapi.json
```

`tests/test_openapi.py` fails while it's out of date, when a route has no docstring, or when a POST
doesn't describe its payload (with `parse_json_input` or `@openapi_doc(requestBody=...)`).
