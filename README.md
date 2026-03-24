# Tracking the C3G projects
[![Test suite](https://github.com/c3g/project_tracking/actions/workflows/PyTest.yml/badge.svg?branch=main)](https://github.com/c3g/project_tracking/actions/workflows/PyTest.yml) on main branch  
[![Test suite](https://github.com/c3g/project_tracking/actions/workflows/PyTest.yml/badge.svg?branch=dev)](https://github.com/c3g/project_tracking/actions/workflows/PyTest.yml) on dev branch

This is an API to access and modify the C3G data processing tracking database.

## Install
We recommend using postgress in producton, but the project is fully compatible with sqlite.
We also publish container on GitHub reposityory (ghcr.io) and test or system using podman.
### From GitHub with sqlite (best for developer):
Sqlite needs to be installed on your machine.
Here, you will deploy a development instance of the app and be able to modify the code in the repo with auto-reload 
```bash
git clone  git@github.com:c3g/project_tracking.git
cd project_tracking
git checkout dev # If you are developing from the dev branch!
python -m venv venv
source ./venv/bin/activate
pip install --upgrade pip
pip install -e  .
# Setting the db url is optional, the default will be in the app installation folder
export C3G_SQLALCHEMY_DATABASE_URI="sqlite:////tmp/my_test_db.sql"
# initialyse the db
flask --app project_tracking init-db
# run the app
flask --app project_tracking --debug run
```

By default, falsk will run the server on  http://127.0.0.1:5000. You can test that everything 
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



Once the server is running, you can still initialise the database, you can even flush it clear of any entry with:

```bash
# WARNING this will erase all entry to you Database!
flask  --app project_tracking init-db --flush --db-uri "sqlite:////tmp/my_test_db.sql"
```

### init-db and --flush in multi-database setups

`init-db` always targets one database URI at a time.

Behavior:
1. `flask --app project_tracking init-db` initializes only `SQLALCHEMY_DATABASE_URI` (default DB).
2. `flask --app project_tracking init-db --db-uri <URI>` initializes only that explicit DB.
3. Adding `--flush` drops and recreates tables only on the targeted DB URI.
4. `flask --app project_tracking init-all-dbs` initializes default DB + all DB URIs from `PROJECT_DATABASES`.
5. `flask --app project_tracking init-all-dbs --flush --yes-i-really-mean-flush` flushes and initializes all configured DBs.

`init-all-dbs --flush` is intentionally guarded and requires `--yes-i-really-mean-flush`.

Example:

```bash
flask --app project_tracking init-db --db-uri "postgresql+psycopg2://USER:PW@DBHOST/default_db?client_encoding=utf8"
flask --app project_tracking init-db --db-uri "postgresql+psycopg2://USER:PW@DBHOST/PRJA_DB?client_encoding=utf8"
flask --app project_tracking init-db --db-uri "postgresql+psycopg2://USER:PW@DBHOST/GRP_DB?client_encoding=utf8"
flask --app project_tracking init-all-dbs
```

Use `--flush` only with an explicit `--db-uri` to avoid wiping the wrong database.

### Using podman and sqlite:
We have a [ghcr.io repo for the project](https://github.com/c3g/project_tracking/pkgs/container/project_tracking)
There are version releases and `latest` tag relates to the latest release.
```bash
SQLITE_DB_FOLDER=<folder on host with WR access>
podman run -v $SQLITE_DB_FOLDER:/sqlite:Z -it --rm -p 8000:8000 -e C3G_INIT_DB=1 ghcr.io/c3g/project_tracking:latest
```
The app runs on port 8000 inside the container, the `-e C3G_INIT_DB=1` option will the db in 
`$SQLITE_DB_FOLDER/tracking_db.sql`. 



### From GitHub using postgress:
postgress needs to be installed with a database  names <DB_NAME>,
assessible by user <POSTGRESS_USER>, with a pasword <POSTGRESS_PW>

```bash
git clone  git@github.com:c3g/project_tracking.git
cd project_tracking
python -m venv venv
source ./venv/bin/activate
pip install .[postgres]
C3G_SQLALCHEMY_DATABASE_URI="postgresql+psycopg2://<POSTGRESS_USER>:<POSTGRESS_PW>@<POSTGRESS_HOST>/<DB_NAME>?client_encoding=utf8"
gunicorn -w 4 'project_tracking:create_app()'
````

### Using podman and postgress:
Here we expect postgres to be listening to the localhost (127.0.0.1) interface. 
The podman option `--network slirp4netns:allow_host_loopback=true` 
options makes it so that the host `127.0.0.1` interface is
reachable with the `10.0.2.2` adress inside the container. That is why the C3G_SQLALCHEMY_DATABASE_URI
is set to that value.
```bash
podman pull ghcr.io/c3g/project_tracking:latest
export C3G_SQLALCHEMY_DATABASE_URI="postgresql+psycopg2://<POSTGRESS_USER>:<POSTGRESS_PW>@10.0.2.2/<POSTGRESS_DB_NAME>?client_encoding=utf8"
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

### Local development

`instance/` is already ignored by git (`.gitignore` contains `instance/`), so local config can safely live there.

Create `instance/config.py`:

```python
SQLALCHEMY_DATABASE_URI = "postgresql+psycopg2://<USER>:<PW>@<HOST>/<DEFAULT_DB>?client_encoding=utf8"

# PostgreSQL identifiers are easiest to operate if you keep DB/user names in
# lowercase snake_case (avoids quoting surprises).
# Routing map used by the API:
# - exact project-name match only (case-insensitive)
PROJECT_DATABASES = {
        "PROJ-A": "postgresql+psycopg2://<USER>:<PW>@<HOST>/prja_db?client_encoding=utf8",
        "PROJ-B": "postgresql+psycopg2://<USER>:<PW>@<HOST>/grp_db?client_encoding=utf8",
        "PROJ-C": "postgresql+psycopg2://<USER>:<PW>@<HOST>/grp_db?client_encoding=utf8",
}
```

You can also use env vars only:

```bash
export C3G_SQLALCHEMY_DATABASE_URI="postgresql+psycopg2://<USER>:<PW>@<HOST>/<DEFAULT_DB>?client_encoding=utf8"
export C3G_PROJECT_DATABASES='{"PROJ-A":"postgresql+psycopg2://<USER>:<PW>@<HOST>/prja_db?client_encoding=utf8","PROJ-B":"postgresql+psycopg2://<USER>:<PW>@<HOST>/grp_db?client_encoding=utf8","PROJ-C":"postgresql+psycopg2://<USER>:<PW>@<HOST>/grp_db?client_encoding=utf8"}'
```

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
3. If a project name has no entry in `PROJECT_DATABASES`, requests for that project use `SQLALCHEMY_DATABASE_URI` (default DB).

Recommended workflow when adding a new project:
1. Decide which DB should own that project.
2. Add or update the exact project-name key in `PROJECT_DATABASES` (env/secret/config).
3. Run schema migration for all configured DBs:

```bash
flask --app project_tracking upgrade-all-dbs
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

If `PROJ-B` is mapped to `GRP_DB` in `PROJECT_DATABASES`, it is created in `GRP_DB`.
If `PROJ-B` is not mapped, it is created in the default database.

### Production recommendation

Use environment/secret injection from your orchestrator (podman secrets, Kubernetes secrets, etc.) and keep DB credentials out of the repository.

Recommended pattern:
1. Set `C3G_SQLALCHEMY_DATABASE_URI` to a safe fallback/default DB.
2. Set `C3G_PROJECT_DATABASES` (JSON) with all routing entries.
3. Run schema upgrades on all configured DBs during deployment:

```bash
flask --app project_tracking upgrade-all-dbs
```

This keeps one migration history while ensuring every database is upgraded to the same revision.

For secret-file based deployments, the app also supports `C3G_*_FILE` variables.
If `C3G_SQLALCHEMY_DATABASE_URI` is not set and `C3G_SQLALCHEMY_DATABASE_URI_FILE` is set,
the app reads the secret file and loads the value automatically.
Same behavior for `C3G_PROJECT_DATABASES_FILE`.

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

## Production deployment with Podman secrets

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
{"PROJ-A":"postgresql+psycopg2://USER:PW@DBHOST/prja_db?client_encoding=utf8","PROJ-B":"postgresql+psycopg2://USER:PW@DBHOST/grp_db?client_encoding=utf8","PROJ-C":"postgresql+psycopg2://USER:PW@DBHOST/grp_db?client_encoding=utf8"}
EOF

sudo chmod 600 /etc/project_tracking/secrets/C3G_*
```

Load the files into Podman's secret store (**run as the same user that owns the systemd unit**):

```bash
podman secret create C3G_SQLALCHEMY_DATABASE_URI /etc/project_tracking/secrets/C3G_SQLALCHEMY_DATABASE_URI
podman secret create C3G_PROJECT_DATABASES       /etc/project_tracking/secrets/C3G_PROJECT_DATABASES
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
systemctl --user status  container-project_tracking-api.service
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
-- Create a dedicated API user (do this once per deployment, not per DB)
CREATE USER project_tracking_api WITH PASSWORD 'strong_random_password';

-- Create the new database
CREATE DATABASE newdb OWNER project_tracking_api ENCODING 'UTF8';

-- Connect to the new database and grant minimum required permissions
\c newdb
GRANT CONNECT ON DATABASE newdb TO project_tracking_api;
GRANT USAGE ON SCHEMA public TO project_tracking_api;
-- After first migration these broader grants are needed:
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO project_tracking_api;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT USAGE, SELECT ON SEQUENCES TO project_tracking_api;
```

#### 2 — Run schema migrations on all databases

Run this before restarting the API so the new DB has the correct schema:

```bash
# From the project repo with virtualenv active and config pointing at all DBs
flask --app project_tracking upgrade-all-dbs
```

#### 3 — Update secret files on the VM

Edit the JSON secret file to add the new mapping:

```bash
sudo vi /etc/project_tracking/secrets/C3G_PROJECT_DATABASES
# Add the new entry, e.g.:
# {"PROJ-A":"...","PROJ-B":"...","PROJ-C":"...","NEWPROJECT":"postgresql+psycopg2://project_tracking_api:PW@DBHOST/newdb?client_encoding=utf8"}
```

#### 4 — Rotate Podman secrets

Podman secrets are immutable; to update one you delete and recreate it:

```bash
podman secret rm  C3G_PROJECT_DATABASES
podman secret create C3G_PROJECT_DATABASES /etc/project_tracking/secrets/C3G_PROJECT_DATABASES
```

#### 5 — Restart the service

```bash
systemctl --user restart container-project_tracking-api.service
systemctl --user status  container-project_tracking-api.service
```

The new routing entry is now live.  
Any request for project name `NEWPROJECT` will be routed to `newdb`.

---

### Production maintenance operations (runbook)

Use this table to decide which operation to run in production.

1. Normal deploy or restart: use migrations only.
2. Brand new empty DB (tables do not exist): initialize once, then migrate.
3. Existing DB that already has tables/data: never run `--flush`.

Recommended defaults in the systemd unit:
1. Keep `C3G_ALEMBIC_UPGRADE=1` enabled.
2. Keep `C3G_INIT_DB` unset.

#### Use case A — Regular restart/deploy

No special action. Just restart:

```bash
systemctl --user restart container-project_tracking-api.service
```

At startup the container runs `upgrade-all-dbs` and migrates all configured DBs.

#### Use case B — You added a new database URI to PROJECT_DATABASES

You usually do **not** need `C3G_INIT_DB` for this.
Preferred sequence:

```bash
# 1) Update C3G_PROJECT_DATABASES secret and recreate the podman secret
podman secret rm  C3G_PROJECT_DATABASES
podman secret create C3G_PROJECT_DATABASES /etc/project_tracking/secrets/C3G_PROJECT_DATABASES

# 2) Restart service (startup will run upgrade-all-dbs)
systemctl --user restart container-project_tracking-api.service
```

If the new DB user/schema permissions are correct, migrations will create the schema on the new DB.

#### Use case C — Brand new empty default DB and you want explicit init first

Run `C3G_INIT_DB` as a one-off, then remove it:

```bash
systemctl --user set-environment C3G_INIT_DB=1
systemctl --user restart container-project_tracking-api.service
systemctl --user unset-environment C3G_INIT_DB
```

This initializes only the default DB URI used by `init-db`.

#### Use case D — Initialize all configured DBs explicitly

Use the CLI command (from the repo with config/secrets available):

```bash
flask --app project_tracking init-all-dbs
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

---

### Run tests
Once you have modified the code, you can run the test to make sure you have not broken anything. In the git repo:
```bash
pip install -e  .[tests]
pytest -v
```






