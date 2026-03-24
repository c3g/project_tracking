#!/usr/bin/env bash

# Load *_FILE secret files into env vars for use by the flask commands below.
# The flask app itself also does this via _load_prefixed_file_env(), but the
# commands (init-db, upgrade-all-dbs) need the vars available in the shell too.
if [[ -v C3G_SQLALCHEMY_DATABASE_URI_FILE ]]; then
  value=$(< "${C3G_SQLALCHEMY_DATABASE_URI_FILE}")
  export C3G_SQLALCHEMY_DATABASE_URI="${value}"
fi
if [[ -v C3G_PROJECT_DATABASES_FILE ]]; then
  value=$(< "${C3G_PROJECT_DATABASES_FILE}")
  export C3G_PROJECT_DATABASES="${value}"
fi

DB_OPS=()

if [[ -v C3G_SQLALCHEMY_DATABASE_URI ]]; then
  DB_OPS=(--db-uri "${C3G_SQLALCHEMY_DATABASE_URI}")
fi

if [[ -v C3G_INIT_DB ]]; then
  echo "Initializing database"
  flask --app "$APP" init-db "${DB_OPS[@]}"
fi

if [[ -v C3G_ALEMBIC_UPGRADE ]]; then
  # Upgrade every configured project database to the same revision.
  # upgrade-all-dbs reads PROJECT_DATABASES and applies the same migration
  # to all of them, keeping schemas identical across all project DBs.
  echo "Running alembic upgrade on all configured databases"
  flask --app "$APP" upgrade-all-dbs
fi

gunicorn "project_tracking:create_app()"  "${@}"
