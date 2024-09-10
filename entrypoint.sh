#!/usr/bin/env bash

if [[ -v "${C3G_SQLALCHEMY_DATABASE_URI_FILE}" ]]; then
  export C3G_SQLALCHEMY_DATABASE_URI=$(< C3G_SQLALCHEMY_DATABASE_URI_FILE)
fi

DB_OPS=

if [[ -v "${C3G_SQLALCHEMY_DATABASE_URI}" ]]; then
   DB_OPS=--db-uri ${C3G_SQLALCHEMY_DATABASE_URI}
fi

if [[ -v "${C3G_INIT_DB}" ]]; then
  echo instanciating data base
  flask  --app $APP   init-db $DB_OPS
fi

if [[ -v "${C3G_ALEMBIC_UPGRADE}" ]]; then
   alembic upgrade head
fi 

gunicorn "project_tracking:create_app()"  "${@}"
