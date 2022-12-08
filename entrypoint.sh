#!/usr/bin/env bash

if [[ -v "${C3G_SQLALCHEMY_DATABASE_URI_FILE}" ]]; then
  export C3G_SQLALCHEMY_DATABASE_URI=$(< C3G_SQLALCHEMY_DATABASE_URI_FILE)
fi


if [[ -v C3G_INIT_DB ]]; then
  echo instanciating data base
  flask  --app $APP init-db
fi

gunicorn "project_tracking:create_app()"  "${@}"
