#!/bin/bash
# Reset the project_tracking DEV databases to empty schemas (runbook: "project_tracking dev deployment runbook").
# Touches only the databases listed below, never anything else on this server.
#   sudo project_tracking-dev-reset         dry run: checks everything, shows what it would do
#   sudo project_tracking-dev-reset --yes   does it: stops the dev API, recreates the databases empty (schemas, no
#                                           project), starts the API
#   ... --only project_tracking_cphi_dev     only that database (repeatable), e.g. to keep the prod copy in the MoH one
# Projects are created afterwards by an admin: pt-cli admin create-project --name X --database <label>. To get MoH's
# production data instead of an empty database, copy prod into the dev MoH database (runbook).
set -euo pipefail

DATABASES=(project_tracking_moh_dev project_tracking_cphi_dev)
API_UNIT=podman-project_tracking-dev-api.service
IMAGE=ghcr.io/c3g/project_tracking:dev
# Roles that read every dev database (the dev pgweb instances of pgweb.c3g-app.sd4h.ca), granted again after the reset
READERS=(pgweb)

log() { echo "$*"; logger -t project_tracking-dev-reset -- "$*"; }
refuse() { log "REFUSED, nothing changed: $*"; exit 1; }

[ "$(id -u)" = 0 ] || { echo "Run it with sudo." >&2; exit 1; }
cd /tmp
YES=0
RESET=()  # the databases reset: --only ones, else all of DATABASES
usage() { echo "Usage: sudo $(basename "$0") [--yes] [--only <database>]..." >&2; exit 2; }
while [ $# -gt 0 ]; do
    case "$1" in
        --yes) YES=1 ;;
        --only)
            [ $# -ge 2 ] || usage
            [[ " ${DATABASES[*]} " == *" $2 "* ]] || { echo "'$2' isn't one of: ${DATABASES[*]}" >&2; exit 2; }
            RESET+=("$2"); shift ;;
        *) usage ;;
    esac
    shift
done
[ ${#RESET[@]} -gt 0 ] || RESET=("${DATABASES[@]}")

G=$(id -u genome)
P=(sudo -u genome "XDG_RUNTIME_DIR=/run/user/$G" podman)
S=(sudo -u genome "XDG_RUNTIME_DIR=/run/user/$G" "DBUS_SESSION_BUS_ADDRESS=unix:path=/run/user/$G/bus" systemctl --user)
psql_at() { sudo -u postgres psql -v ON_ERROR_STOP=1 -Atq "$@"; }
# shellcheck disable=SC2054  # the commas are inside --secret values
ONEOFF=("${P[@]}" run --rm --network slirp4netns:allow_host_loopback=true
    --secret PROJECT_TRACKING_DEV_DATABASE_URI,type=mount --secret PROJECT_TRACKING_DEV_PROJECT_DATABASES,type=mount
    -e C3G_SQLALCHEMY_DATABASE_URI_FILE=/run/secrets/PROJECT_TRACKING_DEV_DATABASE_URI
    -e C3G_PROJECT_DATABASES_FILE=/run/secrets/PROJECT_TRACKING_DEV_PROJECT_DATABASES)

log "Start ($([ $YES = 1 ] && echo "for real" || echo "dry run")) by ${SUDO_USER:-root}: ${RESET[*]}"

# 1. Not while the nightly backups dump every database (backup_stack.sh stops at the first failure)
now=$((10#$(date +%H%M)))
[ $now -ge 755 ] && [ $now -le 835 ] && refuse "the backups run between 08:00 and 08:30"

# 2. Each database is a dev one: its name, and its owner is its own role of the same name
for db in "${DATABASES[@]}"; do
    [[ "$db" =~ ^project_tracking_[a-z0-9]+_dev$ ]] || refuse "'$db' isn't named project_tracking_<name>_dev"
    owner=$(psql_at -c "SELECT pg_get_userbyid(datdba) FROM pg_database WHERE datname = '$db'")
    [ "$owner" = "$db" ] || refuse "'$db' is owned by '${owner:-nobody (no such database)}', not by its own dev role"
done

# 3. The dev API's secrets point at exactly these databases, each with its own role, on this server's PostgreSQL
expected=$(for db in "${DATABASES[@]}"; do echo "$db@10.0.2.2:5432/$db"; done | sort | tr '\n' ' ')
configured=$("${ONEOFF[@]}" --entrypoint python "$IMAGE" -c "
from urllib.parse import urlparse
from project_tracking import create_app
from project_tracking.database import _configured_db_uris
for uri in _configured_db_uris(create_app().config):
    u = urlparse(uri)
    print(f'{u.username}@{u.hostname}:{u.port}{u.path}')" 2>/dev/null | grep '@' | sort | tr '\n' ' ')
[ "$configured" = "$expected" ] || refuse "the dev secrets point at '$configured', expected '$expected'"

# Plan
for db in "${RESET[@]}"; do
    log "  $db: $(psql_at -d "$db" -c "SELECT count(*) FROM project" 2>/dev/null || echo '?') project(s), $(psql_at -d "$db" -c "SELECT count(*) FROM readset" 2>/dev/null || echo '?') readset(s): will be emptied"
done
if [ $YES = 0 ]; then
    log "Dry run: every check passed. Run again with --yes to reset."
    exit 0
fi

# 4. Reset: stop the dev API, recreate the databases empty, create the schemas, start the API (again on any failure)
trap 'log "Failed: starting the dev API again"; "${S[@]}" start "$API_UNIT" || true' ERR
"${S[@]}" stop "$API_UNIT"
for db in "${RESET[@]}"; do
    psql_at -c "DROP DATABASE \"$db\" WITH (FORCE)" -c "CREATE DATABASE \"$db\" OWNER \"$db\" ENCODING 'UTF8'" \
        -c "REVOKE CONNECT ON DATABASE \"$db\" FROM PUBLIC"
    log "  $db recreated empty"
done
"${ONEOFF[@]}" --entrypoint flask "$IMAGE" --app project_tracking init-all-dbs 2>&1 | grep -E "Initializing|initialized" | sed 's/^/  /'
# 5. Read access for the readers again, on the new tables and those the next migrations create
for db in "${RESET[@]}"; do
    for reader in "${READERS[@]}"; do
        psql_at -d "$db" -c "GRANT CONNECT ON DATABASE \"$db\" TO \"$reader\"" -c "GRANT USAGE ON SCHEMA public TO \"$reader\"" \
            -c "GRANT SELECT ON ALL TABLES IN SCHEMA public TO \"$reader\"" \
            -c "ALTER DEFAULT PRIVILEGES FOR ROLE \"$db\" IN SCHEMA public GRANT SELECT ON TABLES TO \"$reader\""
    done
done
log "  read access for ${READERS[*]} granted again"
"${S[@]}" start "$API_UNIT"
trap - ERR
for _ in $(seq 30); do [ "$(curl -s -o /dev/null -w '%{http_code}' http://127.0.0.1:8010/health)" = 200 ] && break; sleep 2; done
for db in "${RESET[@]}"; do
    log "  $db: $(psql_at -d "$db" -c "SELECT count(*) FROM information_schema.tables WHERE table_schema = 'public'") tables, projects: $(psql_at -d "$db" -c "SELECT string_agg(name, ', ') FROM project"), readsets: $(psql_at -d "$db" -c "SELECT count(*) FROM readset"), revision $(psql_at -d "$db" -c "SELECT version_num FROM alembic_version")"
done
log "Done: dev API /health $(curl -s -o /dev/null -w '%{http_code}' http://127.0.0.1:8010/health)."
