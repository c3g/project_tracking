#!/bin/bash
# Copy the production database oauth2-db (release 3.1.6) into project_tracking_moh, owned by its own role of the same
# name (the naming of the dev databases). oauth2-db is only read: it stays as it is, the way back to 3.1.6.
# Runbook: "project_tracking production switch to COmanage". Before it: the role and the empty database exist (runbook).
#   sudo project_tracking-prod-copy-db         dry run: checks everything, shows what it would do
#   sudo project_tracking-prod-copy-db --yes   does it: stops the 3.1.6 API, copies, compares every table's row count,
#                                              gives pgweb its read access; the 3.1.6 API stays stopped (the 4.0.0 pod
#                                              comes next), and is started again if anything fails or the run is cut
#   sudo project_tracking-prod-copy-db --rehearsal [--yes]
#                                              the same into project_tracking_moh_rehearsal (also owned by role
#                                              project_tracking_moh), with the 3.1.6 API left running: times the copy
#                                              on the real data before the day; drop that database afterwards
# Run it in tmux: a copy of 5.8 GB outlives a dropped SSH session.
set -euo pipefail

SOURCE=oauth2-db
ROLE=project_tracking_moh
TARGET=$ROLE
SOURCE_REVISION=64a560017524
OLD_API_UNIT=podman-project_tracking-api.service
READERS=(pgweb)

log() { echo "$*"; logger -t project_tracking-prod-copy-db -- "$*"; }
refuse() { log "REFUSED, nothing changed: $*"; exit 1; }

[ "$(id -u)" = 0 ] || { echo "Run it with sudo." >&2; exit 1; }
cd /tmp
YES=0
REHEARSAL=0
for arg in "$@"; do
    case "$arg" in
        --yes) YES=1 ;;
        --rehearsal) REHEARSAL=1; TARGET=${ROLE}_rehearsal ;;
        *) echo "Usage: sudo $(basename "$0") [--rehearsal] [--yes]" >&2; exit 2 ;;
    esac
done

G=$(id -u genome)
S=(sudo -u genome "XDG_RUNTIME_DIR=/run/user/$G" "DBUS_SESSION_BUS_ADDRESS=unix:path=/run/user/$G/bus" systemctl --user)
psql_at() { sudo -u postgres psql -v ON_ERROR_STOP=1 -Atq "$@"; }
# Exact row count of every table of a database, one "table count" line each, sorted
row_counts() {
    psql_at -d "$1" -c "SELECT tablename || ' ' || (xpath('/row/n/text()',
                            query_to_xml(format('SELECT count(*) AS n FROM public.%I', tablename), false, true, '')))[1]
                        FROM pg_tables WHERE schemaname = 'public' ORDER BY tablename"
}

log "Start ($([ $YES = 1 ] && echo "for real" || echo "dry run")$([ $REHEARSAL = 1 ] && echo ", rehearsal")) by ${SUDO_USER:-root}"

# 1. Not while the nightly backups dump every database
now=$((10#$(date +%H%M)))
[ $now -ge 755 ] && [ $now -le 835 ] && refuse "the backups run between 08:00 and 08:30"

# 2. The source is the 3.1.6 database
revision=$(psql_at -d "$SOURCE" -c "SELECT version_num FROM alembic_version" 2>/dev/null) \
    || refuse "no database '$SOURCE', or it has no alembic_version"
[ "$revision" = "$SOURCE_REVISION" ] || refuse "'$SOURCE' is at revision '$revision', not $SOURCE_REVISION (3.1.6)"

# 3. The target exists, is owned by the role, which isn't a superuser, and is empty
owner=$(psql_at -c "SELECT pg_get_userbyid(datdba) FROM pg_database WHERE datname = '$TARGET'")
[ "$owner" = "$ROLE" ] || refuse "'$TARGET' is owned by '${owner:-nobody (no such database)}', not by role $ROLE"
[ "$(psql_at -c "SELECT rolsuper FROM pg_roles WHERE rolname = '$ROLE'")" = f ] || refuse "role '$ROLE' is a superuser"
tables=$(psql_at -d "$TARGET" -c "SELECT count(*) FROM pg_tables WHERE schemaname = 'public'")
[ "$tables" = 0 ] || refuse "'$TARGET' isn't empty: $tables table(s)"
for reader in "${READERS[@]}"; do
    [ "$(psql_at -c "SELECT count(*) FROM pg_roles WHERE rolname = '$reader'")" = 1 ] || refuse "no role '$reader'"
done

# Plan
log "  $SOURCE: $(psql_at -c "SELECT pg_size_pretty(pg_database_size('$SOURCE'))"), revision $revision, projects: $(psql_at -d "$SOURCE" -c "SELECT string_agg(name, ', ') FROM project"), readsets: $(psql_at -d "$SOURCE" -c "SELECT count(*) FROM readset")"
if [ $REHEARSAL = 1 ]; then
    log "  $OLD_API_UNIT: $("${S[@]}" is-active "$OLD_API_UNIT" || true), left running (rehearsal: writes during the copy may differ the counts)"
else
    log "  $OLD_API_UNIT: $("${S[@]}" is-active "$OLD_API_UNIT" || true), will be stopped (writes stop during the copy)"
fi
log "  $TARGET: empty, will get a copy owned by role $ROLE; read access for: ${READERS[*]}"
log "  free space on the data disk: $(df -h --output=avail "$(psql_at -c 'SHOW data_directory')" | tail -1 | tr -d ' ')"
if [ $YES = 0 ]; then
    log "Dry run: every check passed. Run again with --yes to copy."
    exit 0
fi

# 4. Copy: no writes during it. Objects are created as the target role (--role), without the source's owners and grants
started=$(date +%s)
back() {
    log "Failed or cut: $TARGET may be half filled (empty it before a new try)"
    [ $REHEARSAL = 1 ] || { log "  starting the 3.1.6 API again"; "${S[@]}" start "$OLD_API_UNIT" || true; }
    exit 1
}
trap back ERR INT TERM HUP
if [ $REHEARSAL = 0 ]; then
    "${S[@]}" stop "$OLD_API_UNIT"
    log "  $OLD_API_UNIT stopped, copying"
fi
sudo -u postgres pg_dump -Fc --no-owner --no-acl -d "$SOURCE" \
    | sudo -u postgres pg_restore --exit-on-error --no-owner --no-acl --role="$ROLE" -d "$TARGET"
log "  copied in $(( $(date +%s) - started )) s"

# 5. Nothing lost: the same row count in every table
source_counts=$(row_counts "$SOURCE")
target_counts=$(row_counts "$TARGET")
if [ -z "$source_counts" ] || [ "$source_counts" != "$target_counts" ]; then
    diff <(echo "$source_counts") <(echo "$target_counts") > /tmp/project_tracking-prod-copy-db.diff || true
    log "Row counts differ, or none were read (/tmp/project_tracking-prod-copy-db.diff)"
    false
fi
log "  row counts identical in all $(echo "$target_counts" | wc -l) tables ($(echo "$target_counts" | awk '{n += $2} END {print n}') rows)"

# 6. Only its role connects, and the readers read, tables created later by migrations included
psql_at -d "$TARGET" -c "REVOKE CONNECT ON DATABASE \"$TARGET\" FROM PUBLIC"
for reader in "${READERS[@]}"; do
    psql_at -d "$TARGET" -c "GRANT CONNECT ON DATABASE \"$TARGET\" TO \"$reader\"" \
        -c "GRANT USAGE ON SCHEMA public TO \"$reader\"" \
        -c "GRANT SELECT ON ALL TABLES IN SCHEMA public TO \"$reader\"" \
        -c "ALTER DEFAULT PRIVILEGES FOR ROLE \"$ROLE\" IN SCHEMA public GRANT SELECT ON TABLES TO \"$reader\""
done
trap - ERR INT TERM HUP
log "Done in $(( $(date +%s) - started )) s: $TARGET at revision $(psql_at -d "$TARGET" -c "SELECT version_num FROM alembic_version"), $SOURCE untouched, $OLD_API_UNIT $([ $REHEARSAL = 1 ] && echo "still running" || echo "stopped"). Next: $([ $REHEARSAL = 1 ] && echo "upgrade $TARGET with 4.0.0 (runbook), then drop it" || echo "start the 4.0.0 pod (it upgrades $TARGET)")."
