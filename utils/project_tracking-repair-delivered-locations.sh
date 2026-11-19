#!/bin/bash
# Repair the deliveries issue #81 left behind: a rerun delivered to a bucket key another file of the same readset (same
# name) already owned left the key on that file, and the delivering file DELIVERED without any live location.
#   sudo bash project_tracking-repair-delivered-locations.sh <database>         dry run: does everything, shows the
#                                                                               counts, writes the review list, rolls back
#   sudo bash project_tracking-repair-delivered-locations.sh <database> --yes   the same, committed
# For each bucket key (location endpoint c3g-data-repos) owned by a file with such same-named files left without location,
# the bucket holds the bytes of the file delivered last (the last upload wins):
#   - supersede: the owner was delivered last. It keeps the key; the files without location are deprecated.
#   - move: one of the files without location was delivered last, and it is the newest file. The key goes to it; the
#     other files without location, and the previous owner unless it keeps another live location, are deprecated.
#   - review: nothing changes when the file delivered last is older than the owner (an older run delivered over a newer
#     one?), or a file of the group has the same name in several keys. They are listed in the review CSV.
# A deprecated file gets extra_metadata duplicate_of (same md5sum) or superseded_by (different or unknown md5sum) set to
# the key's owner, and repair = "issue #81". This is what the 4.0.0 ingest now does.
# It holds the API's write lock (database.WRITE_LOCK_KEY) for its whole transaction: API writes to the database wait.
# Run it again any time (e.g. after md5sums are backfilled): it only finds what is still broken.
set -euo pipefail

DB=${1:-}
YES=0
[ "${2:-}" = --yes ] && YES=1
[ -n "$DB" ] && { [ $# = 1 ] || [ $YES = 1 ]; } || { echo "Usage: sudo bash $(basename "$0") <database> [--yes]" >&2; exit 2; }
# PT_PSQL: the psql command (tests run it in a container); by default the server's postgres user
read -r -a PSQL <<< "${PT_PSQL:-sudo -u postgres psql}"
[ -n "${PT_PSQL:-}" ] || [ "$(id -u)" = 0 ] || { echo "Run it with sudo." >&2; exit 1; }
[ -n "${PT_PSQL:-}" ] || cd /tmp
CSV=${PT_REPAIR_CSV:-$HOME/repair-delivered-locations-$DB-$(date +%Y%m%d-%H%M%S).csv}
END=$([ $YES = 1 ] && echo COMMIT || echo ROLLBACK)
OUT=$(mktemp)
trap 'rm -f "$OUT"' EXIT

echo "$DB: $([ $YES = 1 ] && echo "repair" || echo "dry run, rolled back at the end")"
"${PSQL[@]}" -X -q -v ON_ERROR_STOP=1 -d "$DB" > "$OUT" <<SQL
\pset footer off
\pset format aligned
BEGIN;
SELECT pg_advisory_xact_lock(8103206596171625573) AS locked \gset

-- Live DELIVERED files without any live location
CREATE TEMP TABLE orphan ON COMMIT DROP AS
SELECT f.id, f.name, f.md5sum, f.creation FROM file f
WHERE f.state = 'DELIVERED' AND NOT f.deleted AND NOT f.deprecated
  AND NOT EXISTS (SELECT 1 FROM location l WHERE l.file_id = f.id AND NOT l.deleted);

-- Last delivery of each file
CREATE TEMP TABLE delivered ON COMMIT DROP AS
SELECT jf.file_id, max(coalesce(j.stop, j.start, j.creation)) AS at
FROM job_file jf JOIN job j ON j.id = jf.job_id JOIN operation op ON op.id = j.operation_id
WHERE op.name = 'delivery' GROUP BY jf.file_id;

-- A live bucket key, its owner and an orphan of the same name sharing a readset with the owner
CREATE TEMP TABLE pair ON COMMIT DROP AS
SELECT DISTINCT l.id AS location_id, l.uri, w.id AS owner_id, o.id AS orphan_id
FROM orphan o
JOIN readset_file ra ON ra.file_id = o.id
JOIN readset_file rb ON rb.readset_id = ra.readset_id AND rb.file_id <> o.id
JOIN file w ON w.id = rb.file_id AND w.name = o.name AND NOT w.deleted
JOIN location l ON l.file_id = w.id AND l.endpoint = 'c3g-data-repos' AND NOT l.deleted;

-- Per key, the orphan delivered last (then the newest, then the highest id) and the decision
CREATE TEMP TABLE grp ON COMMIT DROP AS
WITH ranked AS (
    SELECT p.*, row_number() OVER (PARTITION BY p.location_id
                                   ORDER BY d.at DESC NULLS LAST, o.creation DESC, o.id DESC) AS rank,
           count(*) OVER (PARTITION BY p.location_id) AS orphans,
           count(*) OVER (PARTITION BY p.orphan_id) AS keys
    FROM pair p JOIN orphan o ON o.id = p.orphan_id LEFT JOIN delivered d ON d.file_id = p.orphan_id
)
SELECT r.location_id, r.uri, r.owner_id, r.orphan_id AS new_id, r.orphans,
       w.creation AS owner_created, dw.at AS owner_delivered, o.creation AS new_created, dn.at AS new_delivered,
       CASE WHEN EXISTS (SELECT 1 FROM ranked x WHERE x.location_id = r.location_id AND x.keys > 1)
                THEN 'review: a file of the group has the same name in several keys'
            WHEN dn.at IS NOT NULL AND dw.at IS NOT NULL AND dn.at <= dw.at THEN 'supersede'
            WHEN o.creation < w.creation THEN 'review: the file delivered last is older than the owner'
            ELSE 'move' END AS decision
FROM ranked r
JOIN file w ON w.id = r.owner_id
JOIN orphan o ON o.id = r.orphan_id
LEFT JOIN delivered dw ON dw.file_id = r.owner_id
LEFT JOIN delivered dn ON dn.file_id = r.orphan_id
WHERE r.rank = 1;

-- Files to deprecate, with the key's owner they point to: the orphans of a superseded key, the other orphans of a
-- moved key, then (after the move) the previous owners left without live location
CREATE TEMP TABLE superseded ON COMMIT DROP AS
SELECT DISTINCT p.orphan_id AS file_id, g.owner_id AS new_id FROM pair p JOIN grp g USING (location_id)
WHERE g.decision = 'supersede'
UNION
SELECT DISTINCT p.orphan_id, g.new_id FROM pair p JOIN grp g USING (location_id)
WHERE g.decision = 'move' AND p.orphan_id <> g.new_id;

\echo == Before
SELECT (SELECT count(*) FROM orphan) AS "DELIVERED without live location",
       (SELECT count(*) FROM pair) AS "pairs with a same-named key owner",
       (SELECT count(*) FROM grp) AS "keys",
       (SELECT count(*) FROM grp WHERE decision = 'move') AS "keys to move",
       (SELECT count(*) FROM grp WHERE decision = 'supersede') AS "keys kept (owner delivered last)",
       (SELECT count(*) FROM grp WHERE orphans > 1 AND decision IN ('move', 'supersede')) AS "of which with several orphans",
       (SELECT count(*) FROM grp WHERE decision LIKE 'review%') AS "keys to review";
SELECT decision, count(*) AS keys FROM grp GROUP BY decision ORDER BY decision;

UPDATE location l SET file_id = g.new_id, modification = now()
FROM grp g WHERE l.id = g.location_id AND g.decision = 'move';

INSERT INTO superseded
SELECT DISTINCT ON (g.owner_id) g.owner_id, g.new_id FROM grp g
WHERE g.decision = 'move'
  AND NOT EXISTS (SELECT 1 FROM location l WHERE l.file_id = g.owner_id AND NOT l.deleted AND NOT l.deprecated)
  AND NOT EXISTS (SELECT 1 FROM file f WHERE f.id = g.owner_id AND (f.deprecated OR f.deleted))
ORDER BY g.owner_id, g.new_delivered DESC NULLS LAST;

UPDATE file f SET deprecated = true, modification = now(),
       -- extra_metadata is SQL NULL, JSON null or an object
       extra_metadata = (CASE WHEN jsonb_typeof(f.extra_metadata) = 'object' THEN f.extra_metadata ELSE '{}'::jsonb END) || jsonb_build_object(
           CASE WHEN f.md5sum IS NOT NULL AND f.md5sum = n.md5sum THEN 'duplicate_of' ELSE 'superseded_by' END, s.new_id,
           'repair', 'issue #81')
FROM superseded s JOIN file n ON n.id = s.new_id
WHERE f.id = s.file_id;

\echo == After
SELECT (SELECT count(*) FROM file f WHERE f.state = 'DELIVERED' AND NOT f.deleted AND NOT f.deprecated
          AND NOT EXISTS (SELECT 1 FROM location l WHERE l.file_id = f.id AND NOT l.deleted))
           AS "DELIVERED without live location",
       (SELECT count(*) FROM grp WHERE decision = 'move') AS "keys moved",
       (SELECT count(*) FROM grp WHERE decision = 'supersede') AS "keys kept",
       (SELECT count(*) FROM superseded) AS "files deprecated",
       (SELECT count(*) FROM grp g WHERE g.decision = 'move' AND EXISTS (SELECT 1 FROM location l
           WHERE l.file_id = g.owner_id AND NOT l.deleted AND NOT l.deprecated)) AS "owners kept live (other location)";

\echo == Review CSV
\copy (SELECT location_id, uri, owner_id, owner_created, owner_delivered, new_id AS delivered_last_id, new_created AS delivered_last_created, new_delivered AS delivered_last_at, orphans, decision FROM grp WHERE decision LIKE 'review%' ORDER BY uri) TO STDOUT WITH CSV HEADER
\echo == End
$END;
SQL

sed -n '/^== Review CSV$/q;p' "$OUT"
sed -n '/^== Review CSV$/,/^== End$/p' "$OUT" | sed '1d;$d' > "$CSV"
echo "Review list: $CSV ($(( $(wc -l < "$CSV") - 1 )) keys)"
echo "$([ $YES = 1 ] && echo "Committed." || echo "Rolled back: nothing changed. Run again with --yes.")"
