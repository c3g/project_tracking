CREATE EXTENSION IF NOT EXISTS pgcrypto;
----------------------------------------------------------------------
--DIGEST CHECKS
----------------------------------------------------------------------
--Test 1: Runs across a random subset of samples (or all samples if adjusted) 
----------------------------------------------------------------------
--Task 1: Pre-run — compute hashes, store in a temp table
----------------------------------------------------------------------
----------------------------------------------------------------------
-- Pre Run Global counts: run before and after dedup script
----------------------------------------------------------------------
DO $$
DECLARE
    file_count BIGINT;
    location_count BIGINT;
    readset_file_count BIGINT;
    job_file_count BIGINT;
BEGIN
    SELECT COUNT(*) INTO file_count FROM file;
    SELECT COUNT(*) INTO location_count FROM location;
    SELECT COUNT(*) INTO readset_file_count FROM readset_file;
    SELECT COUNT(*) INTO job_file_count FROM job_file;

    RAISE NOTICE '--- Pre RunTable counts ---';
    RAISE NOTICE 'file: %', file_count;
    RAISE NOTICE 'location: %', location_count;
    RAISE NOTICE 'readset_file: %', readset_file_count;
    RAISE NOTICE 'job_file: %', job_file_count;
END $$;

SELECT setseed(0.42);  -- any value between -1 and 1

DROP TABLE IF EXISTS validation_samples;

CREATE TEMP TABLE validation_samples AS
SELECT id FROM sample ORDER BY RANDOM() LIMIT 7000;

DROP TABLE IF EXISTS pre_dedup_sample_digest;

CREATE TEMP TABLE pre_dedup_sample_digest (
    sample_id INT,
    digest TEXT,
    hashed_value TEXT,
    snapshot_taken TIMESTAMP DEFAULT NOW()
);

INSERT INTO pre_dedup_sample_digest (sample_id, digest, hashed_value)
SELECT 
    sample_id,
    encode(digest(string_agg(uri, ',' ORDER BY uri), 'md5'), 'hex'),
    string_agg(uri, ',' ORDER BY uri)
FROM (
    SELECT DISTINCT sample_id, uri
    FROM (
        SELECT 
            s.id AS sample_id,
            l.uri AS uri
        FROM sample s
        JOIN readset r ON r.sample_id = s.id
        JOIN readset_file rf ON rf.readset_id = r.id
        JOIN file f ON f.id = rf.file_id
        JOIN location l ON l.file_id = f.id
        --select subset of samples in validation_samples
        WHERE s.id IN (SELECT id FROM validation_samples)

        UNION ALL

        SELECT 
            s.id AS sample_id,
            l.uri AS uri
        FROM sample s
        JOIN readset r ON r.sample_id = s.id
        JOIN readset_job rj ON rj.readset_id = r.id
        JOIN job j ON j.id = rj.job_id
        JOIN job_file jf ON jf.job_id = j.id
        JOIN file f ON f.id = jf.file_id
        JOIN location l ON l.file_id = f.id
        --select subset of samples in validation_samples
        WHERE s.id IN (SELECT id FROM validation_samples)
    ) raw_chain
) deduped
GROUP BY sample_id;

----------------------------------------------------------------------
--Task 2: Run the deduplication script
----------------------------------------------------------------------

-- ================================================
-- Job-level file deduplication inspection table
-- ================================================

DROP TABLE IF EXISTS temp_file_dedup_inspection;

-- Create a temporary table to store inspection results
CREATE TEMP TABLE temp_file_dedup_inspection (
    job_id          INT,
    file_name       TEXT,
    md5sum          TEXT,
    keep_file_id    INT,
    keep_deliverable BOOLEAN,
    keep_type       TEXT,
    keep_state      TEXT,
    keep_creation   TIMESTAMP,
    keep_modification TIMESTAMP,
    keep_deprecated BOOLEAN,
    keep_deleted    BOOLEAN,
    candidate_file_id INT,
    job_file_refs   INT,
    readset_file_refs INT,
    location_refs   INT
) ON COMMIT PRESERVE ROWS;

-- Populate the inspection table
INSERT INTO temp_file_dedup_inspection
SELECT
    fd.job_id,
    fd.name AS file_name,
    fd.md5sum,
    fd.keep_file_id,
    f_keep.deliverable AS keep_deliverable,
    f_keep.type AS keep_type,
    f_keep.state AS keep_state,
    f_keep.creation AS keep_creation,
    f_keep.modification AS keep_modification,
    f_keep.deprecated AS keep_deprecated,
    f_keep.deleted AS keep_deleted,
    file_id AS candidate_file_id,
    (SELECT COUNT(*) FROM job_file jf WHERE jf.file_id = file_id) AS job_file_refs,
    (SELECT COUNT(*) FROM readset_file rf WHERE rf.file_id = file_id) AS readset_file_refs,
    (SELECT COUNT(*) FROM location l WHERE l.file_id = file_id) AS location_refs
FROM (
    SELECT
        jf.job_id,
        f.name,
        f.md5sum,
        (ARRAY_AGG(f.id ORDER BY f.deliverable DESC, f.id ASC))[1] AS keep_file_id,
        ARRAY_AGG(f.id ORDER BY f.id) AS all_file_ids
    FROM file f
    JOIN job_file jf ON jf.file_id = f.id
    GROUP BY jf.job_id, f.name, f.md5sum
    HAVING COUNT(*) > 1
) fd
JOIN file f_keep ON f_keep.id = fd.keep_file_id
-- Expand all_file_ids into one row per candidate file
CROSS JOIN LATERAL unnest(fd.all_file_ids) AS file_id
ORDER BY fd.job_id, fd.name, file_id;

-- Now query the table
SELECT * FROM temp_file_dedup_inspection
ORDER BY job_id, file_name, candidate_file_id;


BEGIN;

DO $$
DECLARE
    dup_group_count BIGINT := 0;
    deleted_rf_conflicts BIGINT := 0;
    remapped_rf BIGINT := 0;
    deleted_jf_conflicts BIGINT := 0;
    remapped_jf BIGINT := 0;
    remapped_loc BIGINT := 0;
    deleted_files BIGINT := 0;
    deleted_jf_final BIGINT := 0;
BEGIN
    RAISE NOTICE '--- Starting file deduplication per job (same name + md5sum) ---';

    ----------------------------------------------------------------------
    -- Step 1: Identify duplicate files per job (same name + md5sum)
    ----------------------------------------------------------------------
    CREATE TEMP TABLE file_dupes_per_job ON COMMIT DROP AS
    SELECT
        jf.job_id,
        f.name,
        f.md5sum,
        -- Prefer deliverable = true, then lowest id
        (ARRAY_AGG(f.id ORDER BY f.deliverable DESC, f.id ASC))[1] AS keep_file_id,
        ARRAY_AGG(f.id ORDER BY f.id) AS all_file_ids
    FROM file f
    JOIN job_file jf ON jf.file_id = f.id
    GROUP BY jf.job_id, f.name, f.md5sum
    HAVING COUNT(*) > 1;

    SELECT COUNT(*) INTO dup_group_count FROM file_dupes_per_job;
    RAISE NOTICE 'Duplicate file groups found (per job): %', dup_group_count;

    IF dup_group_count = 0 THEN
        RAISE NOTICE 'No duplicate groups found; nothing to do.';
        RETURN;
    END IF;

    ----------------------------------------------------------------------
    -- Step 2: Delete conflicting job_file links (pre-delete)
    -- If a job already links to the keep_file_id, delete other job_file rows
    ----------------------------------------------------------------------
    DELETE FROM job_file jf
    USING file_dupes_per_job fd
    WHERE jf.job_id = fd.job_id
      AND jf.file_id = ANY(fd.all_file_ids)
      AND jf.file_id <> fd.keep_file_id
      AND EXISTS (
          SELECT 1 FROM job_file jf2
          WHERE jf2.job_id = jf.job_id
            AND jf2.file_id = fd.keep_file_id
      );
    GET DIAGNOSTICS deleted_jf_conflicts = ROW_COUNT;
    IF deleted_jf_conflicts > 0 THEN
        RAISE NOTICE 'Deleted conflicting job_file links (pre-delete): %', deleted_jf_conflicts;
    END IF;

    ----------------------------------------------------------------------
    -- Step 3: Remap remaining job_file rows to keep_file_id (batched)
    ----------------------------------------------------------------------
    RAISE NOTICE 'Starting batched remap for job_file...';

    -- Build mapping of (job_id, old_file_id) -> keep_file_id
    CREATE TEMP TABLE job_file_remap ON COMMIT DROP AS
    SELECT DISTINCT jf.job_id, jf.file_id AS old_file_id, fd.keep_file_id
    FROM job_file jf
    JOIN file_dupes_per_job fd
      ON jf.job_id = fd.job_id
     AND jf.file_id = ANY(fd.all_file_ids)
     AND jf.file_id <> fd.keep_file_id;

    -- Indexes to speed joins (temporary)
    CREATE INDEX idx_jfr_old_file_id ON job_file_remap (old_file_id);
    CREATE INDEX idx_jfr_job_keep ON job_file_remap (job_id, keep_file_id);
    CREATE INDEX IF NOT EXISTS idx_jf_job_file_id ON job_file (job_id, file_id);

    -- Batched updates to avoid long locks and to be conflict-aware.
    LOOP
        WITH one_update AS (
            SELECT DISTINCT ON (jf.job_id, jfr.keep_file_id)
                   jf.ctid AS target_ctid,
                   jfr.keep_file_id
            FROM job_file jf
            JOIN job_file_remap jfr
              ON jf.job_id = jfr.job_id
             AND jf.file_id = jfr.old_file_id
            WHERE NOT EXISTS (
                SELECT 1 FROM job_file jf2
                WHERE jf2.job_id = jf.job_id
                  AND jf2.file_id = jfr.keep_file_id
            )
            ORDER BY jf.job_id, jfr.keep_file_id
            LIMIT 50000  -- batch size; tune if you have more RAM/IO
        )
        UPDATE job_file jf
        SET file_id = ou.keep_file_id
        FROM one_update ou
        WHERE jf.ctid = ou.target_ctid;

        GET DIAGNOSTICS remapped_jf = ROW_COUNT;
        EXIT WHEN remapped_jf = 0;
        RAISE NOTICE 'Remapped % job_file rows in batch...', remapped_jf;
    END LOOP;

    RAISE NOTICE 'Finished job_file remap (batched)';

    ----------------------------------------------------------------------
    -- Step 4: Remap readset_file rows based on job-level canonicalization
    -- Only remap a readset_file when the readset is linked to the job.
    -- First remove conflicting readset_file links (pre-delete).
    ----------------------------------------------------------------------
    RAISE NOTICE 'Starting safe remap for readset_file based on job dedupe...';

    -- Delete readset_file rows where the readset already links to the keep_file_id
    -- and the deleted row would duplicate that link.
    DELETE FROM readset_file rf
    USING (
        SELECT rf_inner.readset_id,
               rf_inner.file_id AS old_file_id,
               fd.keep_file_id
        FROM readset_file rf_inner
        JOIN readset_job rj
          ON rj.readset_id = rf_inner.readset_id
        JOIN file_dupes_per_job fd
          ON fd.job_id = rj.job_id
        WHERE rf_inner.file_id = ANY(fd.all_file_ids)
          AND rf_inner.file_id <> fd.keep_file_id
          -- keep candidates that would be duplicates because keep_file_id already exists for the same readset
          AND EXISTS (
            SELECT 1 FROM readset_file rf2
            WHERE rf2.readset_id = rf_inner.readset_id
              AND rf2.file_id = fd.keep_file_id
          )
    ) sub
    WHERE rf.readset_id = sub.readset_id
      AND rf.file_id = sub.old_file_id;
    GET DIAGNOSTICS deleted_rf_conflicts = ROW_COUNT;
    IF deleted_rf_conflicts > 0 THEN
        RAISE NOTICE 'Deleted conflicting readset_file links (pre-delete): %', deleted_rf_conflicts;
    END IF;

    -- Now remap remaining readset_file rows to the keep_file_id (one update).
    -- Only remap where the readset is linked to the job owning the duplicate group.
    UPDATE readset_file rf
    SET file_id = fd.keep_file_id
    FROM file_dupes_per_job fd
    JOIN readset_job rj ON rj.job_id = fd.job_id
    WHERE rf.readset_id = rj.readset_id
      AND rf.file_id = ANY(fd.all_file_ids)
      AND rf.file_id <> fd.keep_file_id;
    GET DIAGNOSTICS remapped_rf = ROW_COUNT;
    IF remapped_rf > 0 THEN
        RAISE NOTICE 'Remapped readset_file rows: %', remapped_rf;
    ELSE
        RAISE NOTICE 'No readset_file rows required remap';
    END IF;

    ----------------------------------------------------------------------
    -- Step 5: Remap location rows (global; file_id -> keep_file_id)
    ----------------------------------------------------------------------
    UPDATE location l
    SET file_id = fd.keep_file_id
    FROM file_dupes_per_job fd
    WHERE l.file_id = ANY(fd.all_file_ids)
      AND l.file_id <> fd.keep_file_id;
    GET DIAGNOSTICS remapped_loc = ROW_COUNT;
    IF remapped_loc > 0 THEN
        RAISE NOTICE 'Remapped location rows: %', remapped_loc;
    ELSE
        RAISE NOTICE 'No location rows required remap';
    END IF;

    ----------------------------------------------------------------------
    -- Step 6: Final sanity delete for any remaining job_file duplicates
    -- (if any extra rows remain that would conflict with a final remap)
    ----------------------------------------------------------------------
    RAISE NOTICE 'Performing final sanity deletions for job_file if needed...';

    -- Build table of all (job_id, keep_file_id) that should remain
    WITH keep_pairs AS (
        SELECT job_id, keep_file_id FROM file_dupes_per_job
    ),
    to_delete AS (
        SELECT jf.job_id, jf.file_id
        FROM job_file jf
        JOIN file_dupes_per_job fd ON jf.job_id = fd.job_id
        WHERE jf.file_id = ANY(fd.all_file_ids)
          AND jf.file_id <> fd.keep_file_id
          -- if there exists another row for same job with keep_file_id, this jf is redundant
          AND EXISTS (
              SELECT 1 FROM job_file jf2
              WHERE jf2.job_id = jf.job_id
                AND jf2.file_id = fd.keep_file_id
          )
    )
    DELETE FROM job_file jf
    USING to_delete td
    WHERE jf.job_id = td.job_id
      AND jf.file_id = td.file_id;
    GET DIAGNOSTICS deleted_jf_final = ROW_COUNT;
    IF deleted_jf_final > 0 THEN
        RAISE NOTICE 'Deleted % redundant job_file rows during final sanity pass', deleted_jf_final;
    ELSE
        RAISE NOTICE 'No redundant job_file rows needed deletion';
    END IF;

    ----------------------------------------------------------------------
    -- Step 7: Delete redundant file rows (safe)
    ----------------------------------------------------------------------
    RAISE NOTICE 'Deleting duplicate file rows that are no longer referenced...';
    WITH dup_files AS (
        SELECT unnest(all_file_ids) AS file_id, keep_file_id
        FROM file_dupes_per_job
    )
    DELETE FROM file f
    WHERE f.id IN (
        SELECT df.file_id
        FROM dup_files df
        WHERE df.file_id <> df.keep_file_id
          AND NOT EXISTS (SELECT 1 FROM readset_file rf WHERE rf.file_id = df.file_id)
          AND NOT EXISTS (SELECT 1 FROM job_file jf WHERE jf.file_id = df.file_id)
          AND NOT EXISTS (SELECT 1 FROM location l WHERE l.file_id = df.file_id)
    );
    GET DIAGNOSTICS deleted_files = ROW_COUNT;
    IF deleted_files > 0 THEN
        RAISE NOTICE 'Deleted % duplicate file rows safely', deleted_files;
    ELSE
        RAISE NOTICE 'No duplicate file rows could be deleted (still referenced)';
    END IF;

    ----------------------------------------------------------------------
    -- Step 8: Summary
    ----------------------------------------------------------------------
    RAISE NOTICE '--- File deduplication per job complete ---';
    RAISE NOTICE 'Groups=% , readset_file deleted=% , readset_file remapped=% , job_file remapped (batched)=% , location remapped=% , job_file final deleted=% , files deleted=%',
        dup_group_count, deleted_rf_conflicts, remapped_rf, remapped_jf, remapped_loc, deleted_jf_final, deleted_files;

END $$;

COMMIT;
----------------------------------------------------------------------
--DIGEST CHECKS
----------------------------------------------------------------------
--Test 1: Runs across a random subset of samples (or all samples if adjusted) 
----------------------------------------------------------------------
--Task 3:Post-run — compute the same hashes again
----------------------------------------------------------------------
----------------------------------------------------------------------
-- Post Run Global counts: run before and after dedup script
----------------------------------------------------------------------
DO $$
DECLARE
    file_count BIGINT;
    location_count BIGINT;
    readset_file_count BIGINT;
    job_file_count BIGINT;
BEGIN
    SELECT COUNT(*) INTO file_count FROM file;
    SELECT COUNT(*) INTO location_count FROM location;
    SELECT COUNT(*) INTO readset_file_count FROM readset_file;
    SELECT COUNT(*) INTO job_file_count FROM job_file;

    RAISE NOTICE '--- Post RunTable counts ---';
    RAISE NOTICE 'file: %', file_count;
    RAISE NOTICE 'location: %', location_count;
    RAISE NOTICE 'readset_file: %', readset_file_count;
    RAISE NOTICE 'job_file: %', job_file_count;
END $$;

DROP TABLE IF EXISTS post_dedup_sample_digest;

CREATE TEMP TABLE post_dedup_sample_digest (
    sample_id INT,
    digest TEXT,
    hashed_value TEXT,
    snapshot_taken TIMESTAMP DEFAULT NOW()
);

INSERT INTO post_dedup_sample_digest (sample_id, digest, hashed_value)
SELECT 
    sample_id,
    encode(digest(string_agg(uri, ',' ORDER BY uri), 'md5'), 'hex'),
    string_agg(uri, ',' ORDER BY uri)
FROM (
    SELECT DISTINCT sample_id, uri
    FROM (
        SELECT 
            s.id AS sample_id,
            l.uri AS uri
        FROM sample s
        JOIN readset r ON r.sample_id = s.id
        JOIN readset_file rf ON rf.readset_id = r.id
        JOIN file f ON f.id = rf.file_id
        JOIN location l ON l.file_id = f.id
        --select subset of samples in validation_samples
        WHERE s.id IN (SELECT id FROM validation_samples)

        UNION ALL

        SELECT 
            s.id AS sample_id,
            l.uri AS uri
        FROM sample s
        JOIN readset r ON r.sample_id = s.id
        JOIN readset_job rj ON rj.readset_id = r.id
        JOIN job j ON j.id = rj.job_id
        JOIN job_file jf ON jf.job_id = j.id
        JOIN file f ON f.id = jf.file_id
        JOIN location l ON l.file_id = f.id
        --select subset of samples in validation_samples
        WHERE s.id IN (SELECT id FROM validation_samples)
    ) raw_chain
) deduped
GROUP BY sample_id;

----------------------------------------------------------------------
-- Task 4: Compare pre/post URI manifests (semantic integrity check)
----------------------------------------------------------------------
DO $$
DECLARE
    rec RECORD;
    changed_count BIGINT := 0;
    removed_uris TEXT;
    added_uris TEXT;
    flagged_samples TEXT := '';
    affected_files TEXT;
    flagged_details TEXT := '';
    all_affected_files TEXT := '';
BEGIN
    FOR rec IN (
        SELECT
            COALESCE(pre.sample_id, post.sample_id) AS sample_id,
            pre.digest AS pre_digest,
            post.digest AS post_digest,
            pre.hashed_value AS pre_uris,
            post.hashed_value AS post_uris
        FROM pre_dedup_sample_digest pre
        FULL OUTER JOIN post_dedup_sample_digest post
            ON pre.sample_id = post.sample_id
    )
    LOOP
        ------------------------------------------------------------------
        -- Case 1: sample missing after dedup (data loss)
        ------------------------------------------------------------------
        IF rec.post_digest IS NULL THEN
            changed_count := changed_count + 1;
            flagged_samples := flagged_samples || rec.sample_id || ', ';
            flagged_details := flagged_details ||
                format('  Sample %s: lost all URI associations (no file IDs recoverable)', rec.sample_id) || E'\n';
            RAISE NOTICE
                'VALIDATION FAILURE: Sample % lost all URI associations after dedup',
                rec.sample_id;

        ------------------------------------------------------------------
        -- Case 2: sample newly appears after dedup (unexpected growth)
        ------------------------------------------------------------------
        ELSIF rec.pre_digest IS NULL THEN
            changed_count := changed_count + 1;
            flagged_samples := flagged_samples || rec.sample_id || ', ';
            flagged_details := flagged_details ||
                format('  Sample %s: gained URI associations (no file IDs recoverable)', rec.sample_id) || E'\n';
            RAISE NOTICE
                'VALIDATION FAILURE: Sample % gained URI associations after dedup',
                rec.sample_id;

        ------------------------------------------------------------------
        -- Case 3: both exist but URI set changed
        ------------------------------------------------------------------
        ELSIF rec.pre_digest IS DISTINCT FROM rec.post_digest THEN
            changed_count := changed_count + 1;
            flagged_samples := flagged_samples || rec.sample_id || ', ';
            RAISE NOTICE
                'VALIDATION FAILURE: Sample % URI set changed',
                rec.sample_id;

            SELECT string_agg(uri, ', ' ORDER BY uri)
            INTO removed_uris
            FROM unnest(string_to_array(rec.pre_uris, ',')) AS uri
            WHERE uri NOT IN (
                SELECT unnest(string_to_array(rec.post_uris, ','))
            );

            SELECT string_agg(uri, ', ' ORDER BY uri)
            INTO added_uris
            FROM unnest(string_to_array(rec.post_uris, ',')) AS uri
            WHERE uri NOT IN (
                SELECT unnest(string_to_array(rec.pre_uris, ','))
            );

            IF removed_uris IS NOT NULL THEN
                RAISE NOTICE 'REMOVED URIs (in pre, missing post): %', removed_uris;
            END IF;
            IF added_uris IS NOT NULL THEN
                RAISE NOTICE 'ADDED URIs (in post, missing pre): %', added_uris;
            END IF;

            -- Fetch file IDs associated with the differing URIs for this sample
            SELECT string_agg(DISTINCT f.id::TEXT, ', ' ORDER BY f.id::TEXT)
            INTO affected_files
            FROM file f
            JOIN location l ON l.file_id = f.id
            WHERE l.uri = ANY(
                SELECT uri FROM unnest(string_to_array(rec.pre_uris, ',')) AS uri
                WHERE uri NOT IN (SELECT unnest(string_to_array(rec.post_uris, ',')))
                UNION
                SELECT uri FROM unnest(string_to_array(rec.post_uris, ',')) AS uri
                WHERE uri NOT IN (SELECT unnest(string_to_array(rec.pre_uris, ',')))
            );

            IF affected_files IS NOT NULL THEN
                RAISE NOTICE 'AFFECTED file IDs for sample %: %', rec.sample_id, affected_files;
                flagged_details := flagged_details ||
                    format('  Sample %s: file IDs [%s]', rec.sample_id, affected_files) || E'\n';
                -- Accumulate into the global distinct file ID list
                all_affected_files := all_affected_files || affected_files || ', ';
            ELSE
                flagged_details := flagged_details ||
                    format('  Sample %s: URI set changed but no matching file IDs found', rec.sample_id) || E'\n';
            END IF;

        END IF;
    END LOOP;

    ----------------------------------------------------------------------
    -- Final summary
    ----------------------------------------------------------------------
    IF changed_count = 0 THEN
        RAISE NOTICE
            'Validation PASSED — URI sets unchanged across all validated samples (% samples)',
            (SELECT COUNT(*) FROM pre_dedup_sample_digest);
    ELSE
        RAISE NOTICE
            'Validation FAILED — % samples had URI set changes. Flagged sample IDs: %',
            changed_count,
            rtrim(flagged_samples, ', ');

        RAISE NOTICE E'\n--- Per-sample breakdown ---\n%', flagged_details;

        -- Deduplicate the accumulated file IDs for the final report
        RAISE NOTICE '--- All distinct problematic file IDs ---: %',
            (
                SELECT string_agg(DISTINCT file_id, ', ' ORDER BY file_id)
                FROM unnest(string_to_array(rtrim(all_affected_files, ', '), ', ')) AS file_id
                WHERE file_id <> ''
            );
    END IF;
END $$;