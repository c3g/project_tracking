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
            RAISE NOTICE
                'VALIDATION FAILURE: Sample % lost all URI associations after dedup',
                rec.sample_id;

        ------------------------------------------------------------------
        -- Case 2: sample newly appears after dedup (unexpected growth)
        ------------------------------------------------------------------
        ELSIF rec.pre_digest IS NULL THEN
            changed_count := changed_count + 1;
            flagged_samples := flagged_samples || rec.sample_id || ', ';
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
                -- URIs that appear in pre but not post, or post but not pre
                SELECT uri FROM unnest(string_to_array(rec.pre_uris, ',')) AS uri
                WHERE uri NOT IN (SELECT unnest(string_to_array(rec.post_uris, ',')))
                UNION
                SELECT uri FROM unnest(string_to_array(rec.post_uris, ',')) AS uri
                WHERE uri NOT IN (SELECT unnest(string_to_array(rec.pre_uris, ',')))
            );

            IF affected_files IS NOT NULL THEN
                RAISE NOTICE 'AFFECTED file IDs for sample %: %', rec.sample_id, affected_files;
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
    END IF;
END $$;