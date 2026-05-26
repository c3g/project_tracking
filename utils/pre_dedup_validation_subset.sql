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
SELECT id FROM sample ORDER BY RANDOM() LIMIT 100;

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

