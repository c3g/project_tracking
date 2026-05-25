----------------------------------------------------------------------
--DIGEST CHECKS
----------------------------------------------------------------------
--Test 1: Runs across a random subset of samples (or all samples if adjusted) 
----------------------------------------------------------------------
--Task 1: Pre-run — compute hashes, store in a temp table
----------------------------------------------------------------------

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
    encode(digest(string_agg(chain_row, ',' ORDER BY readset_id, job_id, file_id, location_id), 'md5'), 'hex'),
    string_agg(chain_row, ',' ORDER BY readset_id, job_id, file_id, location_id)
FROM (
    SELECT 
        s.id AS sample_id,
        r.id AS readset_id,
        NULL::INT AS job_id,
        f.id AS file_id,
        l.id AS location_id,
        concat_ws(':', r.id, NULL, f.id, l.id, l.uri) AS chain_row
    FROM sample s
    JOIN readset r ON r.sample_id = s.id
    JOIN readset_file rf ON rf.readset_id = r.id
    JOIN file f ON f.id = rf.file_id
    JOIN location l ON l.file_id = f.id
    --ensure from the sample subset
    WHERE s.id IN (SELECT id FROM validation_samples)

    UNION

    SELECT 
        s.id AS sample_id,
        r.id AS readset_id,
        j.id AS job_id,
        f.id AS file_id,
        l.id AS location_id,
        concat_ws(':', r.id, j.id, f.id, l.id, l.uri) AS chain_row
    FROM sample s
    JOIN readset r ON r.sample_id = s.id
    JOIN readset_job rj ON rj.readset_id = r.id
    JOIN job j ON j.id = rj.job_id
    JOIN job_file jf ON jf.job_id = j.id
    JOIN file f ON f.id = jf.file_id
    JOIN location l ON l.file_id = f.id
    --ensure from the sample subset
    WHERE s.id IN (SELECT id FROM validation_samples)
) chain
GROUP BY sample_id;

----------------------------------------------------------------------
--Task 2: Run the deduplication script
----------------------------------------------------------------------

