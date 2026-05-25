----------------------------------------------------------------------
--DIGEST CHECKS
----------------------------------------------------------------------
--Test 1: Runs across a random subset of samples (or all samples if adjusted) 
----------------------------------------------------------------------
--Task 3:Post-run — compute the same hashes again
----------------------------------------------------------------------
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
--Task 4:Compare — report any samples where the hash changed
----------------------------------------------------------------------
DO $$
DECLARE
    rec RECORD;
    changed_count INT := 0;
BEGIN
    FOR rec IN (
        SELECT 
            pre.sample_id,
            pre.digest AS pre_digest,
            post.digest AS post_digest,
            pre.hashed_value AS pre_hashed_value,
            post.hashed_value AS post_hashed_value
        FROM pre_dedup_sample_digest pre
        JOIN post_dedup_sample_digest post ON pre.sample_id = post.sample_id
        WHERE pre.digest != post.digest
    ) LOOP
        changed_count := changed_count + 1;
        RAISE NOTICE 'Sample % digest changed. PRE: % POST: %', 
            rec.sample_id, rec.pre_digest, rec.post_digest;
    END LOOP;

    IF changed_count = 0 THEN
        RAISE NOTICE 'Validation passed — no digest changes detected across % samples', 
            (SELECT COUNT(*) FROM validation_samples);
    ELSE
        RAISE NOTICE 'Validation FAILED — % samples had digest changes', changed_count;
    END IF;
END $$;