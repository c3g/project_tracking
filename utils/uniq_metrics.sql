BEGIN;

-- Step 1: Identify duplicate metrics and keep the one with the lowest ID
CREATE TEMP TABLE TempDuplicateMetrics AS
SELECT
    rm.readset_id,
    m.name,
    m.value,
    COUNT(*) AS cnt,
    MIN(rm.metric_id) AS keep_metric_id -- Keep the metric with the lowest ID
FROM
    readset_metric rm
JOIN
    metric m ON rm.metric_id = m.id
GROUP BY
    rm.readset_id, m.name, m.value
HAVING
    COUNT(*) > 1;

-- Step 2: Delete duplicate metrics
DELETE FROM
    readset_metric
WHERE
    metric_id IN (
        SELECT
            rm.metric_id
        FROM
            readset_metric rm
        JOIN
            metric m ON rm.metric_id = m.id
        JOIN
            TempDuplicateMetrics dm ON rm.readset_id = dm.readset_id AND m.name = dm.name AND m.value = dm.value
        WHERE
            rm.metric_id != dm.keep_metric_id
    );

-- Step 3: Identify jobs linked to duplicated metrics
CREATE TEMP TABLE TempJobsLinkedToDuplicatedMetrics AS
SELECT
    j.id AS job_id,
    rm.readset_id,
    dm.keep_metric_id
FROM
    job j
JOIN
    readset_job rj ON j.id = rj.job_id
JOIN
    readset_metric rm ON rj.readset_id = rm.readset_id
JOIN
    TempDuplicateMetrics dm ON rm.metric_id != dm.keep_metric_id AND rm.readset_id = dm.readset_id;

-- Step 4: Link jobs to the kept metrics
INSERT INTO metric (job_id, name, value, flag, deliverable, aggregate, deprecated, deleted, creation, modification, extra_metadata, ext_id, ext_src)
SELECT
    jlm.job_id,
    m.name,
    m.value,
    m.flag,
    m.deliverable,
    m.aggregate,
    m.deprecated,
    m.deleted,
    m.creation,
    m.modification,
    m.extra_metadata,
    m.ext_id,
    m.ext_src
FROM
    TempJobsLinkedToDuplicatedMetrics jlm
JOIN
    metric m ON jlm.keep_metric_id = m.id;

COMMIT;

-- Clean up temporary tables
DROP TABLE IF EXISTS TempDuplicateMetrics;
DROP TABLE IF EXISTS TempJobsLinkedToDuplicatedMetrics;