-- Step 1: Drop the temporary table if it exists
DROP TABLE IF EXISTS temp_operation_config;
DROP TABLE IF EXISTS operation_config_mapping;

-- Step 2: Create the temporary table with columns to track the original id and new_id
CREATE TEMP TABLE temp_operation_config (
    md5sum TEXT,
    original_id INTEGER,
    new_id INTEGER
);

-- Step 3: Recompute md5sum and insert into the temporary table
INSERT INTO temp_operation_config (md5sum, original_id, new_id)
SELECT MD5(data), id, NULL
FROM operation_config;

-- Step 4: Identify duplicates and assign the same id to all original_ids with the same md5sum
WITH DuplicateConfigs AS (
    SELECT md5sum, original_id, MIN(original_id) OVER (PARTITION BY md5sum) AS new_id, ROW_NUMBER() OVER (PARTITION BY md5sum ORDER BY original_id) AS rn
    FROM temp_operation_config
)
UPDATE temp_operation_config
SET new_id = DuplicateConfigs.new_id
FROM DuplicateConfigs
WHERE temp_operation_config.original_id = DuplicateConfigs.original_id;

SELECT * FROM temp_operation_config;

-- Step 5: Create a mapping table to track old to new operation_config IDs
CREATE TEMP TABLE operation_config_mapping (
    old_id INTEGER,
    new_id INTEGER
);

-- Insert into the mapping table
INSERT INTO operation_config_mapping
SELECT original_id AS old_id, new_id
FROM temp_operation_config;

-- Debugging Step: Check the contents of the mapping table
SELECT * FROM operation_config_mapping;

-- Step 6: Deduplicate temp_operation_config by keeping only one row per md5sum
WITH DeduplicatedConfigs AS (
    SELECT md5sum, new_id, original_id, ROW_NUMBER() OVER (PARTITION BY md5sum ORDER BY new_id) AS rn
    FROM temp_operation_config
)
DELETE FROM temp_operation_config
WHERE original_id IN (
    SELECT original_id
    FROM DeduplicatedConfigs
    WHERE rn > 1
);
-- Remove original_id column as it's no longer needed
ALTER TABLE temp_operation_config
DROP COLUMN original_id;

-- Step 7: Update operation table to reflect consolidated operation_config IDs
UPDATE operation
SET operation_config_id = mapping.new_id
FROM operation_config_mapping AS mapping
WHERE operation.operation_config_id = mapping.old_id;

-- Step 9: Remove extra rows from operation_config
DELETE FROM operation_config
WHERE id NOT IN (
    SELECT new_id
    FROM temp_operation_config
);

-- Step 10: Drop the temporary tables
DROP TABLE IF EXISTS temp_operation_config;
DROP TABLE IF EXISTS operation_config_mapping;