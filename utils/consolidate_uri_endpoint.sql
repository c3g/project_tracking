-- ABACUS
-- Step 1: Identify Potential Duplicates and Consolidate
WITH potential_duplicates AS (
    SELECT id, uri, REPLACE(uri, 'abacus.genome.mcgill.ca', 'abacus') AS new_uri,
           ROW_NUMBER() OVER (PARTITION BY REPLACE(uri, 'abacus.genome.mcgill.ca', 'abacus') ORDER BY id) AS rn
    FROM location
    WHERE uri LIKE 'abacus.genome.mcgill.ca%' OR uri LIKE 'abacus%'
),
to_delete AS (
    SELECT id
    FROM potential_duplicates
    WHERE rn > 1
)
-- Step 2: Delete Duplicates
DELETE FROM location
WHERE id IN (SELECT id FROM to_delete);

-- Step 3: Update the Remaining Entries
UPDATE location
SET endpoint = REPLACE(endpoint, 'abacus.genome.mcgill.ca', 'abacus'),
    uri = REPLACE(uri, 'abacus.genome.mcgill.ca', 'abacus')
WHERE uri LIKE 'abacus.genome.mcgill.ca%';

-- BELUGA
-- Step 1: Identify Potential Duplicates and Consolidate
WITH potential_duplicates AS (
    SELECT id, uri, REPLACE(uri, 'beluga.genome.mcgill.ca', 'beluga') AS new_uri,
           ROW_NUMBER() OVER (PARTITION BY REPLACE(uri, 'beluga.genome.mcgill.ca', 'beluga') ORDER BY id) AS rn
    FROM location
    WHERE uri LIKE 'beluga.genome.mcgill.ca%' OR uri LIKE 'beluga%'
),
to_delete AS (
    SELECT id
    FROM potential_duplicates
    WHERE rn > 1
)
-- Step 2: Delete Duplicates
DELETE FROM location
WHERE id IN (SELECT id FROM to_delete);

-- Step 3: Update the Remaining Entries
UPDATE location
SET endpoint = REPLACE(endpoint, 'beluga.genome.mcgill.ca', 'beluga'),
    uri = REPLACE(uri, 'beluga.genome.mcgill.ca', 'beluga')
WHERE uri LIKE 'beluga.genome.mcgill.ca%';
