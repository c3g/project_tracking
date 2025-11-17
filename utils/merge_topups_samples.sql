BEGIN;

DO $$
DECLARE
	merge_count int;
BEGIN
	----------------------------------------------------------------------
	-- Step 1: Build temporary table of all merge pairs (automatic + manual)
	----------------------------------------------------------------------
	CREATE TEMP TABLE tmp_merge_pairs AS
	WITH manual_exceptions AS (
	    SELECT 
	        'MoHQ-JG-1-40-015001193583V2-1RT' AS keep_name,
	        'MoHQ-JG-1-40-015001193631V4-1RT' AS remove_name
	    UNION ALL SELECT
	        'MoHQ-JG-7-5-015000860845V2-1RT',
	        'MoHQ-JG-7-5-015000727120V3-1RT'
	    UNION ALL SELECT
	        'MoHQ-MU-34-55-TU-1RT',
	        'MoHQ-MU-34-55-MET-1RT'
	),
	parsed AS (
	    SELECT
	        s.id,
	        s.name,
	        regexp_replace(s.name, '-[0-9]+(RT|DT|DN|FRT)$', '') AS base,
	        (regexp_match(s.name, '-([0-9]+)(RT|DT|DN|FRT)$'))[1]::int AS num,
	        (regexp_match(s.name, '-([0-9]+)(RT|DT|DN|FRT)$'))[2] AS type
	    FROM sample s
	    WHERE s.name ~ '-[0-9]+(RT|DT|DN|FRT)$'
	),
	groups AS (
	    SELECT DISTINCT ON (base, type)
	        base,
	        type,
	        id AS id_keep,
	        name AS name_keep,
	        num AS num_keep
	    FROM parsed
	    ORDER BY base, type, num
	),
	auto_pairs AS (
	    SELECT
	        g.id_keep,
	        g.name_keep,
	        p.id AS id_remove,
	        p.name AS name_remove
	    FROM groups g
	    JOIN parsed p
	      ON p.base = g.base
	     AND p.type = g.type
	     AND p.num > g.num_keep
	),
	manual_pairs AS (
	    SELECT 
	        ks.id AS id_keep,
	        ks.name AS name_keep,
	        rs.id AS id_remove,
	        rs.name AS name_remove
	    FROM manual_exceptions me
	    JOIN sample ks ON ks.name = me.keep_name
	    JOIN sample rs ON rs.name = me.remove_name
	)
	SELECT * FROM auto_pairs
	UNION ALL
	SELECT * FROM manual_pairs;
	
	RAISE NOTICE '% pairs identified for merging.', (SELECT COUNT(*) FROM tmp_merge_pairs);
	
	----------------------------------------------------------------------
	-- Step 2: Append duplicate names to alias of keeper
	----------------------------------------------------------------------
	UPDATE sample s
	SET alias = COALESCE(s.alias, '[]'::jsonb) || to_jsonb(p.name_remove)
	FROM tmp_merge_pairs p
	WHERE s.id = p.id_keep
	  AND NOT EXISTS (
	      SELECT 1
	      FROM jsonb_array_elements_text(COALESCE(s.alias, '[]'::jsonb)) x(val)
	      WHERE x.val = p.name_remove
	  );
	
	----------------------------------------------------------------------
	-- Step 3: Reassign readsets from removed samples to keeper
	----------------------------------------------------------------------
	UPDATE readset r
	SET sample_id = p.id_keep
	FROM tmp_merge_pairs p
	WHERE r.sample_id = p.id_remove;
	
	----------------------------------------------------------------------
	-- Step 4: Delete duplicate samples
	----------------------------------------------------------------------
	DELETE FROM sample s
	USING tmp_merge_pairs p
	WHERE s.id = p.id_remove;
	
	----------------------------------------------------------------------
	-- Step 5: Drop temporary table
	----------------------------------------------------------------------
	DROP TABLE tmp_merge_pairs;

END $$;

COMMIT;
