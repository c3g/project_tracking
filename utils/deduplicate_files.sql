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
    deleted_jf BIGINT := 0;
BEGIN
    RAISE NOTICE '--- Starting GLOBAL file deduplication (same name + md5sum) ---';

    ----------------------------------------------------------------------
    -- Step 1: Identify duplicate files globally (same name + md5sum)
    ----------------------------------------------------------------------
    CREATE TEMP TABLE file_dupes_global ON COMMIT DROP AS
    SELECT
        f.name,
        f.md5sum,
        -- Prefer deliverable = true, then lowest id
        (ARRAY_AGG(f.id ORDER BY f.deliverable DESC, f.id ASC))[1] AS keep_file_id,
        ARRAY_AGG(f.id ORDER BY f.id) AS all_file_ids,
        COUNT(*) AS cnt
    FROM file f
    GROUP BY f.name, f.md5sum
    HAVING COUNT(*) > 1;

    SELECT COUNT(*) INTO dup_group_count FROM file_dupes_global;
    RAISE NOTICE 'Duplicate file groups found (global): %', dup_group_count;

    IF dup_group_count = 0 THEN
        RAISE NOTICE 'No duplicate groups found; nothing to do.';
        RETURN;
    END IF;

    ----------------------------------------------------------------------
    -- Step 2: Delete conflicting readset_file links
    -- If a readset already points to the keep_file_id, drop the others.
    ----------------------------------------------------------------------
    DELETE FROM readset_file rf
    USING file_dupes_global fd
    WHERE rf.file_id = ANY(fd.all_file_ids)
      AND rf.file_id <> fd.keep_file_id
      -- If the readset already has the keep_file_id, then this row is redundant
      AND EXISTS (
          SELECT 1 FROM readset_file rf2
          WHERE rf2.readset_id = rf.readset_id
            AND rf2.file_id = fd.keep_file_id
      );
    GET DIAGNOSTICS deleted_rf_conflicts = ROW_COUNT;
    IF deleted_rf_conflicts > 0 THEN
        RAISE NOTICE 'Deleted conflicting readset_file links: %', deleted_rf_conflicts;
    END IF;

    ----------------------------------------------------------------------
	-- Step 3: Remap remaining readset_file rows to keep_file_id (conflict-safe)
	----------------------------------------------------------------------
	RAISE NOTICE 'Remapping readset_file rows (conflict-safe)...';
	
	LOOP
	    WITH one_update AS (
	        SELECT DISTINCT ON (rf.readset_id, fd.keep_file_id)
	               rf.ctid AS target_ctid,
	               fd.keep_file_id
	        FROM readset_file rf
	        JOIN file_dupes_global fd
	          ON rf.file_id = ANY(fd.all_file_ids)
	         AND rf.file_id <> fd.keep_file_id
	        WHERE NOT EXISTS (
	            SELECT 1
	            FROM readset_file rf2
	            WHERE rf2.readset_id = rf.readset_id
	              AND rf2.file_id = fd.keep_file_id
	        )
	        ORDER BY rf.readset_id, fd.keep_file_id
	        LIMIT 50000  -- batch size; tune if needed
	    )
	    UPDATE readset_file rf
	    SET file_id = ou.keep_file_id
	    FROM one_update ou
	    WHERE rf.ctid = ou.target_ctid;
	
	    GET DIAGNOSTICS remapped_rf = ROW_COUNT;
	    EXIT WHEN remapped_rf = 0;
	END LOOP;
	
	RAISE NOTICE 'Finished remapping readset_file rows (conflict-safe)';

    ----------------------------------------------------------------------
    -- Step 4: Optimized safe remap for job_file (batched)
    ----------------------------------------------------------------------
    RAISE NOTICE 'Starting safe remap for job_file (optimized, global)...';

    CREATE TEMP TABLE job_file_remap ON COMMIT DROP AS
    SELECT DISTINCT jf.job_id, jf.file_id AS old_file_id, fd.keep_file_id
    FROM job_file jf
    JOIN file_dupes_global fd
      ON jf.file_id = ANY(fd.all_file_ids)
     AND jf.file_id <> fd.keep_file_id;

    CREATE INDEX idx_jfr_old_file_id ON job_file_remap (old_file_id);
    CREATE INDEX idx_jfr_job_id_keep ON job_file_remap (job_id, keep_file_id);
    CREATE INDEX IF NOT EXISTS idx_jf_job_file_id ON job_file (job_id, file_id);

    -- Pre-delete job_file rows that would directly conflict (target already exists)
    DELETE FROM job_file jf
    USING job_file_remap jfr
    WHERE jf.job_id = jfr.job_id
      AND jf.file_id = jfr.old_file_id
      AND EXISTS (
          SELECT 1 FROM job_file jf2
          WHERE jf2.job_id = jf.job_id
            AND jf2.file_id = jfr.keep_file_id
      );
    GET DIAGNOSTICS deleted_jf_conflicts = ROW_COUNT;
    IF deleted_jf_conflicts > 0 THEN
        RAISE NOTICE 'Deleted conflicting job_file links (pre-delete): %', deleted_jf_conflicts;
    END IF;

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
    END LOOP;

    RAISE NOTICE 'Finished job_file remap (optimized, global)';

    ----------------------------------------------------------------------
    -- Step 5: Remap location
    ----------------------------------------------------------------------
    UPDATE location l
    SET file_id = fd.keep_file_id
    FROM file_dupes_global fd
    WHERE l.file_id = ANY(fd.all_file_ids)
      AND l.file_id <> fd.keep_file_id;
    GET DIAGNOSTICS remapped_loc = ROW_COUNT;
    IF remapped_loc > 0 THEN
        RAISE NOTICE 'Remapped location rows: %', remapped_loc;
    END IF;

    ----------------------------------------------------------------------
    -- Step 6: Final sanity remap for job_file (conflict-safe)
    -- (catch anything remaining after batches)
    ----------------------------------------------------------------------
    RAISE NOTICE 'Performing final sanity remap for job_file (conflict-safe, global)...';

    -- Delete job_file rows that would cause a duplicate during final remap
    DELETE FROM job_file jf
    USING (
        SELECT jf1.job_id, fd.keep_file_id
        FROM job_file jf1
        JOIN file_dupes_global fd
          ON jf1.file_id = ANY(fd.all_file_ids)
         AND jf1.file_id <> fd.keep_file_id
        JOIN job_file jf2
          ON jf2.job_id = jf1.job_id
         AND jf2.file_id = fd.keep_file_id
        GROUP BY jf1.job_id, fd.keep_file_id
    ) dup
    WHERE jf.job_id = dup.job_id
      AND jf.file_id IN (
          SELECT unnest(all_file_ids[2:]) FROM file_dupes_global
      );
    GET DIAGNOSTICS deleted_jf = ROW_COUNT;
    IF deleted_jf > 0 THEN
        RAISE NOTICE 'Deleted % conflicting job_file rows before final remap', deleted_jf;
    END IF;

    -- Final remap for remaining rows
    WITH to_fix AS (
        SELECT jf.ctid AS target_ctid, fd.keep_file_id
        FROM job_file jf
        JOIN file_dupes_global fd
          ON jf.file_id = ANY(fd.all_file_ids)
         AND jf.file_id <> fd.keep_file_id
    )
    UPDATE job_file jf
    SET file_id = tf.keep_file_id
    FROM to_fix tf
    WHERE jf.ctid = tf.target_ctid;
    GET DIAGNOSTICS remapped_jf = ROW_COUNT;
    IF remapped_jf > 0 THEN
        RAISE NOTICE 'Final job_file remap fixed % remaining rows', remapped_jf;
    ELSE
        RAISE NOTICE 'No remaining job_file rows needed final remap';
    END IF;

    ----------------------------------------------------------------------
    -- Step 7: Delete redundant file rows (safe)
    ----------------------------------------------------------------------
    WITH dup_files AS (
        SELECT unnest(all_file_ids) AS file_id, keep_file_id
        FROM file_dupes_global
    )
    DELETE FROM file f
    WHERE f.id IN (
        SELECT df.file_id
        FROM dup_files df
        WHERE df.file_id <> df.keep_file_id
          AND NOT EXISTS (SELECT 1 FROM readset_file rf WHERE rf.file_id = df.file_id)
          AND NOT EXISTS (SELECT 1 FROM job_file jf WHERE jf.file_id = df.file_id)
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
    RAISE NOTICE '--- GLOBAL File deduplication complete ---';
    RAISE NOTICE 'Groups=% , readset_file deleted=% , readset_file remapped=% , job_file remapped (last batch)=% , location remapped=% , files deleted=%',
        dup_group_count, deleted_rf_conflicts, remapped_rf, remapped_jf, remapped_loc, deleted_files;

END $$;

COMMIT;
