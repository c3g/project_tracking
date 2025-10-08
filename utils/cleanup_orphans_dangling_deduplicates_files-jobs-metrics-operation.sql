BEGIN;

DO $$
DECLARE
    -- general
    total_missing_loc INT;

    -- dedup / remap (files)
    dedup_candidates INT := 0;
    dedup_mapping_pairs INT := 0;
    dedup_inserted_remaps INT := 0;
    dedup_readsetfile_deleted INT := 0;
    dedup_jobfile_deleted INT := 0;
    dedup_file_deleted INT := 0;

    -- job-only
    job_only_count INT := 0;
    job_only_deleted_jobfile INT := 0;
    job_only_deleted_file INT := 0;

    -- investigation (files)
    readset_no_loc_count INT := 0;
    rescued_strict_count INT := 0;
    rescued_loose_count INT := 0;

    -- rescue action counts (files)
    rescue_inserted_remaps INT := 0;
    rescue_deleted_old_rf INT := 0;
    rescue_deleted_jobfile INT := 0;
    rescue_deleted_files INT := 0;

    -- orphan cleanup
    orphan_metrics_deleted INT := 0;
    orphan_metrics_kept INT := 0;
    orphan_jobs_deleted INT := 0;
    orphan_ops_deleted INT := 0;

    -- JOB deduplication counters
    job_groups_count INT := 0;
    job_remap_inserted_readset_job INT := 0;
    job_readset_deleted_count INT := 0;
    jobfile_inserted_count INT := 0;
    jobfile_deleted_count INT := 0;
    metrics_remapped_count INT := 0;
    jobs_deleted_count INT := 0;

    -- METRIC deduplication counters
    metric_groups_count INT := 0;
    readset_metric_inserted_count INT := 0;
    readset_metric_deleted_count INT := 0;
    metrics_deleted_count INT := 0;
BEGIN
    RAISE NOTICE '--- Starting merged cleanup + rescue transaction ---';

    ----------------------------------------------------------------------
    -- Step 0: General stats
    ----------------------------------------------------------------------
    SELECT count(*) INTO total_missing_loc
    FROM file f
    LEFT JOIN location l ON f.id = l.file_id
    WHERE l.file_id IS NULL;
    RAISE NOTICE 'Total files missing location (initial): %', total_missing_loc;

    ----------------------------------------------------------------------
    -- Step 1: Deduplication + strict remapping (name+operation+job)
    ----------------------------------------------------------------------
    RAISE NOTICE 'Step 1: Deduplication + strict remapping (name+op+job)...';

    CREATE TEMP TABLE candidate_files ON COMMIT DROP AS
    SELECT f.id AS file_id, f.name, j.operation_id, j.id AS job_id
    FROM file f
    JOIN job_file jf ON f.id = jf.file_id
    JOIN job j ON jf.job_id = j.id
    LEFT JOIN location l ON f.id = l.file_id
    WHERE l.file_id IS NULL
      AND EXISTS (
          SELECT 1
          FROM file f2
          JOIN job_file jf2 ON f2.id = jf2.file_id
          JOIN job j2 ON jf2.job_id = j2.id
          JOIN location l2 ON f2.id = l2.file_id
          WHERE f2.name = f.name
            AND j2.operation_id = j.operation_id
            AND j2.id = j.id
      );

    SELECT COUNT(*) INTO dedup_candidates FROM candidate_files;
    RAISE NOTICE 'Dedup candidates found (strict): %', dedup_candidates;

    CREATE TEMP TABLE keep_files ON COMMIT DROP AS
    SELECT DISTINCT f2.id AS file_id, f2.name, j2.operation_id, j2.id AS job_id
    FROM file f2
    JOIN job_file jf2 ON f2.id = jf2.file_id
    JOIN job j2 ON jf2.job_id = j2.id
    JOIN location l2 ON f2.id = l2.file_id;

    CREATE TEMP TABLE mapping_pairs ON COMMIT DROP AS
    SELECT DISTINCT rf.readset_id,
                    rf.file_id AS remove_file_id,
                    k.file_id AS keep_file_id
    FROM readset_file rf
    JOIN candidate_files c ON rf.file_id = c.file_id
    JOIN keep_files k
      ON k.name = c.name
     AND k.operation_id = c.operation_id
     AND k.job_id = c.job_id;

    SELECT COUNT(*) INTO dedup_mapping_pairs FROM mapping_pairs;
    RAISE NOTICE 'Mapping pairs (strict) found: %', dedup_mapping_pairs;

    -- insert missing readset -> keep_file if needed
    INSERT INTO readset_file (readset_id, file_id)
    SELECT mp.readset_id, mp.keep_file_id
    FROM mapping_pairs mp
    LEFT JOIN readset_file rf_check
           ON rf_check.readset_id = mp.readset_id
          AND rf_check.file_id = mp.keep_file_id
    WHERE rf_check.readset_id IS NULL;
    GET DIAGNOSTICS dedup_inserted_remaps = ROW_COUNT;
    RAISE NOTICE 'Inserted strict readset->keep_file mappings (new): %', dedup_inserted_remaps;

    -- delete old readset_file rows pointing at candidate unlocated files
    DELETE FROM readset_file rf
    WHERE rf.file_id IN (SELECT file_id FROM candidate_files);
    GET DIAGNOSTICS dedup_readsetfile_deleted = ROW_COUNT;
    RAISE NOTICE 'Deleted old readset_file rows (strict candidates): %', dedup_readsetfile_deleted;

    -- delete job_file links for strict candidate files
    DELETE FROM job_file jf
    WHERE jf.file_id IN (SELECT file_id FROM candidate_files);
    GET DIAGNOSTICS dedup_jobfile_deleted = ROW_COUNT;
    RAISE NOTICE 'Deleted job_file links for strict candidates: %', dedup_jobfile_deleted;

    -- delete candidate file rows
    DELETE FROM file f
    WHERE f.id IN (SELECT file_id FROM candidate_files);
    GET DIAGNOSTICS dedup_file_deleted = ROW_COUNT;
    RAISE NOTICE 'Deleted strict candidate file rows: %', dedup_file_deleted;

    ----------------------------------------------------------------------
    -- Step 2: Job-only missing-location files (safe delete)
    ----------------------------------------------------------------------
    RAISE NOTICE 'Step 2: Job-only missing-location cleanup (safe delete)...';

    CREATE TEMP TABLE job_only_files ON COMMIT DROP AS
    SELECT f.id AS file_id
    FROM file f
    LEFT JOIN location l ON f.id = l.file_id
    LEFT JOIN readset_file rf ON f.id = rf.file_id
    JOIN job_file jf ON f.id = jf.file_id
    WHERE l.file_id IS NULL
      AND rf.file_id IS NULL;

    SELECT COUNT(*) INTO job_only_count FROM job_only_files;
    RAISE NOTICE 'Job-only missing-location files identified: %', job_only_count;

    DELETE FROM job_file jf
    WHERE jf.file_id IN (SELECT file_id FROM job_only_files);
    GET DIAGNOSTICS job_only_deleted_jobfile = ROW_COUNT;
    RAISE NOTICE 'Deleted job_file links (job-only): %', job_only_deleted_jobfile;

    DELETE FROM file f
    WHERE f.id IN (SELECT file_id FROM job_only_files);
    GET DIAGNOSTICS job_only_deleted_file = ROW_COUNT;
    RAISE NOTICE 'Deleted job-only file rows: %', job_only_deleted_file;

    ----------------------------------------------------------------------
    -- Step 3: Investigate readset-linked missing-location files without strict duplicate
    ----------------------------------------------------------------------
    RAISE NOTICE 'Step 3: Investigate readset-linked missing-location files without strict duplicate...';

    CREATE TEMP TABLE readset_no_loc ON COMMIT DROP AS
    SELECT f.id AS file_id, f.name, rf.readset_id, j.operation_id, j.id AS job_id
    FROM file f
    JOIN readset_file rf ON f.id = rf.file_id
    JOIN job_file jf ON f.id = jf.file_id
    JOIN job j ON jf.job_id = j.id
    LEFT JOIN location l ON f.id = l.file_id
    WHERE l.file_id IS NULL
      AND NOT EXISTS (
          SELECT 1
          FROM file f2
          JOIN job_file jf2 ON f2.id = jf2.file_id
          JOIN job j2 ON jf2.job_id = j2.id
          JOIN location l2 ON f2.id = l2.file_id
          WHERE f2.name = f.name
            AND j2.operation_id = j.operation_id
            AND j2.id = j.id
      );

    SELECT COUNT(*) INTO readset_no_loc_count FROM readset_no_loc;
    RAISE NOTICE 'Readset-linked missing-location files (no strict duplicate): %', readset_no_loc_count;

    -- strict rescue attempt using location.uri suffix constrained to same job+op+name (investigative)
    CREATE TEMP TABLE rescued_strict ON COMMIT DROP AS
    SELECT rnl.file_id AS remove_file_id, rnl.name, rnl.readset_id, fw.file_id AS located_file_id
    FROM readset_no_loc rnl
    JOIN (
        SELECT f2.id AS file_id, f2.name, j2.operation_id, j2.id AS job_id
        FROM file f2
        JOIN job_file jf2 ON f2.id = jf2.file_id
        JOIN job j2 ON jf2.job_id = j2.id
        JOIN location l2 ON f2.id = l2.file_id
    ) fw ON fw.name = rnl.name
         AND fw.operation_id = rnl.operation_id
         AND fw.job_id = rnl.job_id
    JOIN location lfw ON fw.file_id = lfw.file_id
    WHERE lfw.uri LIKE '%' || rnl.name;

    SELECT COUNT(*) INTO rescued_strict_count FROM rescued_strict;
    RAISE NOTICE 'Rescuable (strict, job+op+name & URI suffix): %', rescued_strict_count;

    -- loose rescue attempt: same operation + name, location.uri LIKE '%name' (different job allowed)
    CREATE TEMP TABLE rescued_loose ON COMMIT DROP AS
    SELECT rnl.file_id AS remove_file_id, rnl.name, rnl.readset_id, fw.file_id AS located_file_id, fw.job_id AS located_job_id
    FROM readset_no_loc rnl
    JOIN (
        SELECT f2.id AS file_id, f2.name, j2.operation_id, j2.id AS job_id
        FROM file f2
        JOIN job_file jf2 ON f2.id = jf2.file_id
        JOIN job j2 ON jf2.job_id = j2.id
        JOIN location l2 ON f2.id = l2.file_id
    ) fw ON fw.name = rnl.name
         AND fw.operation_id = rnl.operation_id
    JOIN location lfw ON fw.file_id = lfw.file_id
    WHERE lfw.uri LIKE '%' || rnl.name;

    SELECT COUNT(*) INTO rescued_loose_count FROM rescued_loose;
    RAISE NOTICE 'Rescuable (loose, op+name & URI suffix across jobs): %', rescued_loose_count;

    ----------------------------------------------------------------------
    -- Step 4: Automatic rescue/remap for rescued_loose (controlled, constrained)
    ----------------------------------------------------------------------
    IF (rescued_loose_count > 0) THEN
        RAISE NOTICE 'Step 4: Performing automatic rescue/remap for loose-rescuable cases: % items', rescued_loose_count;

        -- keep only distinct pairs (readset, remove_file, located_file)
        CREATE TEMP TABLE rescue_actions ON COMMIT DROP AS
        SELECT DISTINCT rl.readset_id, rl.remove_file_id, rl.located_file_id
        FROM rescued_loose rl;

        -- 4a: insert missing readset_file links to located_file
        INSERT INTO readset_file (readset_id, file_id)
        SELECT ra.readset_id, ra.located_file_id
        FROM rescue_actions ra
        ON CONFLICT (readset_id, file_id) DO NOTHING;
        GET DIAGNOSTICS rescue_inserted_remaps = ROW_COUNT;
        RAISE NOTICE 'Inserted readset->located_file mappings (rescue): %', rescue_inserted_remaps;

        -- 4b: delete old readset_file entries pointing at remove_file_id
        DELETE FROM readset_file rf
        WHERE (rf.readset_id, rf.file_id) IN (
            SELECT ra.readset_id, ra.remove_file_id FROM rescue_actions ra
        );
        GET DIAGNOSTICS rescue_deleted_old_rf = ROW_COUNT;
        RAISE NOTICE 'Deleted old readset_file rows (rescue): %', rescue_deleted_old_rf;

        -- 4c: delete job_file links for remove files (if any remain)
        DELETE FROM job_file jf
        WHERE jf.file_id IN (SELECT remove_file_id FROM rescue_actions);
        GET DIAGNOSTICS rescue_deleted_jobfile = ROW_COUNT;
        RAISE NOTICE 'Deleted job_file links for rescued remove_files: %', rescue_deleted_jobfile;

        -- 4d: delete remove_file rows themselves
        DELETE FROM file f
        WHERE f.id IN (SELECT remove_file_id FROM rescue_actions);
        GET DIAGNOSTICS rescue_deleted_files = ROW_COUNT;
        RAISE NOTICE 'Deleted file rows (rescue): %', rescue_deleted_files;
    ELSE
        RAISE NOTICE 'Step 4: No automatic rescue/remap actions to perform.';
    END IF;

    ----------------------------------------------------------------------
    -- Step 5: Orphan jobs / metrics / operations (safe handling)
    ----------------------------------------------------------------------
    RAISE NOTICE 'Step 5: Cleaning up orphan jobs, metrics, operations (safe metrics)...';

    -- Orphan jobs: no readset_job and no job_file
    CREATE TEMP TABLE orphan_jobs ON COMMIT DROP AS
    SELECT j.id AS job_id
    FROM job j
    LEFT JOIN readset_job rj ON j.id = rj.job_id
    LEFT JOIN job_file jf ON j.id = jf.job_id
    WHERE rj.job_id IS NULL
      AND jf.job_id IS NULL;

    -- Metrics pointing to those orphan jobs
    CREATE TEMP TABLE orphan_metrics ON COMMIT DROP AS
    SELECT m.id AS metric_id, m.job_id
    FROM metric m
    JOIN orphan_jobs oj ON m.job_id = oj.job_id;

    -- Metrics safe to delete (not linked to any readset)
    CREATE TEMP TABLE orphan_metrics_to_delete ON COMMIT DROP AS
    SELECT om.metric_id
    FROM orphan_metrics om
    LEFT JOIN readset_metric rm ON om.metric_id = rm.metric_id
    WHERE rm.metric_id IS NULL;

    DELETE FROM metric m
    WHERE m.id IN (SELECT metric_id FROM orphan_metrics_to_delete);
    GET DIAGNOSTICS orphan_metrics_deleted = ROW_COUNT;

    SELECT COUNT(*) INTO orphan_metrics_kept
    FROM orphan_metrics om
    WHERE om.metric_id NOT IN (SELECT metric_id FROM orphan_metrics_to_delete);

    RAISE NOTICE 'Deleted orphan metrics (safe): %', orphan_metrics_deleted;
    RAISE NOTICE 'Kept orphan metrics due to readset linkage: %', orphan_metrics_kept;

    -- Delete jobs that now have no metrics and are orphan
    DELETE FROM job j
    WHERE j.id IN (
        SELECT oj.job_id
        FROM orphan_jobs oj
        LEFT JOIN metric m ON oj.job_id = m.job_id
        WHERE m.id IS NULL
    );
    GET DIAGNOSTICS orphan_jobs_deleted = ROW_COUNT;
    RAISE NOTICE 'Deleted orphan jobs with no remaining metrics: %', orphan_jobs_deleted;

    -- Orphan operations: no readset_operation and no remaining jobs
    CREATE TEMP TABLE orphan_operations ON COMMIT DROP AS
    SELECT o.id AS operation_id
    FROM operation o
    LEFT JOIN readset_operation ro ON o.id = ro.operation_id
    WHERE ro.operation_id IS NULL
      AND NOT EXISTS (SELECT 1 FROM job j WHERE j.operation_id = o.id);

    DELETE FROM operation o
    WHERE o.id IN (SELECT operation_id FROM orphan_operations);
    GET DIAGNOSTICS orphan_ops_deleted = ROW_COUNT;
    RAISE NOTICE 'Deleted orphan operations: %', orphan_ops_deleted;

	----------------------------------------------------------------------
	-- Step 6: Deduplicate operations (run_processing + transfer)
	----------------------------------------------------------------------
	RAISE NOTICE 'Step 6: Deduplicating operations (run_processing + transfer)...';
	
	-- Identify duplicate operations (same name, cmd_line, platform, operation_config_id)
	CREATE TEMP TABLE operation_duplicates ON COMMIT DROP AS
	SELECT o.id AS op_id,
	       MIN(o.id) OVER (PARTITION BY o.name, o.cmd_line, o.platform, o.operation_config_id) AS keep_op_id
	FROM operation o
	WHERE o.name IN ('run_processing', 'transfer');
	
	-- Count of operation groups to dedupe
	SELECT COUNT(DISTINCT keep_op_id) INTO total_missing_loc
	FROM operation_duplicates
	WHERE keep_op_id <> op_id;
	RAISE NOTICE 'Operation groups to dedupe: %', total_missing_loc;
	
	-- 1. Delete conflicting readset_operation rows before remapping
	DELETE FROM readset_operation ro
	USING operation_duplicates od
	WHERE ro.operation_id = od.op_id
	  AND EXISTS (
	      SELECT 1
	      FROM readset_operation ro2
	      WHERE ro2.readset_id = ro.readset_id
	        AND ro2.operation_id = od.keep_op_id
	  );
	
	-- 2. Update readset_operation safely
	UPDATE readset_operation ro
	SET operation_id = od.keep_op_id
	FROM operation_duplicates od
	WHERE ro.operation_id = od.op_id
	  AND ro.operation_id <> od.keep_op_id;
	
	-- 3. Update job.operation_id
	UPDATE job j
	SET operation_id = od.keep_op_id
	FROM operation_duplicates od
	WHERE j.operation_id = od.op_id
	  AND j.operation_id <> od.keep_op_id;
	
	-- 4. Delete duplicate operations
	DELETE FROM operation o
	WHERE o.id IN (
	    SELECT op_id
	    FROM operation_duplicates
	    WHERE keep_op_id <> op_id
	);
	
	RAISE NOTICE 'Duplicate operations collapsed into canonical entries.';


    ----------------------------------------------------------------------
    -- Step 7: Deduplicate jobs (same operation_id + name + start + stop)
    ----------------------------------------------------------------------
    RAISE NOTICE 'Step 6: Deduplicating jobs (same op+name+start+stop)...';

    CREATE TEMP TABLE job_groups ON COMMIT DROP AS
    SELECT operation_id, name, start, stop,
           MIN(id) AS keep_job_id,
           ARRAY_AGG(id) AS all_job_ids
    FROM job
    GROUP BY operation_id, name, start, stop
    HAVING COUNT(*) > 1;

    SELECT COUNT(*) INTO job_groups_count FROM job_groups;
    RAISE NOTICE 'Job groups to dedupe: %', job_groups_count;

    -- Remap readset_job: insert target mapping and remove old ones
    INSERT INTO readset_job (readset_id, job_id)
    SELECT DISTINCT rj.readset_id, jg.keep_job_id
    FROM readset_job rj
    JOIN job_groups jg ON rj.job_id = ANY(jg.all_job_ids)
    WHERE rj.job_id <> jg.keep_job_id
    ON CONFLICT (readset_id, job_id) DO NOTHING;
    GET DIAGNOSTICS job_remap_inserted_readset_job = ROW_COUNT;
    RAISE NOTICE 'Inserted remapped readset_job rows: %', job_remap_inserted_readset_job;

    DELETE FROM readset_job rj
    USING job_groups jg
    WHERE rj.job_id = ANY(jg.all_job_ids)
      AND rj.job_id <> jg.keep_job_id;
    GET DIAGNOSTICS job_readset_deleted_count = ROW_COUNT;
    RAISE NOTICE 'Deleted old readset_job rows for duplicates: %', job_readset_deleted_count;

    -- Remap job_file: insert keep mapping then delete old
    INSERT INTO job_file (job_id, file_id)
    SELECT DISTINCT jg.keep_job_id, jf.file_id
    FROM job_file jf
    JOIN job_groups jg ON jf.job_id = ANY(jg.all_job_ids)
    WHERE jf.job_id <> jg.keep_job_id
    ON CONFLICT (job_id, file_id) DO NOTHING;
    GET DIAGNOSTICS jobfile_inserted_count = ROW_COUNT;
    RAISE NOTICE 'Inserted remapped job_file rows: %', jobfile_inserted_count;

    DELETE FROM job_file jf
    USING job_groups jg
    WHERE jf.job_id = ANY(jg.all_job_ids)
      AND jf.job_id <> jg.keep_job_id;
    GET DIAGNOSTICS jobfile_deleted_count = ROW_COUNT;
    RAISE NOTICE 'Deleted old job_file rows for duplicates: %', jobfile_deleted_count;

    -- Remap metrics to keep_job_id
    UPDATE metric m
    SET job_id = jg.keep_job_id
    FROM job_groups jg
    WHERE m.job_id = ANY(jg.all_job_ids)
      AND m.job_id <> jg.keep_job_id;
    GET DIAGNOSTICS metrics_remapped_count = ROW_COUNT;
    RAISE NOTICE 'Remapped metrics to keep_job_id: %', metrics_remapped_count;

    -- Delete duplicate job rows (keep the keep_job_id)
    DELETE FROM job j
    USING job_groups jg
    WHERE j.id = ANY(jg.all_job_ids)
      AND j.id <> jg.keep_job_id;
    GET DIAGNOSTICS jobs_deleted_count = ROW_COUNT;
    RAISE NOTICE 'Deleted duplicate job rows: %', jobs_deleted_count;

    ----------------------------------------------------------------------
    -- Step 8: Deduplicate metrics (same job_id + name + value) AFTER job dedup
    ----------------------------------------------------------------------
    RAISE NOTICE 'Step 7: Deduplicating metrics (same job+name+value after job dedup)...';

    CREATE TEMP TABLE metric_groups ON COMMIT DROP AS
    SELECT job_id, name, value,
           MIN(id) AS keep_metric_id,
           ARRAY_AGG(id) AS all_metric_ids
    FROM metric
    GROUP BY job_id, name, value
    HAVING COUNT(*) > 1;

    SELECT COUNT(*) INTO metric_groups_count FROM metric_groups;
    RAISE NOTICE 'Metric groups to dedupe: %', metric_groups_count;

    -- Remap readset_metric: insert mapping to keep_metric_id if not present
    INSERT INTO readset_metric (readset_id, metric_id)
    SELECT DISTINCT rm.readset_id, mg.keep_metric_id
    FROM readset_metric rm
    JOIN metric_groups mg ON rm.metric_id = ANY(mg.all_metric_ids)
    WHERE rm.metric_id <> mg.keep_metric_id
    ON CONFLICT (readset_id, metric_id) DO NOTHING;
    GET DIAGNOSTICS readset_metric_inserted_count = ROW_COUNT;
    RAISE NOTICE 'Inserted remapped readset_metric rows: %', readset_metric_inserted_count;

    -- Delete old readset_metric entries that referenced duplicate metrics
    DELETE FROM readset_metric rm
    USING metric_groups mg
    WHERE rm.metric_id = ANY(mg.all_metric_ids)
      AND rm.metric_id <> mg.keep_metric_id;
    GET DIAGNOSTICS readset_metric_deleted_count = ROW_COUNT;
    RAISE NOTICE 'Deleted old readset_metric rows for duplicates: %', readset_metric_deleted_count;

    -- Finally, delete duplicate metric rows (keep the keep_metric_id)
    DELETE FROM metric m
    USING metric_groups mg
    WHERE m.id = ANY(mg.all_metric_ids)
      AND m.id <> mg.keep_metric_id;
    GET DIAGNOSTICS metrics_deleted_count = ROW_COUNT;
    RAISE NOTICE 'Deleted duplicate metric rows: %', metrics_deleted_count;

    ----------------------------------------------------------------------
    -- Final summary
    ----------------------------------------------------------------------
    RAISE NOTICE '--- Final summary ---';
    RAISE NOTICE 'Initial files missing location: %', total_missing_loc;
    RAISE NOTICE 'Strict dedup candidates: % ; mapping pairs (strict): % ; strict inserted remaps: %', dedup_candidates, dedup_mapping_pairs, dedup_inserted_remaps;
    RAISE NOTICE 'Deleted (strict dedup) job_file: % ; readset_file: % ; files: %', dedup_jobfile_deleted, dedup_readsetfile_deleted, dedup_file_deleted;
    RAISE NOTICE 'Job-only missing-location: % ; deleted job_file: % ; deleted files: %', job_only_count, job_only_deleted_jobfile, job_only_deleted_file;
    RAISE NOTICE 'Readset-linked no-strict-dup count: % ; rescued_strict: % ; rescued_loose_found: %', readset_no_loc_count, rescued_strict_count, rescued_loose_count;
    RAISE NOTICE 'Rescue actions: inserted remaps: % ; deleted old rf: % ; deleted jobfile: % ; deleted files: %', rescue_inserted_remaps, rescue_deleted_old_rf, rescue_deleted_jobfile, rescue_deleted_files;
    RAISE NOTICE 'Orphan metrics deleted: % ; orphan metrics kept: % ; orphan jobs deleted: % ; orphan ops deleted: %', orphan_metrics_deleted, orphan_metrics_kept, orphan_jobs_deleted, orphan_ops_deleted;

    RAISE NOTICE 'Job dedup: groups: % ; readset_job inserted remaps: % ; readset_job deleted old: % ; job_file inserted remaps: % ; job_file deleted old: % ; metrics remapped: % ; jobs deleted: %',
        job_groups_count, job_remap_inserted_readset_job, job_readset_deleted_count, jobfile_inserted_count, jobfile_deleted_count, metrics_remapped_count, jobs_deleted_count;

    RAISE NOTICE 'Metric dedup: groups: % ; readset_metric inserted remaps: % ; readset_metric deleted old: % ; metrics deleted: %',
        metric_groups_count, readset_metric_inserted_count, readset_metric_deleted_count, metrics_deleted_count;

    RAISE NOTICE '--- End of cleanup + rescue transaction ---';
END $$;

COMMIT;
