# Vocabulary for JSON entry for ingesting run processing
# project table
PROJECT_FMS_ID = "project_fms_id"
PROJECT_NAME = "project_name"

# patient table
PATIENT = "patient"
PATIENT_FMS_ID = "patient_fms_id"
PATIENT_NAME = "patient_name"
PATIENT_COHORT = "patient_cohort"
PATIENT_INSTITUTION = "patient_institution"

# sample table
SAMPLE = "sample"
SAMPLE_FMS_ID = "sample_fms_id"
SAMPLE_NAME = "sample_name"
SAMPLE_TUMOUR = "sample_tumour"

# experiment table
EXPERIMENT_SEQUENCING_TECHNOLOGY = "experiment_sequencing_technology"
EXPERIMENT_TYPE = "experiment_type"
EXPERIMENT_LIBRARY_KIT = "experiment_library_kit"
EXPERIMENT_KIT_EXPIRATION_DATE = "experiment_kit_expiration_date"

# run table
RUN_FMS_ID = "run_fms_id"
RUN_NAME = "run_name"
RUN_INSTRUMENT = "run_instrument"
RUN_DATE = "run_date"

# readset table
READSET = "readset"
READSET_NAME = "readset_name"
READSET_LANE = "readset_lane"
READSET_ADAPTER1 = "readset_adapter1"
READSET_ADAPTER2 = "readset_adapter2"
READSET_SEQUENCING_TYPE = "readset_sequencing_type"
READSET_QUALITY_OFFSET = "readset_quality_offset"

# operation table
# Following is not coming from JSON
# OPERATION_PLATFORM = "operation_platform"
# OPERATION_CMD_LINE = "operation_cmd_line"
# OPERATION_NAME = "operation_name"
# OPERATION_STATUS = "operation_status"

# operation_config table
# Not sure if coming from JSON?
OPERATION_CONFIG_NAME = "operation_config_name"
OPERATION_CONFIG_VERSION = "operation_config_version"

# bundle table
# Not sure if coming from JSON?
BUNDLE_CONFIG_URI = "bundle_config_uri"
BUNDLE_URI = "bundle_uri"
BUNDLE_DELIVERABLE = "bundle_deliverable"

# file table
FILE = "file"
FILE_CONTENT = "file_content"
# FILE_TYPE = "file_type" # parsed from content while ingesting
# Not sure if coming from JSON?
FILE_DELIVERABLE = "file_deliverable"

# file table: event file
FILE_CONFIG_CONTENT = "file_config_content"
FILE_CONFIG_TYPE = "file_config_type"
FILE_EXTRA_METADATA = "file_extra_metadata"

# job table
# Following is not coming from JSON
# JOB_NAME = "job_name"
# JOB_START = "job_start"
# JOB_STOP = "job_stop"
# JOB_STATUS = "job_status"
# JOB_TYPE = "job_type" # What do we want for this one?

# metric table
METRIC = "metric"
METRIC_NAME = "metric_name"
METRIC_VALUE = "metric_value"
METRIC_FLAG = "metric_flag"
# Following is not coming from JSON
# Defining all metrics in the table?
# METRIC_RAW_READS_COUNT = "metric_raw_reads_count"
# METRIC_RAW_DUPLICATION_RATE = "metric_raw_duplication_rate"
# METRIC_RAW_MEDIAN_INSERT_SIZE = "metric_raw_median_insert_size"
# METRIC_RAW_MEAN_INSERT_SIZE = "metric_raw_mean_insert_size"
# METRIC_RAW_MEAN_COVERAGE = "metric_raw_mean_coverage"
