# Vocabulary and standard format entry

# Generic
DATE_LONG_FMT = "%Y-%m-%d %H:%M:%S"
DATE_FMT = "%Y-%m-%d"
TABLE = "table"
COLUMN = "column"
OLD = "old"
NEW = "new"
ID = "id"
MODIFICATION = "modification"
CASCADE = "cascade"
CASCADE_DOWN = "cascade_down"
CASCADE_UP = "cascade_up"
DELETE = "delete"

# project table
PROJECT_EXT_ID = "project_ext_id"
PROJECT_EXT_SRC = "project_ext_src"
PROJECT_NAME = "project_name"

# specimen table
SPECIMEN = "specimen"
SPECIMEN_ID = "specimen_id"
SPECIMEN_EXT_ID = "specimen_ext_id"
SPECIMEN_EXT_SRC = "specimen_ext_src"
SPECIMEN_NAME = "specimen_name"
SPECIMEN_COHORT = "specimen_cohort"
SPECIMEN_INSTITUTION = "specimen_institution"

# sample table
SAMPLE = "sample"
SAMPLE_ID = "sample_id"
SAMPLE_EXT_ID = "sample_ext_id"
SAMPLE_EXT_SRC = "sample_ext_src"
SAMPLE_NAME = "sample_name"
SAMPLE_ALIAS = "sample_alias"
SAMPLE_TUMOUR = "sample_tumour"

# experiment table
EXPERIMENT_SEQUENCING_TECHNOLOGY = "experiment_sequencing_technology"
EXPERIMENT_TYPE = "experiment_type"
EXPERIMENT_NUCLEIC_ACID_TYPE = "experiment_nucleic_acid_type"
EXPERIMENT_LIBRARY_KIT = "experiment_library_kit"
EXPERIMENT_KIT_EXPIRATION_DATE = "experiment_kit_expiration_date"
EXPERIMENT_TYPE_LIST = ["PCR-FREE", "RNASEQ"]

# run table
RUN = "run"
RUN_ID = "run_id"
RUN_EXT_ID = "run_ext_id"
RUN_EXT_SRC = "run_ext_src"
RUN_NAME = "run_name"
RUN_INSTRUMENT = "run_instrument"
RUN_DATE = "run_date"

# readset table
READSET = "readset"
READSET_ID = "readset_id"
READSET_NAME = "readset_name"
READSET_LANE = "readset_lane"
READSET_ADAPTER1 = "readset_adapter1"
READSET_ADAPTER2 = "readset_adapter2"
READSET_SEQUENCING_TYPE = "readset_sequencing_type"
READSET_QUALITY_OFFSET = "readset_quality_offset"

# operation table
OPERATION_PLATFORM = "operation_platform"
OPERATION_CMD_LINE = "operation_cmd_line"
OPERATION_NAME = "operation_name"
# Following is not coming from JSON
# OPERATION_STATUS = "operation_status"

# operation_config table
# Not sure if coming from JSON?
OPERATION_CONFIG_NAME = "operation_config_name"
OPERATION_CONFIG_VERSION = "operation_config_version"
OPERATION_CONFIG_MD5SUM = "operation_config_md5sum"
OPERATION_CONFIG_DATA = "operation_config_data"

# location table
# Not sure if coming from JSON?
LOCATION_URI = "location_uri"
SRC_LOCATION_URI = "src_location_uri"
DEST_LOCATION_URI = "dest_location_uri"
LOCATION_ENDPOINT = "location_endpoint"
LOCATION_DELIVERABLE = "location_deliverable"

# file table
FILE = "file"
FILE_NAME = "file_name"
FILE_DELIVERABLE = "file_deliverable"
FILE_EXTRA_METADATA = "file_extra_metadata"
# Not sure if coming from JSON?
# FILE_TYPE = "file_type" # parsed from content while ingesting

# job table
JOB = "job"
JOB_NAME = "job_name"
JOB_START = "job_start"
JOB_STOP = "job_stop"
JOB_STATUS = "job_status"
# Following is not coming from JSON
# JOB_TYPE = "job_type" # What do we want for this one?

# metric table
METRIC = "metric"
METRIC_NAME = "metric_name"
METRIC_VALUE = "metric_value"
METRIC_FLAG = "metric_flag"
METRIC_DELIVERABLE = "metric_deliverable"
# Following is not coming from JSON
# Defining all metrics in the table?
# METRIC_RAW_READS_COUNT = "metric_raw_reads_count"
# METRIC_RAW_DUPLICATION_RATE = "metric_raw_duplication_rate"
# METRIC_RAW_MEDIAN_INSERT_SIZE = "metric_raw_median_insert_size"
# METRIC_RAW_MEAN_INSERT_SIZE = "metric_raw_mean_insert_size"
# METRIC_RAW_MEAN_COVERAGE = "metric_raw_mean_coverage"
