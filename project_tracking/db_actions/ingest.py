"""
Ingesting data into the database
"""
# Standard library
import logging
import json
import os
import re
from datetime import datetime
from pathlib import Path

# Third-party
from sqlalchemy import select, inspect
from sqlalchemy.exc import SQLAlchemyError, IntegrityError

# Local modules
from .errors import DidNotFindError, RequestError, UniqueConstraintError, error_reference
from .route import projects
from .sample_group import check_sample_groups, add_samples_to_groups
from .utils import check_enum, check_one_nucleic_acid_type, sample_nucleic_acid_types
from ..analysis import RESERVED_OPERATION_NAMES, analysis_operations
from ..metrics import metric_vocabulary
from .metric_levels import NO_VALUE_FLAGS, check_metric_names, explains_no_value, has_value, store_metrics
from .. import vocabulary as vb
from .. import database
from ..model import (
    LaneEnum,
    NucleicAcidTypeEnum,
    PreservationEnum,
    SequencingTypeEnum,
    StatusEnum,
    FlagEnum,
    StateEnum,
    AggregateEnum,
    Specimen,
    Sample,
    Experiment,
    Run,
    Location,
    Readset,
    Operation,
    OperationConfig,
    Job,
    Metric,
    File
    )

logger = logging.getLogger(__name__)

def unique_constraint_error(session, json_format, ingest_data):
    """
    When unique constraint errors, checks which entity is causing the error and returns it/them as a list
    """
    ret = []
    if json_format == "run_processing":
        for specimen_json in ingest_data[vb.SPECIMEN]:
            for sample_json in specimen_json[vb.SAMPLE]:
                for readset_json in sample_json[vb.READSET]:
                    readset_name = readset_json[vb.READSET_NAME]
                    stmt = select(Readset).where(Readset.name == readset_name)
                    readset = session.execute(stmt).scalar_one_or_none()
                    if readset:
                        ret.append(f"'Readset' with 'name' '{readset_name}' already exists in the database and 'name' has to be unique")
    return ret

ONCOTREE_CODE_PATTERN = re.compile(r"^[A-Z0-9]+$")


def normalize_tumour_metadata(ingest_data):
    """
    Uppercase every sample OncoTree code and replace the spaces of every cancer type with underscores in place;
    raises RequestError listing all malformed ones. An empty cancer type is dropped.
    """
    invalid = []
    for specimen_json in ingest_data[vb.SPECIMEN]:
        for sample_json in specimen_json[vb.SAMPLE]:
            cancer_type = sample_json.get(vb.SAMPLE_CANCER_TYPE)
            if cancer_type is not None:
                if not isinstance(cancer_type, str):
                    invalid.append(f"'Sample' '{sample_json[vb.SAMPLE_NAME]}' has an invalid 'cancer_type' {cancer_type!r} (expected a string, e.g. 'Lung Adenocarcinoma').")
                else:
                    # Underscores keep it one word on the command lines of tools such as oncoanalyser
                    sample_json[vb.SAMPLE_CANCER_TYPE] = "_".join(cancer_type.split()) or None
            code = sample_json.get(vb.SAMPLE_ONCOTREE_CODE)
            if code is None:
                continue
            code = str(code).strip().upper()
            if not ONCOTREE_CODE_PATTERN.match(code):
                invalid.append(f"'Sample' '{sample_json[vb.SAMPLE_NAME]}' has an invalid 'oncotree_code' '{sample_json[vb.SAMPLE_ONCOTREE_CODE]}' (expected letters and digits only, e.g. 'LUAD').")
            sample_json[vb.SAMPLE_ONCOTREE_CODE] = code
    if invalid:
        raise RequestError(message=invalid)


MD5SUM_RE = re.compile(r"[0-9a-f]{32}")


def check_md5sum(invalid, missing, file_json, allow_missing):
    """
    Check the md5sum of a payload's file: 32 hexadecimal characters, stored in lowercase. A missing or empty one is
    refused, unless allow_missing (files no longer on disk, e.g. payloads rebuilt for past runs): it is then stored as
    None and the file name added to missing.
    """
    name = file_json[vb.FILE_NAME]
    value = file_json.get(vb.FILE_MD5SUM)
    if isinstance(value, str):
        value = value.strip().lower() or None
    if value is None:
        if allow_missing:
            file_json[vb.FILE_MD5SUM] = None
            missing.append(name)
        else:
            invalid.append(f"'File' '{name}' has no '{vb.FILE_MD5SUM}' (send '{vb.ALLOW_MISSING_MD5SUM}': true only for files no longer on disk).")
        return
    if not isinstance(value, str) or not MD5SUM_RE.fullmatch(value):
        invalid.append(f"'File' '{name}' has an invalid '{vb.FILE_MD5SUM}' {file_json[vb.FILE_MD5SUM]!r} (expected 32 hexadecimal characters).")
        return
    file_json[vb.FILE_MD5SUM] = value


def missing_md5sum_warning(missing):
    """Warning listing the files ingested without md5sum, or None."""
    if missing:
        return f"{len(missing)} file(s) ingested without md5sum ('{vb.ALLOW_MISSING_MD5SUM}'): {', '.join(missing)}."
    return None


# Shape of each ingest payload, checked by check_payload_shape before anything else reads it. At each level:
# "required" keys must be there (null allowed), "not_null" keys there, not null and not empty, "dates" null, empty or in their
# format (GenPipes sends an empty job_start for a job that didn't run),
# "lists" the lists of objects under it ("optional" ones may be missing or null), "label" and "name" name an
# object in the messages.
_JOB_TIMES = {vb.JOB_START: vb.DATE_LONG_FMT, vb.JOB_STOP: vb.DATE_LONG_FMT}
_METRIC_SHAPE = {"label": "Metric", "name": vb.METRIC_NAME, "not_null": (vb.METRIC_NAME,), "required": (vb.METRIC_VALUE,)}
_FILE_SHAPE = {"label": "File", "name": vb.FILE_NAME, "not_null": (vb.FILE_NAME, vb.LOCATION_URI)}
_COPY_SHAPE = {
    "not_null": (vb.OPERATION_PLATFORM,),
    "required": (vb.OPERATION_CMD_LINE,),
    "dates": _JOB_TIMES,
    "lists": {vb.READSET: {
        "label": "Readset", "name": vb.READSET_NAME, "not_null": (vb.READSET_NAME,),
        "lists": {vb.FILE: {
            "label": "File", "name": vb.SRC_LOCATION_URI, "not_null": (vb.SRC_LOCATION_URI, vb.DEST_LOCATION_URI),
        }},
    }},
}
PAYLOAD_SHAPES = {
    "run_processing": {
        "not_null": (vb.OPERATION_PLATFORM, vb.RUN_NAME, vb.RUN_DATE),
        "required": (vb.RUN_EXT_ID, vb.RUN_EXT_SRC, vb.RUN_INSTRUMENT),
        "dates": {vb.RUN_DATE: vb.DATE_LONG_FMT, **_JOB_TIMES},
        "lists": {vb.SPECIMEN: {
            "label": "Specimen", "name": vb.SPECIMEN_NAME, "not_null": (vb.SPECIMEN_NAME,),
            "required": (vb.SPECIMEN_COHORT, vb.SPECIMEN_INSTITUTION),
            "lists": {vb.SAMPLE: {
                "label": "Sample", "name": vb.SAMPLE_NAME, "not_null": (vb.SAMPLE_NAME,), "required": (vb.SAMPLE_TUMOUR,),
                "lists": {vb.READSET: {
                    "label": "Readset", "name": vb.READSET_NAME,
                    "not_null": (vb.READSET_NAME,),
                    "required": (vb.EXPERIMENT_SEQUENCING_TECHNOLOGY, vb.EXPERIMENT_TYPE, vb.EXPERIMENT_LIBRARY_KIT,
                                 vb.EXPERIMENT_KIT_EXPIRATION_DATE, vb.READSET_ADAPTER1, vb.READSET_ADAPTER2),
                    "dates": {vb.EXPERIMENT_KIT_EXPIRATION_DATE: vb.DATE_FMT},
                    "lists": {vb.FILE: _FILE_SHAPE, vb.METRIC: _METRIC_SHAPE},
                }},
            }},
        }},
    },
    "transfer": _COPY_SHAPE,
    "delivery": _COPY_SHAPE,
    "analysis": {
        "not_null": (vb.OPERATION_PLATFORM, vb.OPERATION_CONFIG_NAME, vb.OPERATION_CONFIG_DATA),
        "required": (vb.OPERATION_CMD_LINE, vb.OPERATION_CONFIG_VERSION, vb.OPERATION_CONFIG_MD5SUM),
        "lists": {vb.SAMPLE: {
            "label": "Sample", "name": vb.SAMPLE_NAME, "not_null": (vb.SAMPLE_NAME,),
            "lists": {vb.READSET: {
                "label": "Readset", "name": vb.READSET_NAME, "not_null": (vb.READSET_NAME,),
                "lists": {vb.JOB: {
                    "label": "Job", "name": vb.JOB_NAME, "not_null": (vb.JOB_NAME,), "dates": _JOB_TIMES,
                    "lists": {vb.FILE: {**_FILE_SHAPE, "optional": True}, vb.METRIC: {**_METRIC_SHAPE, "optional": True}},
                }},
            }},
        }},
    },
}


def check_payload_shape(ingest_data, kind):
    """
    Check an ingest payload has the keys, lists and dates its route reads (PAYLOAD_SHAPES[kind]) before anything
    else reads it; raises RequestError listing every problem, so a malformed payload gets a 400 saying what to fix
    instead of a 500.
    """
    if not isinstance(ingest_data, dict):
        raise RequestError(message=[f"The {kind} payload must be a JSON object, got {type(ingest_data).__name__}."])
    invalid = []
    _check_shape(invalid, ingest_data, PAYLOAD_SHAPES[kind], f"The {kind} payload", None)
    if invalid:
        raise RequestError(message=invalid)


def _shown(value):
    """A payload value as messages quote it, cut to 80 characters."""
    text = repr(value)
    return text if len(text) <= 80 else text[:77] + "..."


def _check_shape(invalid, obj, shape, where, own):
    """Append to invalid the problems of obj (named where in messages; own: its name without its parent)."""
    for key in shape.get("required", ()):
        if key not in obj:
            invalid.append(f"{where} has no '{key}'.")
    for key in shape.get("not_null", ()):
        if obj.get(key) is None or obj.get(key) == "":
            invalid.append(f"{where} has no '{key}'" + (f" ({_shown(obj[key])})." if key in obj else "."))
    for key, fmt in shape.get("dates", {}).items():
        value = obj.get(key)
        if value is None or value == "":
            continue
        try:
            datetime.strptime(value, fmt)
        except (TypeError, ValueError):
            invalid.append(f"{where} has an invalid '{key}' {_shown(value)} (expected e.g. '{datetime(2024, 1, 31, 14, 5).strftime(fmt)}').")
    for key, sub in shape.get("lists", {}).items():
        items = obj.get(key)
        if items is None and sub.get("optional"):
            continue
        if not isinstance(items, list):
            invalid.append(f"{where} has no list '{key}'" + (f" (got {_shown(items)})." if key in obj else "."))
            continue
        for number, item in enumerate(items, 1):
            name = item.get(sub["name"]) if isinstance(item, dict) else None
            item_own = f"'{sub['label']}' '{name}'" if isinstance(name, str) and name else f"'{sub['label']}' #{number}"
            item_where = f"{item_own} of {own}" if own else item_own
            if not isinstance(item, dict):
                invalid.append(f"{item_where} must be a JSON object, got {_shown(item)}.")
                continue
            _check_shape(invalid, item, sub, item_where, item_own)


def strip_metric_value(metric_json):
    """Drop the spaces and line breaks around a text metric value, e.g. '0.0001\\n' left by the tool's output."""
    value = metric_json.get(vb.METRIC_VALUE)
    if isinstance(value, str):
        metric_json[vb.METRIC_VALUE] = value.strip()


def check_run_processing_payload(ingest_data):
    """
    Check every enum field of a run_processing payload before anything is written; raises RequestError
    listing all invalid values. Values must be exact (e.g. 'ON_HOLD', lane '1'); readset_state and
    metric_flag and readset_preservation may be null or missing. Also trims readset_project_year and readset_project_phase (free text,
    a number becomes text, empty becomes None), rejecting other types, and checks every file's md5sum
    (check_md5sum). Returns the names of the files allowed without md5sum.
    """
    invalid = []
    missing = []
    allow_missing = ingest_data.get(vb.ALLOW_MISSING_MD5SUM) is True
    for specimen_json in ingest_data[vb.SPECIMEN]:
        for sample_json in specimen_json[vb.SAMPLE]:
            for readset_json in sample_json[vb.READSET]:
                name = readset_json[vb.READSET_NAME]
                check_enum(invalid, LaneEnum, readset_json.get(vb.READSET_LANE), vb.READSET_LANE, "Readset", name)
                check_enum(invalid, NucleicAcidTypeEnum, readset_json.get(vb.EXPERIMENT_NUCLEIC_ACID_TYPE), vb.EXPERIMENT_NUCLEIC_ACID_TYPE, "Readset", name)
                check_enum(invalid, SequencingTypeEnum, readset_json.get(vb.READSET_SEQUENCING_TYPE), vb.READSET_SEQUENCING_TYPE, "Readset", name)
                check_enum(invalid, StateEnum, readset_json.get(vb.READSET_STATE), vb.READSET_STATE, "Readset", name, optional=True)
                for key in (vb.READSET_PROJECT_YEAR, vb.READSET_PROJECT_PHASE):
                    value = readset_json.get(key)
                    if value is None:
                        continue
                    if isinstance(value, bool) or not isinstance(value, (str, int)):
                        invalid.append(f"'Readset' '{name}' has an invalid '{key}' {value!r} (expected text).")
                        continue
                    readset_json[key] = str(value).strip() or None
                check_enum(invalid, PreservationEnum, readset_json.get(vb.READSET_PRESERVATION), vb.READSET_PRESERVATION, "Readset", name, optional=True)
                for metric_json in readset_json[vb.METRIC]:
                    strip_metric_value(metric_json)
                    check_enum(invalid, FlagEnum, metric_json.get(vb.METRIC_FLAG), vb.METRIC_FLAG, "Metric", f"{metric_json[vb.METRIC_NAME]}' of 'Readset' '{name}", optional=True)
                for file_json in readset_json[vb.FILE]:
                    check_md5sum(invalid, missing, file_json, allow_missing)
    if invalid:
        raise RequestError(message=invalid)
    return missing


def check_sample_nucleic_acid_type(ingest_data, session):
    """
    A sample has one nucleic acid type: raises RequestError listing every sample whose readsets, in the payload and
    in the database (deprecated and deleted ones too), would have several experiment_nucleic_acid_type.
    """
    readsets_by_type = {}
    for specimen_json in ingest_data[vb.SPECIMEN]:
        for sample_json in specimen_json[vb.SAMPLE]:
            types = readsets_by_type.setdefault(sample_json[vb.SAMPLE_NAME], {})
            for readset_json in sample_json[vb.READSET]:
                types.setdefault(readset_json[vb.EXPERIMENT_NUCLEIC_ACID_TYPE], []).append(readset_json[vb.READSET_NAME])
    sample_nucleic_acid_types(session, Sample.name.in_(readsets_by_type), readsets_by_type)
    check_one_nucleic_acid_type(
        readsets_by_type, "give each nucleic acid type its own sample, or fix or curate the readsets of the other types."
    )


def _allowed(values):
    return ", ".join(repr(value) for value in values) or "none"


def check_analysis_operation(invalid, ingest_data):
    """
    Append a message to invalid for each operation_name, operation_pipeline or operation_protocol not allowed
    by the analysis operations vocabulary.
    """
    vocabulary = analysis_operations()
    name = ingest_data.get(vb.OPERATION_NAME)
    pipeline = ingest_data.get(vb.OPERATION_PIPELINE)
    protocol = ingest_data.get(vb.OPERATION_PROTOCOL)
    if name in RESERVED_OPERATION_NAMES:
        invalid.append(f"'{vb.OPERATION_NAME}' {name!r} is reserved: expected one of {_allowed(vocabulary)}.")
        return
    if name not in vocabulary:
        invalid.append(f"'{vb.OPERATION_NAME}' {name!r} is not an allowed analysis operation: expected one of {_allowed(vocabulary)}.")
        return
    pipelines = vocabulary[name]
    if not pipelines:
        if pipeline is not None:
            invalid.append(f"'{vb.OPERATION_NAME}' {name!r} takes no '{vb.OPERATION_PIPELINE}', got {pipeline!r}.")
        return
    if pipeline not in pipelines:
        invalid.append(f"'{vb.OPERATION_PIPELINE}' {pipeline!r} is not allowed for {name!r}: expected one of {_allowed(pipelines)}.")
        return
    protocols = pipelines[pipeline]
    if not protocols:
        if protocol is not None:
            invalid.append(f"'{vb.OPERATION_PIPELINE}' {pipeline!r} takes no '{vb.OPERATION_PROTOCOL}', got {protocol!r}.")
    elif protocol not in protocols:
        invalid.append(f"'{vb.OPERATION_PROTOCOL}' {protocol!r} is not allowed for {name!r} {pipeline!r}: expected one of {_allowed(protocols)}.")


def check_analysis_payload(ingest_data):
    """
    Check an analysis payload before anything is written: the operation name, pipeline and protocol against the
    analysis operations vocabulary, every enum field (operation_status, job_status and metric_flag may be null
    or missing) and every file's md5sum (check_md5sum). Raises RequestError listing all problems; returns the names
    of the files allowed without md5sum.
    """
    invalid = []
    missing = []
    allow_missing = ingest_data.get(vb.ALLOW_MISSING_MD5SUM) is True
    check_analysis_operation(invalid, ingest_data)
    check_enum(invalid, StatusEnum, ingest_data.get(vb.OPERATION_STATUS), vb.OPERATION_STATUS, "Operation",
               ingest_data.get(vb.OPERATION_NAME), optional=True)
    for sample_json in ingest_data[vb.SAMPLE]:
        for readset_json in sample_json[vb.READSET]:
            for job_json in readset_json[vb.JOB]:
                name = f"{job_json[vb.JOB_NAME]}' of 'Readset' '{readset_json[vb.READSET_NAME]}"
                check_enum(invalid, StatusEnum, job_json.get(vb.JOB_STATUS) or None, vb.JOB_STATUS, "Job", name, optional=True)
                check_job_times(invalid, job_json, name)
                if job_json.get(vb.METRIC) and (job_json.get(vb.JOB_STATUS) != "COMPLETED" or not job_json.get(vb.JOB_STOP)):
                    # The newest job's metric wins (metric_levels.py): only a finished job can carry one
                    invalid.append(f"'Job' '{name}' has metrics: only a COMPLETED job with a '{vb.JOB_STOP}' can carry metrics.")
                for metric_json in job_json.get(vb.METRIC, []):
                    strip_metric_value(metric_json)
                    check_enum(invalid, FlagEnum, metric_json.get(vb.METRIC_FLAG), vb.METRIC_FLAG, "Metric", f"{metric_json[vb.METRIC_NAME]}' of 'Job' '{name}", optional=True)
                for file_json in job_json.get(vb.FILE, []):
                    check_md5sum(invalid, missing, file_json, allow_missing)
    check_metric_names(invalid, ingest_data, metric_vocabulary())
    if invalid:
        raise RequestError(message=invalid)
    return missing


def check_job_times(invalid, job_json, name):
    """
    A job's times must fit its status: PENDING has no job_start nor job_stop, RUNNING a job_start and no job_stop,
    a finished job a job_stop (a CANCELLED one may never have started). GenPipes sends a job that
    didn't run without status: it is skipped. Job.from_attributes relies on it to tell
    the same attempt progressing from a new one.
    """
    status = job_json.get(vb.JOB_STATUS) or None
    start, stop = bool(job_json.get(vb.JOB_START)), bool(job_json.get(vb.JOB_STOP))
    if status == StatusEnum.PENDING.value and (start or stop):
        invalid.append(f"'Job' '{name}' is PENDING: it can't have a '{vb.JOB_START}' nor a '{vb.JOB_STOP}'.")
    elif status == StatusEnum.RUNNING.value and (not start or stop):
        invalid.append(f"'Job' '{name}' is RUNNING: it needs a '{vb.JOB_START}' and no '{vb.JOB_STOP}'.")
    elif status in {s.value for s in StatusEnum} - {"PENDING", "RUNNING"} and not stop:
        invalid.append(f"'Job' '{name}' is {status}: it needs a '{vb.JOB_STOP}'.")


def check_transfer_payload(ingest_data):
    """
    Check the optional readset_state of each readset of a transfer payload before anything is written; raises
    RequestError listing all invalid values.
    """
    invalid = []
    for readset_json in ingest_data[vb.READSET]:
        check_enum(invalid, StateEnum, readset_json.get(vb.READSET_STATE), vb.READSET_STATE, "Readset",
                   readset_json[vb.READSET_NAME], optional=True)
    if invalid:
        raise RequestError(message=invalid)


def analysis_operation_status(ingest_data):
    """
    Status of an analysis operation: operation_status when given, otherwise COMPLETED when every job of the payload
    is COMPLETED and RUNNING when any job isn't (failed, pending or without status: the run isn't finished).
    """
    if ingest_data.get(vb.OPERATION_STATUS):
        return StatusEnum(ingest_data[vb.OPERATION_STATUS])
    statuses = [
        job_json.get(vb.JOB_STATUS)
        for sample_json in ingest_data[vb.SAMPLE]
        for readset_json in sample_json[vb.READSET]
        for job_json in readset_json[vb.JOB]
    ]
    if statuses and all(status == StatusEnum.COMPLETED.value for status in statuses):
        return StatusEnum.COMPLETED
    return StatusEnum.RUNNING


def ingest_run_processing(project_id: str, ingest_data: dict, session):
    """Ingesting run for MoH"""
    if isinstance(ingest_data, (str, bytes)):
        ingest_data = json.loads(ingest_data)

    ret = {
        "DB_ACTION_WARNING": [],
        "DB_ACTION_OUTPUT": []
        }

    project = projects(project_id=project_id, session=session)["DB_ACTION_OUTPUT"][0]

    check_payload_shape(ingest_data, "run_processing")
    force = ingest_data.get(vb.FORCE, False)

    # Looking for potential readsets duplicates before starting the ingestion to be able to report all of them at once
    message = unique_constraint_error(session, "run_processing", ingest_data)
    if message:
        raise UniqueConstraintError(message=message)
    normalize_tumour_metadata(ingest_data)
    check_sample_groups(ingest_data, session)
    missing_md5sum = check_run_processing_payload(ingest_data)
    check_sample_nucleic_acid_type(ingest_data, session)
    if missing_md5sum:
        ret["DB_ACTION_WARNING"].append(missing_md5sum_warning(missing_md5sum))
    # Run metrics are always per readset: the vocabulary only gives their aggregate, an unlisted one is warned about
    run_metrics = metric_vocabulary().get("run_processing", {})
    unknown_run_metrics = set()
    unexplained_run_metrics = set()

    operation, warning = Operation.from_attributes(
        platform=ingest_data[vb.OPERATION_PLATFORM],
        name="run_processing",
        status=StatusEnum("COMPLETED"),
        project=project,
        session=session
    )
    if warning:
        ret["DB_ACTION_WARNING"].append(warning)

    try:
        job_start_value = ingest_data.get(vb.JOB_START) or None
        if job_start_value is not None:
            job_start = datetime.strptime(job_start_value, vb.DATE_LONG_FMT)
        else:
            job_start = datetime.now()
    except TypeError:
        job_start = datetime.now()
    try:
        job_stop_value = ingest_data.get(vb.JOB_STOP) or None
        if job_stop_value is not None:
            job_stop = datetime.strptime(job_stop_value, vb.DATE_LONG_FMT)
        else:
            job_stop = datetime.now()
    except TypeError:
        job_stop = datetime.now()
    job, warning = Job.from_attributes(
        name="run_processing",
        status=StatusEnum("COMPLETED"),
        start=job_start,
        stop=job_stop,
        operation=operation,
        session=session
        )
    if warning:
        ret["DB_ACTION_WARNING"].append(warning)
    session.add(job)
    # Defining Run
    run, warning = Run.from_attributes(
        ext_id=ingest_data[vb.RUN_EXT_ID],
        ext_src=ingest_data[vb.RUN_EXT_SRC],
        name=ingest_data[vb.RUN_NAME],
        instrument=ingest_data[vb.RUN_INSTRUMENT],
        date=datetime.strptime(ingest_data[vb.RUN_DATE], vb.DATE_LONG_FMT),
        session=session
        )
    if warning:
        ret["DB_ACTION_WARNING"].append(warning)

    samples_groups = []
    for specimen_json in ingest_data[vb.SPECIMEN]:
        specimen = Specimen.from_name(
            name=specimen_json[vb.SPECIMEN_NAME],
            cohort=specimen_json[vb.SPECIMEN_COHORT],
            institution=specimen_json[vb.SPECIMEN_INSTITUTION],
            project=project,
            session=session
            )
        for sample_json in specimen_json[vb.SAMPLE]:
            if vb.SAMPLE_ALIAS in sample_json:
                sample_alias = sample_json[vb.SAMPLE_ALIAS]
            else:
                sample_alias = None
            sample = Sample.from_name(
                name=sample_json[vb.SAMPLE_NAME],
                alias=sample_alias,
                tumour=sample_json[vb.SAMPLE_TUMOUR],
                specimen=specimen,
                session=session
                )
            if sample_json.get(vb.SAMPLE_ONCOTREE_CODE):
                warning = sample.set_oncotree_code(sample_json[vb.SAMPLE_ONCOTREE_CODE], force=force)
                if warning:
                    ret["DB_ACTION_WARNING"].append(warning)
            if sample_json.get(vb.SAMPLE_CANCER_TYPE):
                warning = sample.set_cancer_type(sample_json[vb.SAMPLE_CANCER_TYPE], force=force)
                if warning:
                    ret["DB_ACTION_WARNING"].append(warning)
            if sample_json.get(vb.SAMPLE_GROUP_NAME):
                samples_groups.append((sample, sample_json[vb.SAMPLE_GROUP_NAME]))
            for readset_json in sample_json[vb.READSET]:
                if readset_json[vb.EXPERIMENT_KIT_EXPIRATION_DATE]:
                    kit_expiration_date = datetime.strptime(readset_json[vb.EXPERIMENT_KIT_EXPIRATION_DATE], vb.DATE_FMT)
                else:
                    kit_expiration_date = None
                # Defining Experiment
                experiment, warning = Experiment.from_attributes(
                    sequencing_technology=readset_json[vb.EXPERIMENT_SEQUENCING_TECHNOLOGY],
                    type=readset_json[vb.EXPERIMENT_TYPE],
                    nucleic_acid_type=readset_json[vb.EXPERIMENT_NUCLEIC_ACID_TYPE],
                    library_kit=readset_json[vb.EXPERIMENT_LIBRARY_KIT],
                    kit_expiration_date=kit_expiration_date,
                    session=session
                    )
                if warning:
                    ret["DB_ACTION_WARNING"].append(warning)

                readset = Readset(
                    name=readset_json[vb.READSET_NAME],
                    lane=LaneEnum(readset_json[vb.READSET_LANE]),
                    adapter1=readset_json[vb.READSET_ADAPTER1],
                    adapter2=readset_json[vb.READSET_ADAPTER2],
                    sequencing_type=SequencingTypeEnum(readset_json[vb.READSET_SEQUENCING_TYPE]),
                    state=StateEnum(readset_json.get(vb.READSET_STATE) or StateEnum.VALID.value),
                    project_year=readset_json.get(vb.READSET_PROJECT_YEAR),
                    project_phase=readset_json.get(vb.READSET_PROJECT_PHASE),
                    preservation=PreservationEnum(readset_json[vb.READSET_PRESERVATION]) if readset_json.get(vb.READSET_PRESERVATION) else None,
                    sample=sample,
                    experiment=experiment,
                    run=run,
                    operations=[operation],
                    jobs=[job]
                    )
                for file_json in readset_json[vb.FILE]:
                    suffixes = Path(file_json[vb.FILE_NAME]).suffixes
                    file_type = os.path.splitext(file_json[vb.FILE_NAME])[-1][1:]
                    if ".gz" in suffixes:
                        index = suffixes.index(".gz")
                        file_type = "".join(suffixes[index - 1:]).replace(".", "", 1)
                        if file_type.startswith("."):
                            file_type = file_type[1:]
                    if vb.FILE_DELIVERABLE in file_json:
                        file_deliverable = file_json[vb.FILE_DELIVERABLE]
                    else:
                        file_deliverable = False
                    # Need to have the following otherwise assigning extra_metadata to None converts null into json in the db
                    if vb.FILE_EXTRA_METADATA in file_json.keys():
                        # No need to use .from_attributes because run_processing is ingested only one time for a given Readset
                        file = File(
                            name=file_json[vb.FILE_NAME],
                            md5sum=file_json.get(vb.FILE_MD5SUM, None),
                            type=file_type,
                            extra_metadata=file_json[vb.FILE_EXTRA_METADATA],
                            deliverable=file_deliverable,
                            readsets=[readset],
                            jobs=[job]
                            )
                    else:
                        # No need to use .from_attributes because run_processing is ingested only one time for a given Readset
                        file = File(
                            name=file_json[vb.FILE_NAME],
                            md5sum=file_json.get(vb.FILE_MD5SUM, None),
                            type=file_type,
                            deliverable=file_deliverable,
                            readsets=[readset],
                            jobs=[job]
                            )
                    location, warning = Location.from_uri(uri=file_json[vb.LOCATION_URI], file=file, session=session)
                    if warning:
                        ret["DB_ACTION_WARNING"].append(warning)
                    # Before adding a new location for the current File make sure an existing one doesn't exist otherwise update it
                    if location not in file.locations:
                        if location.deleted:
                            ret["DB_ACTION_WARNING"].append(f"Existing deleted location found for uri '{location.uri}', undeleting it.")
                            location.deleted = False
                        if location.deprecated:
                            ret["DB_ACTION_WARNING"].append(f"Existing deprecated location found for uri '{location.uri}', undeprecating it.")
                            location.deprecated = False
                        file.locations.append(location)
                for metric_json in readset_json[vb.METRIC]:
                    if not has_value(metric_json) and not explains_no_value(metric_json):
                        unexplained_run_metrics.add(f"{metric_json[vb.METRIC_NAME]}' of 'Readset' '{readset.name}")
                        continue
                    if vb.METRIC_DELIVERABLE in metric_json:
                        metric_deliverable = metric_json[vb.METRIC_DELIVERABLE]
                    else:
                        metric_deliverable = False
                    metric_flag = FlagEnum(metric_json[vb.METRIC_FLAG]) if metric_json.get(vb.METRIC_FLAG) else None
                    entry = run_metrics.get(metric_json[vb.METRIC_NAME])
                    if entry is None:
                        unknown_run_metrics.add(metric_json[vb.METRIC_NAME])
                    metric, warning = Metric.get_or_create(
                        name=metric_json[vb.METRIC_NAME],
                        value=metric_json[vb.METRIC_VALUE],
                        flag=metric_flag,
                        deliverable=metric_deliverable,
                        job=job,
                        readsets=[readset],
                        session=session,
                        dont_merge=True
                        )
                    if entry and entry.get("aggregate"):
                        metric.aggregate = AggregateEnum(entry["aggregate"])
                    if warning:
                        ret["DB_ACTION_WARNING"].append(warning)

                session.add(readset)
            try:
                session.flush()
            except IntegrityError as error:
                session.rollback()
                message = unique_constraint_error(session, "run_processing", ingest_data)
                if not message:
                    # The database's message (constraint, SQL, values) stays in the log
                    reference = error_reference()
                    logger.error("Integrity error %s in run_processing: %s", reference, error.orig, exc_info=error)
                    raise UniqueConstraintError(
                        message=f"The database refused the run (a duplicate or a missing value; logged, reference "
                                f"{reference}): nothing was saved."
                    ) from error
                raise UniqueConstraintError(message=message) from error

    ret["DB_ACTION_WARNING"].extend(add_samples_to_groups(samples_groups, session))
    if unexplained_run_metrics:
        ret["DB_ACTION_WARNING"].append(
            f"'Metric' '{sorted(unexplained_run_metrics)[0]}'"
            + (f" and {len(unexplained_run_metrics) - 1} more" if len(unexplained_run_metrics) > 1 else "")
            + f" without value and without a flag among {sorted(NO_VALUE_FLAGS)} to explain it: not stored."
        )
    if unknown_run_metrics:
        ret["DB_ACTION_WARNING"].append(
            f"Metrics {sorted(unknown_run_metrics)} aren't in the run_processing metric vocabulary (GET /vocabulary): "
            f"stored per readset, without unit nor aggregate."
        )

    operation_id = operation.id
    job_id = job.id

    try:
        session.commit()
    except SQLAlchemyError as error:
        session.rollback()
        logger.error("Error: %s", error)

    # operation
    stmt = select(Operation).where(Operation.id == operation_id)
    operation = session.execute(stmt).scalar_one_or_none()
    # job
    stmt = select(Job).where(Job.id == job_id)
    job = session.execute(stmt).scalar_one_or_none()

    ret["DB_ACTION_OUTPUT"].append(operation)
    # If no warning
    if not ret["DB_ACTION_WARNING"]:
        ret.pop("DB_ACTION_WARNING")

    return ret


def attach_destination(file, uri, session, warnings):
    """
    Give file the location of uri, the destination of a transfer or delivery: the bytes there are now file's, as the
    last copy wins. A location another file owned moves to file (issue #81: it used to stay on the other file and
    leave file without location). The other file is then deprecated, unless it keeps another live location, with
    extra_metadata 'duplicate_of' (same md5sum) or 'superseded_by' (different or unknown md5sum) set to file's id.
    Undeletes and undeprecates the location. Appends a message to warnings for anything but a new location or one
    file already owns (a replay).
    """
    stmt = select(Location).where(Location.uri == uri)
    location = session.execute(stmt).scalar_one_or_none()
    if location is None:
        location, _ = Location.from_uri(uri=uri, file=file, session=session)
    elif location.file is not file:
        warnings.append(move_location(location, file))
    if location.deleted:
        warnings.append(f"Existing deleted location found for uri '{location.uri}', undeleting it.")
        location.deleted = False
    if location.deprecated:
        warnings.append(f"Existing deprecated location found for uri '{location.uri}', undeprecating it.")
        location.deprecated = False
    return location


def move_location(location, file):
    """Move location from the file owning it to file (see attach_destination); returns the warning."""
    owner = location.file
    message = f"Location '{location.uri}' moved from 'File' {owner.id} to 'File' {file.id} '{file.name}'"
    if owner.md5sum is None or file.md5sum is None:
        same, detail = False, "md5sum not verified"
    elif owner.md5sum == file.md5sum:
        same, detail = True, f"same md5sum {file.md5sum}"
    else:
        same, detail = False, f"md5sum {owner.md5sum} replaced by {file.md5sum}"
    message += f" ({detail})"
    if owner.creation and file.creation and file.creation < owner.creation:
        message += f"; 'File' {file.id} is older than 'File' {owner.id}: check an older run wasn't copied over a newer one"
    keeps_location = any(
        other is not location and not other.deleted and not other.deprecated for other in owner.locations
    )
    location.file = file
    if owner.deprecated or owner.deleted:
        return message + "."
    if keeps_location:
        return message + f"; 'File' {owner.id} keeps its other locations."
    key = "duplicate_of" if same else "superseded_by"
    owner.extra_metadata = {**(owner.extra_metadata or {}), key: file.id}
    owner.deprecated = True
    return message + f"; 'File' {owner.id} is deprecated ({key} {file.id})."


def ingest_transfer(project_id: str, ingest_data, session, check_readset_name=True):
    """Ingesting transfer"""
    if isinstance(ingest_data, (str, bytes)):
        ingest_data = json.loads(ingest_data)

    if not session:
        session = database.get_session()

    ret = {
        "DB_ACTION_WARNING": [],
        "DB_ACTION_OUTPUT": []
    }

    project = projects(project_id=project_id, session=session)["DB_ACTION_OUTPUT"][0]

    check_payload_shape(ingest_data, "transfer")
    check_transfer_payload(ingest_data)

    operation, warning = Operation.from_attributes(
        platform=ingest_data[vb.OPERATION_PLATFORM],
        name="transfer",
        cmd_line=ingest_data[vb.OPERATION_CMD_LINE],
        status=StatusEnum("COMPLETED"),
        project=project,
        session=session
    )
    if warning:
        ret["DB_ACTION_WARNING"].append(warning)

    try:
        job_start_value = ingest_data.get(vb.JOB_START) or None
        if job_start_value is not None:
            job_start = datetime.strptime(job_start_value, vb.DATE_LONG_FMT)
        else:
            job_start = datetime.now()
    except TypeError:
        job_start = datetime.now()
    try:
        job_stop_value = ingest_data.get(vb.JOB_STOP) or None
        if job_stop_value is not None:
            job_stop = datetime.strptime(job_stop_value, vb.DATE_LONG_FMT)
        else:
            job_stop = datetime.now()
    except TypeError:
        job_stop = datetime.now()
    job, warning = Job.from_attributes(
        name="transfer",
        status=StatusEnum("COMPLETED"),
        start=job_start,
        stop=job_stop,
        operation=operation,
        session=session
    )
    if warning:
        ret["DB_ACTION_WARNING"].append(warning)
    # session.add(job)
    readset_list = []
    for readset_json in ingest_data[vb.READSET]:
        readset_name = readset_json[vb.READSET_NAME]
        stmt = select(Readset).where(
            Readset.name == readset_name,
            Readset.deprecated.is_(False),
            Readset.deleted.is_(False)
        )
        readset = session.execute(stmt).scalar_one_or_none()
        if not readset:
            raise DidNotFindError(f"No 'Readset' with 'name' '{readset_name}'")
        readset_list.append(readset)
        # Optional new state of the readset, checked by check_transfer_payload
        state = readset_json.get(vb.READSET_STATE)
        if state and readset.state != StateEnum(state):
            previous = readset.state.value if readset.state else None
            ret["DB_ACTION_WARNING"].append(
                f"'Readset' '{readset_name}' state changed from '{previous}' to '{state}'."
            )
            readset.state = StateEnum(state)
        for file_json in readset_json[vb.FILE]:
            src_uri = file_json[vb.SRC_LOCATION_URI]
            dest_uri = file_json[vb.DEST_LOCATION_URI]
            if check_readset_name:
                stmt = (
                    select(File)
                    .where(File.deprecated.is_(False), File.deleted.is_(False))
                    .join(File.readsets)
                    .where(Readset.name == readset_name)
                    .join(File.locations)
                    .where(Location.uri == src_uri)
                )
                file = session.execute(stmt).scalar_one_or_none()
                if not file:
                    raise DidNotFindError(f"No 'File' with 'uri' '{src_uri}' and 'Readset' with 'name' '{readset_name}'")
            else:
                stmt = (
                    select(File)
                    .where(File.deprecated.is_(False), File.deleted.is_(False))
                    .join(File.locations)
                    .where(Location.uri == src_uri)
                )
                file = session.execute(stmt).scalar_one_or_none()
                if not file:
                    raise DidNotFindError(f"No 'File' with 'uri' '{src_uri}'")

            attach_destination(file, dest_uri, session, ret["DB_ACTION_WARNING"])
            if job not in file.jobs:
                file.jobs.append(job)
    # Get existing readset IDs from the relationship
    existing_operation_readset_ids = {r.id for r in operation.readsets}
    # Filter only new readsets not already present
    operation_readset_list_to_add = [r for r in readset_list if r.id not in existing_operation_readset_ids]
    # Extend the relationship with non-redundant readsets
    if operation_readset_list_to_add:
        operation.readsets.extend(operation_readset_list_to_add)
    # operation.readsets = readset_list
    # Get existing readset IDs from the relationship
    existing_job_readset_ids = {r.id for r in job.readsets}
    # Filter only new readsets not already present
    job_readset_list_to_add = [r for r in readset_list if r.id not in existing_job_readset_ids]
    # Extend the relationship with non-redundant readsets
    if job_readset_list_to_add:
        job.readsets.extend(job_readset_list_to_add)
    # job.readsets = readset_list

    session.add(job)
    session.flush()

    operation_id = operation.id
    job_id = job.id

    try:
        session.commit()
    except SQLAlchemyError as error:
        session.rollback()
        logger.error("Error: %s", error)

    # operation
    stmt = select(Operation).where(Operation.id == operation_id)
    operation = session.execute(stmt).scalar_one_or_none()
    # job
    stmt = select(Job).where(Job.id == job_id)
    job = session.execute(stmt).scalar_one_or_none()

    ret["DB_ACTION_OUTPUT"].append(operation)
    # If no warning
    if not ret["DB_ACTION_WARNING"]:
        ret.pop("DB_ACTION_WARNING")
    # return [operation, jobs]
    return ret

def ingest_analysis(project_id: str, ingest_data, session):
    """Ingesting an analysis operation (e.g. a GenPipes run)"""
    if isinstance(ingest_data, (str, bytes)):
        ingest_data = json.loads(ingest_data)

    if not session:
        session = database.get_session()

    ret = {
        "DB_ACTION_WARNING": [],
        "DB_ACTION_OUTPUT": []
        }

    project = projects(project_id=project_id, session=session)["DB_ACTION_OUTPUT"][0]

    check_payload_shape(ingest_data, "analysis")
    force = ingest_data.get(vb.FORCE, False)
    missing_md5sum = check_analysis_payload(ingest_data)
    if missing_md5sum:
        ret["DB_ACTION_WARNING"].append(missing_md5sum_warning(missing_md5sum))

    job = None

    config_md5sum = ingest_data[vb.OPERATION_CONFIG_MD5SUM]
    same_md5sum = config_md5sum and session.scalars(
        select(OperationConfig).where(OperationConfig.md5sum == config_md5sum)
    ).first()
    if same_md5sum and (
        same_md5sum.name, same_md5sum.version, same_md5sum.data, same_md5sum.deprecated, same_md5sum.deleted
    ) != (
        ingest_data[vb.OPERATION_CONFIG_NAME], ingest_data[vb.OPERATION_CONFIG_VERSION],
        bytes(ingest_data[vb.OPERATION_CONFIG_DATA], 'utf-8'), False, False
    ):
        # md5sum is unique: without this check the insert below fails with an IntegrityError (500)
        raise RequestError(message=(
            f"'OperationConfig' with 'md5sum' '{config_md5sum}' already exists as '{same_md5sum.name}' version "
            f"'{same_md5sum.version}' (id {same_md5sum.id}{', deprecated' if same_md5sum.deprecated else ''}"
            f"{', deleted' if same_md5sum.deleted else ''}) with another name, version or content: send the md5sum "
            f"of this config file."
        ))

    operation_config, warning = OperationConfig.from_attributes(
        name=ingest_data[vb.OPERATION_CONFIG_NAME],
        version=ingest_data[vb.OPERATION_CONFIG_VERSION],
        md5sum=ingest_data[vb.OPERATION_CONFIG_MD5SUM],
        data=bytes(ingest_data[vb.OPERATION_CONFIG_DATA], 'utf-8'),
        session=session
    )
    if warning:
        ret["DB_ACTION_WARNING"].append(warning)

    operation, warning = Operation.from_attributes(
        platform=ingest_data[vb.OPERATION_PLATFORM],
        name=ingest_data[vb.OPERATION_NAME],
        cmd_line=ingest_data[vb.OPERATION_CMD_LINE],
        status=analysis_operation_status(ingest_data),
        project=project,
        operation_config=operation_config,
        session=session
    )
    if warning:
        ret["DB_ACTION_WARNING"].append(warning)
    ret["DB_ACTION_WARNING"].extend(operation.set_analysis(
        ingest_data.get(vb.OPERATION_PIPELINE),
        ingest_data.get(vb.OPERATION_PROTOCOL),
        force=force
    ))

    readset_list = []
    # Metrics of an operation in the metric vocabulary are stored once the whole payload is read (store_metrics)
    metrics_vocabulary = metric_vocabulary().get(operation.name)
    pending_metrics = {}
    unleveled_metrics = 0
    if not ingest_data[vb.SAMPLE]:
        raise RequestError("No 'Sample' found, this json won't be ingested.")
    for sample_json in ingest_data[vb.SAMPLE]:
        stmt = (
            select(Sample)
            .where(Sample.deprecated.is_(False), Sample.deleted.is_(False))
            .where(Sample.name == sample_json[vb.SAMPLE_NAME])
        )
        sample = session.execute(stmt).scalar_one_or_none()
        if not sample:
            raise DidNotFindError(f"No Sample named '{sample_json[vb.SAMPLE_NAME]}'")
        for readset_json in sample_json[vb.READSET]:
            stmt = (
                select(Readset)
                .where(Readset.deprecated.is_(False), Readset.deleted.is_(False))
                .where(Readset.name == readset_json[vb.READSET_NAME])
            )
            readset = session.execute(stmt).scalar_one_or_none()
            readset_list.append(readset)
            if not readset:
                raise DidNotFindError(f"No Readset named '{readset_json[vb.READSET_NAME]}'")
            if readset.sample != sample:
                raise DidNotFindError(f"'Sample' with 'name' '{sample_json[vb.SAMPLE_NAME]}' not linked with 'Readset' with 'name' '{readset_json[vb.READSET_NAME]}'")
            for job_json in readset_json[vb.JOB]:
                try:
                    job_start = datetime.strptime(job_json.get(vb.JOB_START) or None, vb.DATE_LONG_FMT)
                except TypeError:
                    job_start = None
                try:
                    job_stop = datetime.strptime(job_json.get(vb.JOB_STOP) or None, vb.DATE_LONG_FMT)
                except TypeError:
                    job_stop = None
                # Check if job_status exists otherwise skip it
                if job_json.get(vb.JOB_STATUS):
                    job, warning = Job.from_attributes(
                        name=job_json[vb.JOB_NAME],
                        status=StatusEnum(job_json[vb.JOB_STATUS]),
                        start=job_start,
                        stop=job_stop,
                        operation=operation,
                        session=session
                    )
                    if warning:
                        ret["DB_ACTION_WARNING"].append(warning)
                    if readset not in job.readsets:
                        job.readsets.append(readset)
                    if vb.FILE in job_json:
                        for file_json in job_json[vb.FILE]:
                            suffixes = Path(file_json[vb.FILE_NAME]).suffixes
                            file_type = os.path.splitext(file_json[vb.FILE_NAME])[-1][1:]
                            if ".gz" in suffixes:
                                index = suffixes.index(".gz")
                                file_type = "".join(suffixes[index - 1:]).replace(".", "", 1)
                            file_deliverable = file_json.get(vb.FILE_DELIVERABLE, False)
                            # Need to have the following otherwise assigning extra_metadata to None converts null into json in the db
                            if vb.FILE_EXTRA_METADATA in file_json:
                                # Before adding a new file make sure an existing one doesn't exist otherwise update it
                                file, warning = File.get_or_create(
                                    name=file_json[vb.FILE_NAME],
                                    md5sum=file_json.get(vb.FILE_MD5SUM, None),
                                    type=file_type,
                                    extra_metadata=file_json[vb.FILE_EXTRA_METADATA],
                                    deliverable=file_deliverable,
                                    readsets=[readset],
                                    jobs=[job],
                                    session=session
                                )
                                if warning:
                                    ret["DB_ACTION_WARNING"].append(warning)
                            else:
                                # Before adding a new file make sure an existing one doesn't exist otherwise update it
                                file, warning = File.get_or_create(
                                    name=file_json[vb.FILE_NAME],
                                    md5sum=file_json.get(vb.FILE_MD5SUM, None),
                                    type=file_type,
                                    deliverable=file_deliverable,
                                    readsets=[readset],
                                    jobs=[job],
                                    session=session
                                )
                                if warning:
                                    ret["DB_ACTION_WARNING"].append(warning)
                            location, warning = Location.from_uri(uri=file_json[vb.LOCATION_URI], file=file, session=session)
                            if warning:
                                ret["DB_ACTION_WARNING"].append(warning)
                            # Before adding a new location for the current File make sure an existing one doesn't exist otherwise update it
                            if location not in file.locations:
                                if location.deleted:
                                    ret["DB_ACTION_WARNING"].append(f"Existing deleted location found for uri '{location.uri}', undeleting it.")
                                    location.deleted = False
                                if location.deprecated:
                                    ret["DB_ACTION_WARNING"].append(f"Existing deprecated location found for uri '{location.uri}', undeprecating it.")
                                    location.deprecated = False
                                file.locations.append(location)
                    if vb.METRIC in job_json and metrics_vocabulary is not None:
                        for metric_json in job_json[vb.METRIC]:
                            pending_metrics.setdefault((job, metric_json[vb.METRIC_NAME]), []).append((readset, metric_json))
                    elif vb.METRIC in job_json:
                        for metric_json in job_json[vb.METRIC]:
                            unleveled_metrics += 1
                            metric_deliverable = metric_json.get(vb.METRIC_DELIVERABLE, False)
                            metric_flag = FlagEnum(metric_json[vb.METRIC_FLAG]) if metric_json.get(vb.METRIC_FLAG) else None
                            # Before adding a new metric for the current Readset make sure an existing one doesn't exist otherwise update it
                            metric, warning = Metric.get_or_create(
                                name=metric_json[vb.METRIC_NAME],
                                value=metric_json[vb.METRIC_VALUE],
                                flag=metric_flag,
                                deliverable=metric_deliverable,
                                job=job,
                                readsets=[readset],
                                session=session
                            )
                            if warning:
                                ret["DB_ACTION_WARNING"].append(warning)
                            if metric not in job.metrics:
                                job.metrics.append(metric)
                # If job status is null then skip it as we don't want to ingest data not generated
                else:
                    ret["DB_ACTION_WARNING"].append(f"'Readset' with 'name' '{readset.name}' has 'Job' with 'name' '{job_json[vb.JOB_NAME]}' with no status, skipping.")
                # job is None until a job with a status is met
                if job is not None:
                    session.add(job)
                    session.flush()

    if pending_metrics:
        ret["DB_ACTION_WARNING"].extend(store_metrics(pending_metrics, metrics_vocabulary, operation.name, session, force=force))
    if unleveled_metrics:
        ret["DB_ACTION_WARNING"].append(
            f"'Operation' '{operation.name}' has no metric vocabulary (GET /vocabulary): its {unleveled_metrics} "
            f"metrics have no level, so a sample's metric isn't told apart from a readset's."
        )

    # Get existing readset IDs from the relationship
    existing_readset_ids = {r.id for r in operation.readsets}
    # Filter only new readsets not already present
    readset_list_to_add = [r for r in readset_list if r.id not in existing_readset_ids]
    # Extend the relationship with non-redundant readsets
    if readset_list_to_add:
        operation.readsets.extend(readset_list_to_add)
    # operation.readsets = readset_list
    operation_id = operation.id
    job_ids = [job.id for job in operation.jobs]
    if not job_ids:
        raise RequestError("No 'Job' has a status, this json won't be ingested.")

    try:
        session.commit()
    except SQLAlchemyError as error:
        session.rollback()
        logger.error("Error: %s", error)

    # operation
    stmt = select(Operation).where(Operation.id == operation_id)
    operation = session.execute(stmt).scalar_one_or_none()
    # jobs
    ret["DB_ACTION_OUTPUT"].append(operation)
    # If no warning
    if not ret["DB_ACTION_WARNING"]:
        ret.pop("DB_ACTION_WARNING")
    return ret

def ingest_delivery(project_id: str, ingest_data, session, check_readset_name=True):
    """Ingesting delivery"""
    if isinstance(ingest_data, (str, bytes)):
        ingest_data = json.loads(ingest_data)

    if not session:
        session = database.get_session()

    ret = {
        "DB_ACTION_WARNING": [],
        "DB_ACTION_OUTPUT": []
    }

    project = projects(project_id=project_id, session=session)["DB_ACTION_OUTPUT"][0]

    check_payload_shape(ingest_data, "delivery")
    delete = ingest_data.get(vb.DELETE, True)

    operation, warning = Operation.from_attributes(
        platform=ingest_data[vb.OPERATION_PLATFORM],
        name="delivery",
        cmd_line=ingest_data[vb.OPERATION_CMD_LINE],
        status=StatusEnum("COMPLETED"),
        project=project,
        session=session
    )
    if warning:
        ret["DB_ACTION_WARNING"].append(warning)

    try:
        job_start_value = ingest_data.get(vb.JOB_START) or None
        if job_start_value is not None:
            job_start = datetime.strptime(job_start_value, vb.DATE_LONG_FMT)
        else:
            job_start = datetime.now()
    except TypeError:
        job_start = datetime.now()
    try:
        job_stop_value = ingest_data.get(vb.JOB_STOP) or None
        if job_stop_value is not None:
            job_stop = datetime.strptime(job_stop_value, vb.DATE_LONG_FMT)
        else:
            job_stop = datetime.now()
    except TypeError:
        job_stop = datetime.now()
    job, warning = Job.from_attributes(
        name="delivery",
        status=StatusEnum("COMPLETED"),
        start=job_start,
        stop=job_stop,
        operation=operation,
        session=session
    )
    if warning:
        ret["DB_ACTION_WARNING"].append(warning)
    # session.add(job)

    readset_list = []
    for readset_json in ingest_data[vb.READSET]:
        readset_name = readset_json[vb.READSET_NAME]
        stmt = select(Readset).where(
            Readset.name == readset_name,
            Readset.deprecated.is_(False),
            Readset.deleted.is_(False)
        )
        readset = session.execute(stmt).scalar_one_or_none()
        if not readset:
            raise DidNotFindError(f"No 'Readset' with 'name' '{readset_name}'")
        readset_list.append(readset)
        for file_json in readset_json[vb.FILE]:
            src_uri = file_json[vb.SRC_LOCATION_URI]
            dest_uri = file_json[vb.DEST_LOCATION_URI]
            if check_readset_name:
                stmt = (
                    select(File)
                    .where(File.deprecated.is_(False), File.deleted.is_(False))
                    .join(File.readsets)
                    .where(Readset.name == readset_name)
                    .join(File.locations)
                    .where(Location.uri == src_uri)
                )
                file = session.execute(stmt).scalar_one_or_none()
                if not file:
                    raise DidNotFindError(f"No 'File' with 'uri' '{src_uri}' and 'Readset' with 'name' '{readset_name}'")
                file.state = StateEnum("DELIVERED")
            else:
                stmt = (
                    select(File)
                    .where(File.deprecated.is_(False), File.deleted.is_(False))
                    .join(File.locations)
                    .where(Location.uri == src_uri)
                )
                file = session.execute(stmt).scalar_one_or_none()
                if not file:
                    raise DidNotFindError(f"No 'File' with 'uri' '{src_uri}'")
                file.state = StateEnum("DELIVERED")

            # The destination first: the source is then deleted only if it is another location
            new_location = attach_destination(file, dest_uri, session, ret["DB_ACTION_WARNING"])
            if delete:
                stmt = (
                    select(Location)
                    .where(Location.uri == src_uri)
                )
                location = session.execute(stmt).scalar_one_or_none()
                if location is not None and location is not new_location:
                    location.deleted = True
            if job not in file.jobs:
                file.jobs.append(job)

    # Get existing readset IDs from the relationship
    existing_operation_readset_ids = {r.id for r in operation.readsets}
    # Filter only new readsets not already present
    operation_readset_list_to_add = [r for r in readset_list if r.id not in existing_operation_readset_ids]
    # Extend the relationship with non-redundant readsets
    if operation_readset_list_to_add:
        operation.readsets.extend(operation_readset_list_to_add)
    # operation.readsets = readset_list
    # Get existing readset IDs from the relationship
    existing_job_readset_ids = {r.id for r in job.readsets}
    # Filter only new readsets not already present
    job_readset_list_to_add = [r for r in readset_list if r.id not in existing_job_readset_ids]
    # Extend the relationship with non-redundant readsets
    if job_readset_list_to_add:
        job.readsets.extend(job_readset_list_to_add)
    # job.readsets = readset_list

    session.add(job)
    session.flush()

    operation_id = operation.id
    job_id = job.id

    try:
        session.commit()
    except SQLAlchemyError as error:
        session.rollback()
        logger.error("Error: %s", error)

    # operation
    stmt = select(Operation).where(Operation.id == operation_id)
    operation = session.execute(stmt).scalar_one_or_none()
    # job
    stmt = select(Job).where(Job.id == job_id)
    job = session.execute(stmt).scalar_one_or_none()

    ret["DB_ACTION_OUTPUT"].append(operation)
    # If no warning
    if not ret["DB_ACTION_WARNING"]:
        ret.pop("DB_ACTION_WARNING")

    return ret
