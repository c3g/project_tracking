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
from .errors import DidNotFindError, RequestError, UniqueConstraintError
from .route import projects
from .sample_group import check_sample_groups, add_samples_to_groups
from .utils import check_enum
from ..analysis import RESERVED_OPERATION_NAMES, analysis_operations
from .. import vocabulary as vb
from .. import database
from ..model import (
    LaneEnum,
    SequencingTypeEnum,
    StatusEnum,
    FlagEnum,
    StateEnum,
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


def check_run_processing_payload(ingest_data):
    """
    Check every enum field of a run_processing payload before anything is written; raises RequestError
    listing all invalid values. Values must be exact (e.g. 'ON_HOLD', lane '1'); readset_state and
    metric_flag may be null or missing.
    """
    invalid = []
    for specimen_json in ingest_data[vb.SPECIMEN]:
        for sample_json in specimen_json[vb.SAMPLE]:
            for readset_json in sample_json[vb.READSET]:
                name = readset_json[vb.READSET_NAME]
                check_enum(invalid, LaneEnum, readset_json.get(vb.READSET_LANE), vb.READSET_LANE, "Readset", name)
                check_enum(invalid, SequencingTypeEnum, readset_json.get(vb.READSET_SEQUENCING_TYPE), vb.READSET_SEQUENCING_TYPE, "Readset", name)
                check_enum(invalid, StateEnum, readset_json.get(vb.READSET_STATE), vb.READSET_STATE, "Readset", name, optional=True)
                for metric_json in readset_json[vb.METRIC]:
                    check_enum(invalid, FlagEnum, metric_json.get(vb.METRIC_FLAG), vb.METRIC_FLAG, "Metric", f"{metric_json[vb.METRIC_NAME]}' of 'Readset' '{name}", optional=True)
    if invalid:
        raise RequestError(message=invalid)


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
    analysis operations vocabulary, and every enum field (operation_status, job_status and metric_flag may be null
    or missing). Raises RequestError listing all problems.
    """
    invalid = []
    check_analysis_operation(invalid, ingest_data)
    check_enum(invalid, StatusEnum, ingest_data.get(vb.OPERATION_STATUS), vb.OPERATION_STATUS, "Operation",
               ingest_data.get(vb.OPERATION_NAME), optional=True)
    for sample_json in ingest_data[vb.SAMPLE]:
        for readset_json in sample_json[vb.READSET]:
            for job_json in readset_json[vb.JOB]:
                name = f"{job_json[vb.JOB_NAME]}' of 'Readset' '{readset_json[vb.READSET_NAME]}"
                check_enum(invalid, StatusEnum, job_json.get(vb.JOB_STATUS) or None, vb.JOB_STATUS, "Job", name, optional=True)
                for metric_json in job_json.get(vb.METRIC, []):
                    check_enum(invalid, FlagEnum, metric_json.get(vb.METRIC_FLAG), vb.METRIC_FLAG, "Metric", f"{metric_json[vb.METRIC_NAME]}' of 'Job' '{name}", optional=True)
    if invalid:
        raise RequestError(message=invalid)


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
    if not isinstance(ingest_data, dict):
        ingest_data = json.loads(ingest_data)

    ret = {
        "DB_ACTION_WARNING": [],
        "DB_ACTION_OUTPUT": []
        }

    project = projects(project_id=project_id, session=session)["DB_ACTION_OUTPUT"][0]

    force = ingest_data.get(vb.FORCE, False)

    # Looking for potential readsets duplicates before starting the ingestion to be able to report all of them at once
    message = unique_constraint_error(session, "run_processing", ingest_data)
    if message:
        raise UniqueConstraintError(message=message)
    normalize_tumour_metadata(ingest_data)
    check_sample_groups(ingest_data, session)
    check_run_processing_payload(ingest_data)

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
        job_start_value = ingest_data.get(vb.JOB_START)
        if job_start_value is not None:
            job_start = datetime.strptime(job_start_value, vb.DATE_LONG_FMT)
        else:
            job_start = datetime.now()
    except TypeError:
        job_start = datetime.now()
    try:
        job_stop_value = ingest_data.get(vb.JOB_STOP)
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
                    sample=sample,
                    experiment=experiment,
                    run=run,
                    operations=[operation],
                    jobs=[job]
                    )
                for file_json in readset_json[vb.FILE]:
                    if not file_json.get(vb.FILE_MD5SUM):
                        if not force:
                            ret["DB_ACTION_WARNING"].append(f"File '{file_json[vb.FILE_NAME]}' has no md5sum, skipping. Use force to ingest anyway.")
                            continue
                        ret["DB_ACTION_WARNING"].append(f"File '{file_json[vb.FILE_NAME]}' has no md5sum, ingesting anyway (force=True).")
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
                    if vb.METRIC_DELIVERABLE in metric_json:
                        metric_deliverable = metric_json[vb.METRIC_DELIVERABLE]
                    else:
                        metric_deliverable = False
                    metric_flag = FlagEnum(metric_json[vb.METRIC_FLAG]) if metric_json.get(vb.METRIC_FLAG) else None
                    _, warning = Metric.get_or_create(
                        name=metric_json[vb.METRIC_NAME],
                        value=metric_json[vb.METRIC_VALUE],
                        flag=metric_flag,
                        deliverable=metric_deliverable,
                        job=job,
                        readsets=[readset],
                        session=session,
                        dont_merge=True
                        )
                    if warning:
                        ret["DB_ACTION_WARNING"].append(warning)

                session.add(readset)
            try:
                session.flush()
            except IntegrityError as error:
                session.rollback()
                message = unique_constraint_error(session, "run_processing", ingest_data)
                if not message:
                    raise UniqueConstraintError(message=str(error.orig)) from error
                raise UniqueConstraintError(message=message) from error

    ret["DB_ACTION_WARNING"].extend(add_samples_to_groups(samples_groups, session))

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


def ingest_transfer(project_id: str, ingest_data, session, check_readset_name=True):
    """Ingesting transfer"""
    if not isinstance(ingest_data, dict):
        ingest_data = json.loads(ingest_data)

    if not session:
        session = database.get_session()

    ret = {
        "DB_ACTION_WARNING": [],
        "DB_ACTION_OUTPUT": []
    }

    project = projects(project_id=project_id, session=session)["DB_ACTION_OUTPUT"][0]

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
        job_start_value = ingest_data.get(vb.JOB_START)
        if job_start_value is not None:
            job_start = datetime.strptime(job_start_value, vb.DATE_LONG_FMT)
        else:
            job_start = datetime.now()
    except TypeError:
        job_start = datetime.now()
    try:
        job_stop_value = ingest_data.get(vb.JOB_STOP)
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

            new_location, warning = Location.from_uri(uri=dest_uri, file=file, session=session)
            if warning:
                ret["DB_ACTION_WARNING"].append(warning)
            if new_location.deleted:
                ret["DB_ACTION_WARNING"].append(f"Existing deleted location found for uri '{new_location.uri}', undeleting it.")
                new_location.deleted = False
            if new_location.deprecated:
                ret["DB_ACTION_WARNING"].append(f"Existing deprecated location found for uri '{new_location.uri}', undeprecating it.")
                new_location.deprecated = False
            if job not in file.jobs:
                file.jobs.append(job)
            session.add(new_location)
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
    if not isinstance(ingest_data, dict):
        ingest_data = json.loads(ingest_data)

    if not session:
        session = database.get_session()

    ret = {
        "DB_ACTION_WARNING": [],
        "DB_ACTION_OUTPUT": []
        }

    project = projects(project_id=project_id, session=session)["DB_ACTION_OUTPUT"][0]

    force = ingest_data.get(vb.FORCE, False)
    check_analysis_payload(ingest_data)

    job = None

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
                    job_start = datetime.strptime(job_json[vb.JOB_START], vb.DATE_LONG_FMT)
                except TypeError:
                    job_start = None
                try:
                    job_stop = datetime.strptime(job_json[vb.JOB_STOP], vb.DATE_LONG_FMT)
                except TypeError:
                    job_stop = None
                # Check if job_status exists otherwise skip it
                if job_json[vb.JOB_STATUS]:
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
                            if not file_json.get(vb.FILE_MD5SUM):
                                if not force:
                                    ret["DB_ACTION_WARNING"].append(f"File '{file_json[vb.FILE_NAME]}' has no md5sum, skipping. Use force to ingest anyway.")
                                    continue
                                ret["DB_ACTION_WARNING"].append(f"File '{file_json[vb.FILE_NAME]}' has no md5sum, ingesting anyway (force=True).")
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
                    if vb.METRIC in job_json:
                        for metric_json in job_json[vb.METRIC]:
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
    if not isinstance(ingest_data, dict):
        ingest_data = json.loads(ingest_data)

    if not session:
        session = database.get_session()

    ret = {
        "DB_ACTION_WARNING": [],
        "DB_ACTION_OUTPUT": []
    }

    project = projects(project_id=project_id, session=session)["DB_ACTION_OUTPUT"][0]

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
        job_start_value = ingest_data.get(vb.JOB_START)
        if job_start_value is not None:
            job_start = datetime.strptime(job_start_value, vb.DATE_LONG_FMT)
        else:
            job_start = datetime.now()
    except TypeError:
        job_start = datetime.now()
    try:
        job_stop_value = ingest_data.get(vb.JOB_STOP)
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

            if delete:
                stmt = (
                    select(Location)
                    .where(Location.uri == src_uri)
                )
                location = session.execute(stmt).scalar_one_or_none()
                if location is not None:
                    location.deleted = True

            new_location, warning = Location.from_uri(uri=dest_uri, file=file, session=session)
            if warning:
                ret["DB_ACTION_WARNING"].append(warning)
            if new_location.deleted:
                ret["DB_ACTION_WARNING"].append(f"Existing deleted location found for uri '{new_location.uri}', undeleting it.")
                new_location.deleted = False
            if new_location.deprecated:
                ret["DB_ACTION_WARNING"].append(f"Existing deprecated location found for uri '{new_location.uri}', undeprecating it.")
                new_location.deprecated = False
            if job not in file.jobs:
                file.jobs.append(job)
            session.add(new_location)

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
