"""
Ingesting data into the database
"""
# Standard library
import logging
import json
import os
from datetime import datetime
from pathlib import Path

# Third-party
from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError, IntegrityError

# Local modules
from .errors import DidNotFindError, RequestError, UniqueConstraintError
from .route import projects
from .. import vocabulary as vb
from .. import database
from ..model import (
    LaneEnum,
    SequencingTypeEnum,
    StatusEnum,
    FlagEnum,
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

def ingest_run_processing(project_id: str, ingest_data: dict, session):
    """Ingesting run for MoH"""
    if not isinstance(ingest_data, dict):
        ingest_data = json.loads(ingest_data)

    ret = {
        "DB_ACTION_WARNING": [],
        "DB_ACTION_OUTPUT": []
        }

    project = projects(project_id=project_id, session=session)["DB_ACTION_OUTPUT"][0]

    operation = Operation(
        platform=ingest_data[vb.OPERATION_PLATFORM],
        name="run_processing",
        status=StatusEnum("COMPLETED"),
        project=project
        )
    job = Job(
        name="run_processing",
        status=StatusEnum("COMPLETED"),
        start=datetime.now(),
        stop=datetime.now(),
        operation=operation
        )
    session.add(job)
    # Defining Run
    run = Run.from_attributes(
        ext_id=ingest_data[vb.RUN_EXT_ID],
        ext_src=ingest_data[vb.RUN_EXT_SRC],
        name=ingest_data[vb.RUN_NAME],
        instrument=ingest_data[vb.RUN_INSTRUMENT],
        date=datetime.strptime(ingest_data[vb.RUN_DATE], vb.DATE_LONG_FMT),
        session=session
        )

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
            for readset_json in sample_json[vb.READSET]:
                if readset_json[vb.EXPERIMENT_KIT_EXPIRATION_DATE]:
                    kit_expiration_date = datetime.strptime(readset_json[vb.EXPERIMENT_KIT_EXPIRATION_DATE], vb.DATE_FMT)
                else:
                    kit_expiration_date = None
                # Defining Experiment
                experiment = Experiment.from_attributes(
                    sequencing_technology=readset_json[vb.EXPERIMENT_SEQUENCING_TECHNOLOGY],
                    type=readset_json[vb.EXPERIMENT_TYPE],
                    nucleic_acid_type=readset_json[vb.EXPERIMENT_NUCLEIC_ACID_TYPE],
                    library_kit=readset_json[vb.EXPERIMENT_LIBRARY_KIT],
                    kit_expiration_date=kit_expiration_date,
                    session=session
                    )
                readset = Readset(
                    name=readset_json[vb.READSET_NAME],
                    lane=LaneEnum(readset_json[vb.READSET_LANE]),
                    adapter1=readset_json[vb.READSET_ADAPTER1],
                    adapter2=readset_json[vb.READSET_ADAPTER2],
                    sequencing_type=SequencingTypeEnum(readset_json[vb.READSET_SEQUENCING_TYPE]),
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
                        # No need to use .from_attributes because run_processing is ingested ionly one time for a given Readset
                        file = File(
                            name=file_json[vb.FILE_NAME],
                            type=file_type,
                            extra_metadata=file_json[vb.FILE_EXTRA_METADATA],
                            deliverable=file_deliverable,
                            readsets=[readset],
                            jobs=[job]
                            )
                    else:
                        # No need to use .from_attributes because run_processing is ingested ionly one time for a given Readset
                        file = File(
                            name=file_json[vb.FILE_NAME],
                            type=file_type,
                            deliverable=file_deliverable,
                            readsets=[readset],
                            jobs=[job]
                            )
                    _ = Location.from_uri(uri=file_json[vb.LOCATION_URI], file=file, session=session)
                for metric_json in readset_json[vb.METRIC]:
                    if vb.METRIC_DELIVERABLE in metric_json:
                        metric_deliverable = metric_json[vb.METRIC_DELIVERABLE]
                    else:
                        metric_deliverable = False
                    if vb.METRIC_FLAG in metric_json:
                        metric_flag = FlagEnum(metric_json[vb.METRIC_FLAG])
                    else:
                        metric_flag = None
                    _, warning = Metric.get_or_create(
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

                session.add(readset)
            try:
                session.flush()
            except IntegrityError as error:
                session.rollback()
                message = unique_constraint_error(session, "run_processing", ingest_data)
                if not message:
                    raise UniqueConstraintError(message=str(error.orig)) from error
                raise UniqueConstraintError(message=message) from error

    operation_id = operation.id
    job_id = job.id

    try:
        session.commit()
    except SQLAlchemyError as error:
        logger.error("Error: %s", error)
        session.rollback()

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

    operation = Operation(
        platform=ingest_data[vb.OPERATION_PLATFORM],
        name="transfer",
        cmd_line=ingest_data[vb.OPERATION_CMD_LINE],
        status=StatusEnum("COMPLETED"),
        project=project
    )
    job = Job(
        name="transfer",
        status=StatusEnum("COMPLETED"),
        start=datetime.now(),
        stop=datetime.now(),
        operation=operation
    )
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

            new_location = Location.from_uri(uri=dest_uri, file=file, session=session)
            file.jobs.append(job)
            session.add(new_location)
    operation.readsets = readset_list
    job.readsets = readset_list

    session.add(job)
    session.flush()

    operation_id = operation.id
    job_id = job.id

    try:
        session.commit()
    except SQLAlchemyError as error:
        logger.error("Error: %s", error)
        session.rollback()

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

def ingest_genpipes(project_id: str, ingest_data, session):
    """Ingesting GenPipes run"""
    if not isinstance(ingest_data, dict):
        ingest_data = json.loads(ingest_data)

    if not session:
        session = database.get_session()

    ret = {
        "DB_ACTION_WARNING": [],
        "DB_ACTION_OUTPUT": []
        }

    project = projects(project_id=project_id, session=session)["DB_ACTION_OUTPUT"][0]

    operation_config = OperationConfig.from_attributes(
        name=ingest_data[vb.OPERATION_CONFIG_NAME],
        version=ingest_data[vb.OPERATION_CONFIG_VERSION],
        md5sum=ingest_data[vb.OPERATION_CONFIG_MD5SUM],
        data=bytes(ingest_data[vb.OPERATION_CONFIG_DATA], 'utf-8'),
        session=session
    )

    operation, warning = Operation.from_attributes(
        platform=ingest_data[vb.OPERATION_PLATFORM],
        name="genpipes",
        cmd_line=ingest_data[vb.OPERATION_CMD_LINE],
        status=StatusEnum("COMPLETED"),
        project=project,
        operation_config=operation_config,
        session=session
    )
    if warning:
        ret["DB_ACTION_WARNING"].append(warning)

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
                    job = Job(
                        name=job_json[vb.JOB_NAME],
                        status=StatusEnum(job_json[vb.JOB_STATUS]),
                        start=job_start,
                        stop=job_stop,
                        operation=operation,
                        readsets=[readset]
                    )
                    # Required to lookup with get_or_create
                    session.add(job)
                    session.flush()
                    if vb.FILE in job_json:
                        for file_json in job_json[vb.FILE]:
                            suffixes = Path(file_json[vb.FILE_NAME]).suffixes
                            file_type = os.path.splitext(file_json[vb.FILE_NAME])[-1][1:]
                            if ".gz" in suffixes:
                                index = suffixes.index(".gz")
                                file_type = "".join(suffixes[index - 1:]).replace(".", "", 1)
                            file_deliverable = file_json.get(vb.FILE_DELIVERABLE, False)
                            # Need to have an the following otherwise assigning extra_metadata to None converts null into json in the db
                            if vb.FILE_EXTRA_METADATA in file_json:
                                # Before adding a new file make sure an existing one doesn't exist otherwise update it
                                file, warning = File.get_or_create(
                                    name=file_json[vb.FILE_NAME],
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
                                    type=file_type,
                                    deliverable=file_deliverable,
                                    readsets=[readset],
                                    jobs=[job],
                                    session=session
                                )
                                if warning:
                                    ret["DB_ACTION_WARNING"].append(warning)
                            _ = Location.from_uri(uri=file_json[vb.LOCATION_URI], file=file, session=session)
                    if vb.METRIC in job_json:
                        for metric_json in job_json[vb.METRIC]:
                            metric_deliverable = metric_json.get(vb.METRIC_DELIVERABLE, False)
                            metric_flag = FlagEnum(metric_json[vb.METRIC_FLAG]) if vb.METRIC_FLAG in metric_json else None
                            # Before adding a new metric for the current Readset make sure an existing one doesn't exist otherwise update it
                            _, warning = Metric.get_or_create(
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
                # If job status is null then skip it as we don't want to ingest data not generated
                else:
                    ret["DB_ACTION_WARNING"].append(f"'Readset' with 'name' '{readset.name}' has 'Job' with 'name' '{job_json[vb.JOB_NAME]}' with no status, skipping.")

                try:
                    session.add(job)
                    session.flush()
                except UnboundLocalError:
                    pass
    operation.readsets = readset_list
    operation_id = operation.id
    job_ids = [job.id for job in operation.jobs]
    if not job_ids:
        raise RequestError("No 'Job' has a status, this json won't be ingested.")
    try:
        session.commit()
    except SQLAlchemyError as error:
        logger.error("Error: %s", error)
        session.rollback()

    # operation
    stmt = select(Operation).where(Operation.id == operation_id)
    operation = session.execute(stmt).scalar_one_or_none()
    # jobs
    ret["DB_ACTION_OUTPUT"].append(operation)
    # If no warning
    if not ret["DB_ACTION_WARNING"]:
        ret.pop("DB_ACTION_WARNING")
    return ret
