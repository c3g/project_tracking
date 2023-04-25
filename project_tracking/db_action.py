import inspect
import re
import json
import os
import logging
import csv
import sqlite3

from datetime import datetime
from sqlalchemy import select, exc
from pathlib import Path

from . import vocabulary as vb
from . import database
from .model import (
    LaneEnum,
    SequencingTypeEnum,
    StatusEnum,
    FlagEnum,
    AggregateEnum,
    readset_file,
    Project,
    Patient,
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


def projects(project_name = None, session=None):
    """
    Fetching all projects in database
    """
    if session is None:
        session = database.get_session()

    if project_name is None:
        stmt = (select(Project))
    else:
        if isinstance(project_name, str):
            project_name = [project_name]
        stmt = (select(Project).where(Project.name.in_(project_name)))

    return session.scalars(stmt).unique().all()

def metrics(project_name=None, readset_id=None, metric_id = None, sample_id=None):
    """
    Fetching all metrics that are part of the project or readset or sample
    """
    session = database.get_session()
    if isinstance(project_name, str):
        project_name = [project_name]

    if metric_id and project_name:
        if isinstance(metric_id, int):
            metric_id = [metric_id]
        stmt = (select(Metric)
                .where(Metric.id.in_(metric_id))
                .join(Metric.readsets)
                .join(Readset.sample)
                .join(Sample.patient).
                join(Patient.project).
                where(Project.name.in_(project_name))
                )
    elif readset_id and project_name:
        if isinstance(readset_id, int):
            readset_id = [readset_id]
        stmt = (select(Metric)
                .join(Metric.readsets)
                .where(Readset.id.in_(readset_id))
                .join(Metric.readsets)
                .join(Readset.sample)
                .join(Sample.patient).
                join(Patient.project).
                where(Project.name.in_(project_name))
                )
    elif sample_id and project_name:
        if isinstance(sample_id, int):
            sample_id = [sample_id]
        stmt = (select(Metric)
                .join(Metric.readsets)
                .join(Readset.sample)
                .where(Sample.id.in_(sample_id))
                .join(Metric.readsets)
                .join(Readset.sample)
                .join(Sample.patient).
                join(Patient.project).
                where(Project.name.in_(project_name))
                )

    return session.scalars(stmt).unique().all()


def files(project_name, readset_id, run_processing=True):
    """
    Fetching all files that are linked to readset
    """
    session = database.get_session()

    if isinstance(project_name, str):
        project_name = [project_name]
    if isinstance(readset_id, str):
        readset_id = [readset_id]

    if project_name is  None and readset_id is None:
        stmt = select(File)
    elif project_name and readset_id and run_processing:
        stmt = (select(File)
                .join(File.readsets)
                .where(Readset.id.in_(readset_id))
                # .join(File.jobs)
                # .where(Job.name==vb.RUN_PROCESSING)
                .join(File.jobs)
                .join(Job.operation)
                .join(Operation.project)
                .where(Project.name.in_(project_name))
                )

    return session.scalars(stmt).unique().all()


def readsets(project_name=None, sample_id=None, readset_id=None):
    """
    Fetching all readsets that are part of the project or sample
    """
    session = database.get_session()

    if isinstance(project_name, str):
        project_name = [project_name]

    if project_name is None and sample_id is None and readset_id is None:
        stmt = select(Readset)
    elif project_name and sample_id is None and readset_id is None:
        stmt = (select(Readset)
                .join(Readset.sample)
                .join(Sample.patient).
                join(Patient.project).
                where(Project.name.in_(project_name)))
    elif sample_id and project_name:
        if isinstance(sample_id, int):
            sample_id = [sample_id]
        stmt = (select(Readset)
                .join(Readset.sample)
                .where(Sample.id.in_(sample_id)).where(Project.name.in_(project_name))
                .join(Readset.sample)
                .join(Sample.patient).
                join(Patient.project).
                where(Project.name.in_(project_name))
                )
    elif readset_id and project_name:
        if isinstance(readset_id, int):
            readset_id = [readset_id]
        stmt = (select(Readset)
                .where(Readset.id.in_(readset_id))
                .join(Readset.sample)
                .join(Sample.patient).
                join(Patient.project).
                where(Project.name.in_(project_name))
                )

    return session.scalars(stmt).unique().all()



def patient_pair(project_name: str, pair: bool, patient_id=None, tumor: bool=True):
    """
    Pair = True: Returns only patients that have a tumorus and a normal sample
    Pair = False, Tumor = True: Returns patients that only have a tumorus samples
    Pair = False, Tumor = False: Returns  patients that only have a Normal samples
    """

    session = database.get_session()
    if isinstance(project_name, str):
        project_name = [project_name]

    if patient_id is None:
        stmt1 = (select(Patient).join(Patient.samples).where(Sample.tumour == True)
                 .where(Project.name.in_(project_name)))
        stmt2 = (select(Patient).join(Patient.samples).where(Sample.tumour == False)
                 .where(Project.name.in_(project_name)))
    else:
        if isinstance(patient_id, int):
            patient_id = [patient_id]
        stmt1 = (select(Patient).join(Patient.samples).where(Sample.tumour == True)
                 .where(Project.name.in_(project_name))
                 .where(Patient.id.in_(patient_id)))
        stmt2 = (select(Patient).join(Patient.samples).where(Sample.tumour == False)
                 .where(Project.name.in_(project_name))
                .where(Patient.id.in_(patient_id)))


    s1 = set(session.scalars(stmt1).all())
    s2 = set(session.scalars(stmt2).all())
    if pair:
        return s2.intersection(s1)
    elif tumor:
        return s1.difference(s2)
    else:
        return s2.difference(s1)


def patients(project_name = None, patient_id = None):
    """Fetchin all patients form projets or selected patient from id
    """
    session = database.get_session()
    if isinstance(project_name, str):
        project_name = [project_name]

    if project_name is None and patient_id is None:
        stmt = (select(Patient))
    elif patient_id is None and project_name:
        stmt = (select(Patient).join(Patient.project).where(Project.name.in_(project_name)))
    else:
        if isinstance(patient_id, int):
            patient_id = [patient_id]
        stmt = (select(Patient).where(Patient.id.in_(patient_id))
                .where(Project.name.in_(project_name)))

    return session.scalars(stmt).unique().all()




def samples(project_name= None, sample_id = None):
    """Fetchin all projects in database
    still need to check if sample are part of project when
     both are provided
    """
    session = database.get_session()
    if isinstance(project_name, str):
        project_name = [project_name]
    if project_name is None:
        stmt = (select(Sample))
    elif sample_id is None:
        stmt = (select(Sample).join(Sample.patient).join(Patient.project)
                .where(Project.name.in_(project_name)))
    else:
        if isinstance(sample_id, int):
            sample_id = [sample_id]
        stmt = (select(Sample)
                .where(Sample.id.in_(sample_id))
                .join(Sample.patient)
                .join(Patient.project)
                .where(Project.name.in_(project_name))
                )

    return session.scalars(stmt).unique().all()


def create_project(project_name, fms_id=None, session=None):
    """
    Creating new project
    Returns project even if it already exist
    """
    if not session:
        session = database.get_session()

    project = Project(name=project_name, fms_id=fms_id)

    session.add(project)

    try:
        session.commit()
    except exc.SQLAlchemyError as error:
        # crash if project does not exist
        logger.warning(f"Could no commit {project_name}: {error}")
        session.rollback()

    return session.scalars(select(Project).where(Project.name == project_name)).one()

def ingest_run_processing(project_name, ingest_data, session=None):
    """Ingesting run for MoH"""
    if not isinstance(ingest_data, dict):
        ingest_data = json.loads(ingest_data)

    if not session:
        session = database.get_session()

    project = projects(project_name=project_name, session=session)[0]

    operation = Operation(
        platform=ingest_data[vb.OPERATION_PLATFORM],
        name="run_processing",
        status=StatusEnum("DONE"),
        project=project
        )
    job = Job(
        name="run_processing",
        status=StatusEnum("DONE"),
        start=datetime.now(),
        stop=datetime.now(),
        operation=operation
        )
    session.add(job)
    # Defining Run
    run = Run.from_attributes(
        fms_id=ingest_data[vb.RUN_FMS_ID],
        name=ingest_data[vb.RUN_NAME],
        instrument=ingest_data[vb.RUN_INSTRUMENT],
        date=datetime.strptime(ingest_data[vb.RUN_DATE], vb.DATE_LONG_FMT),
        session=session
        )

    for patient_json in ingest_data[vb.PATIENT]:
        patient = Patient.from_name(
            name=patient_json[vb.PATIENT_NAME],
            cohort=patient_json[vb.PATIENT_COHORT],
            institution=patient_json[vb.PATIENT_INSTITUTION],
            project=project,
            session=session
            )
        for sample_json in patient_json[vb.SAMPLE]:
            sample = Sample.from_name(
                name=sample_json[vb.SAMPLE_NAME],
                tumour=sample_json[vb.SAMPLE_TUMOUR],
                patient=patient,
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
                    library_kit=readset_json[vb.EXPERIMENT_LIBRARY_KIT],
                    kit_expiration_date=kit_expiration_date,
                    session=session
                    )
                # Let this fails if readset already exists.
                readset = Readset(
                    name=readset_json[vb.READSET_NAME],
                    lane=LaneEnum(readset_json[vb.READSET_LANE]),
                    adapter1=readset_json[vb.READSET_ADAPTER1],
                    adapter2=readset_json[vb.READSET_ADAPTER2],
                    sequencing_type=SequencingTypeEnum(readset_json[vb.READSET_SEQUENCING_TYPE]),
                    quality_offset=readset_json[vb.READSET_QUALITY_OFFSET],
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
                        file_type = "".join(suffixes[-2:])
                    # Need to have an the following otherwise assigning extra_metadata to None converts null into json in the db
                    if vb.FILE_EXTRA_METADATA in file_json.keys():
                        file = File(
                            name=file_json[vb.FILE_NAME],
                            type=file_type,
                            extra_metadata=file_json[vb.FILE_EXTRA_METADATA],
                            readsets=[readset],
                            jobs=[job]
                            )
                    else:
                        file = File(
                            name=file_json[vb.FILE_NAME],
                            type=file_type,
                            readsets=[readset],
                            jobs=[job]
                            )
                    location = Location.from_uri(uri=file_json[vb.LOCATION_URI], file=file, session=session)
                for metric_json in readset_json[vb.METRIC]:
                    Metric(
                        name=metric_json[vb.METRIC_NAME],
                        value=metric_json[vb.METRIC_VALUE],
                        flag=FlagEnum(metric_json[vb.METRIC_FLAG]),
                        job=job,
                        readsets=[readset]
                        )

            session.add(readset)
            session.flush()

    operation_id = operation.id
    job_id = job.id

    try:
        session.commit()
    except exc.SQLAlchemyError as error:
        logger.error("Error: %s", error)
        session.rollback()

    # operation
    operation = session.scalars(select(Operation).where(Operation.id == operation_id)).first()
    # job
    job = session.scalars(select(Job).where(Job.id == job_id)).first()

    return [operation, job]


def ingest_transfer(project_name, ingest_data, session=None, check_readset_name=True):
    """Ingesting transfer"""
    if not isinstance(ingest_data, dict):
        ingest_data = json.loads(ingest_data)

    if not session:
        session = database.get_session()

    project = projects(project_name=project_name, session=session)[0]

    operation = Operation(
        platform=ingest_data[vb.OPERATION_PLATFORM],
        name="transfer",
        cmd_line=ingest_data[vb.OPERATION_CMD_LINE],
        status=StatusEnum("DONE"),
        project=project
        )
    job = Job(
        name="transfer",
        status=StatusEnum("DONE"),
        start=datetime.now(),
        stop=datetime.now(),
        operation=operation
        )

    for readset_json in ingest_data[vb.READSET]:
        readset_name = readset_json[vb.READSET_NAME]
        for file_json in readset_json[vb.FILE]:
            src_uri = file_json[vb.SRC_LOCATION_URI]
            dest_uri = file_json[vb.DEST_LOCATION_URI]
            if check_readset_name:
                file = session.scalars(
                    select(File)
                    .join(File.readsets)
                    .where(Readset.name ==  readset_name )
                    .join(File.locations    )
                    .where(Location.uri == src_uri)
                    ).unique().first()
                if not file:
                    raise Exception(f"No file with uri: {src_uri} and readset {readset_name}")
            else:
                file = session.scalars(
                    select(File)
                        .join(File.readsets)
                        .where(Readset.name == readset_name)
                        .join(File.locations)
                        .where(Location.uri == src_uri)
                ).unique().first()
                if not file:
                    raise Exception(f"No file with uri: {src_uri}")

            new_location = Location.from_uri(uri=dest_uri, file=file, session=session)
            file.jobs.append(job)
            session.add(new_location)

    session.add(job)
    session.flush()

    operation_id = operation.id
    job_id = job.id

    try:
        session.commit()
    except exc.SQLAlchemyError as error:
        logger.error("Error: %s", error)
        session.rollback()

    # operation
    operation = session.scalars(select(Operation).where(Operation.id == operation_id)).first()
    # job
    job = session.scalars(select(Job).where(Job.id == job_id)).first()

    return [operation, job]


def digest_readset_file(project_name, digest_data, session=None):
    """Digesting readset file fields for GenPipes"""
    if not session:
        session = database.get_session()

    samples = []
    readsets = []
    output = []

    if vb.LOCATION_ENDPOINT in digest_data.keys():
        location_endpoint = digest_data[vb.LOCATION_ENDPOINT]
    else:
        location_endpoint = None

    if vb.SAMPLE_NAME in digest_data.keys():
        for sample_name in digest_data[vb.SAMPLE_NAME]:
            sample = session.scalars(select(Sample).where(Sample.name == sample_name)).unique().first()
            samples.append(sample)
    if vb.SAMPLE_ID in digest_data.keys():
        for sample_id in digest_data[vb.SAMPLE_ID]:
            sample = session.scalars(select(Sample).where(Sample.id == sample_id)).unique().first()
            samples.append(sample)
    if samples:
        set(samples)
        for sample in samples:
            for readset in sample.readsets:
                readsets.append(readset)
    if vb.READSET_NAME in digest_data.keys():
        for readset_name in digest_data[vb.READSET_NAME]:
            readset = session.scalars(select(Readset).where(Readset.name == readset_name)).unique().first()
            readsets.append(readset)
    if vb.READSET_ID in digest_data.keys():
        for readset_id in digest_data[vb.READSET_ID]:
            readset = session.scalars(select(Readset).where(Readset.id == readset_id)).unique().first()
            readsets.append(readset)
    if readsets:
        set(readsets)
        for readset in readsets:
            bed = ""
            for file in readset.files:
                if file.type in ["fastq", "fq", "fq.gz", "fastq.gz"]:
                    bam = ""
                    if file.extra_metadata["read_type"] == "R1":
                        if location_endpoint:
                            for location in file.locations:
                                if location_endpoint == location.endpoint:
                                    fastq1 = location.uri.split("://")[-1]
                        else:
                            fastq1 = file.locations[-1].uri.split("://")[-1]
                    elif file.extra_metadata["read_type"] == "R2":
                        if location_endpoint:
                            for location in file.locations:
                                if location_endpoint == location.endpoint:
                                    fastq1 = location.uri.split("://")[-1]
                        else:
                            fastq1 = file.locations[-1].uri.split("://")[-1]
                elif file.type == "bam":
                    bam = ""
                    if location_endpoint:
                        for location in file.locations:
                            if location_endpoint == location.endpoint:
                                bam = location.uri.split("://")[-1]
                        if not bam:
                            raise ValueError(f"looking for bam file in '{location_endpoint}', file only existe on {[l.endpoint for l in file.locations]} system")
                    else:
                        bam = file.locations[-1].uri.split("://")[-1]
                    fastq1 = ""
                    fastq2 = ""
                if file.type == "bed":
                    bed = file.name
            readset_line = {
                "Sample": readset.sample.name,
                "Readset": readset.name,
                "LibraryType": readset.experiment.library_kit,
                "RunType": readset.sequencing_type.value,
                "Run": readset.run.name,
                "Lane": readset.lane.value,
                "Adapter1": readset.adapter1,
                "Adapter2": readset.adapter2,
                "QualityOffset": readset.quality_offset,
                "BED": bed,
                "FASTQ1": fastq1,
                "FASTQ2": fastq2,
                "BAM": bam
                }
            output.append(readset_line)

    return output

def digest_pair_file(project_name, digest_data, session=None):
    """Digesting pair file fields for GenPipes"""
    if not session:
        session = database.get_session()

    pair_dict = {}
    samples = []
    # readsets = []
    output = []

    if vb.SAMPLE_NAME in digest_data.keys():
        for sample_name in digest_data[vb.SAMPLE_NAME]:
            sample = session.scalars(select(Sample).where(Sample.name == sample_name)).unique().first()
            samples.append(sample)
    if vb.SAMPLE_ID in digest_data.keys():
        for sample_id in digest_data[vb.SAMPLE_ID]:
            sample = session.scalars(select(Sample).where(Sample.id == sample_id)).unique().first()
            samples.append(sample)
    if vb.READSET_NAME in digest_data.keys():
        for readset_name in digest_data[vb.READSET_NAME]:
            readset = session.scalars(select(Readset).where(Readset.name == readset_name)).unique().first()
            samples.append(readset.sample)
            # readsets.append(readset)
    if vb.READSET_ID in digest_data.keys():
        for readset_id in digest_data[vb.READSET_ID]:
            readset = session.scalars(select(Readset).where(Readset.id == readset_id)).unique().first()
            samples.append(readset.sample)
            # readsets.append(readset)
    if samples:
        set(samples)
        for sample in samples:
            if not sample.patient.name in pair_dict.keys():
                pair_dict[sample.patient.name] = {
                    "T": None,
                    "N": None
                    }
            if sample.tumour:
                pair_dict[sample.patient.name]["T"] = sample.name
            else:
                pair_dict[sample.patient.name]["N"] = sample.name
    if pair_dict:
        for patient_name, dict_tn in pair_dict.items():
            pair_line = {
                "Patient": patient_name,
                "Sample_N": dict_tn["N"],
                "Sample_T": dict_tn["T"]
                }
            output.append(pair_line)

    return output

def ingest_genpipes(project_name, ingest_data, session=None):
    """Ingesting GenPipes run"""
    if not isinstance(ingest_data, dict):
        ingest_data = json.loads(ingest_data)

    if not session:
        session = database.get_session()

    project = projects(project_name=project_name, session=session)[0]

    operation_config = OperationConfig(
        name=ingest_data[vb.OPERATION_CONFIG_NAME],
        version=ingest_data[vb.OPERATION_CONFIG_VERSION],
        md5sum=ingest_data[vb.OPERATION_CONFIG_MD5SUM],
        data=bytes(ingest_data[vb.OPERATION_CONFIG_DATA], 'utf-8')
        )

    operation = Operation(
        platform=ingest_data[vb.OPERATION_PLATFORM],
        name="genpipes",
        cmd_line=ingest_data[vb.OPERATION_CMD_LINE],
        status=StatusEnum("DONE"),
        project=project,
        operation_config=operation_config
        )

    for sample_json in ingest_data[vb.SAMPLE]:
        sample = session.scalars(
            select(Sample)
            .where(Sample.name == sample_json[vb.SAMPLE_NAME])
            ).unique().first()
        if not sample:
            raise Exception(f"No sample named {sample_json[vb.SAMPLE_NAME]}")
        for readset_json in sample_json[vb.READSET]:
            readset = session.scalars(
                select(Readset)
                .where(Readset.name == readset_json[vb.READSET_NAME])
                ).unique().first()
            if not readset:
                raise Exception(f"No readset named {readset_json[vb.READSET_NAME]}")
            if readset.sample != sample:
                raise Exception(f"sample {sample_json[vb.SAMPLE_NAME]} not linked with readset {readset_json[vb.READSET_NAME]}")
            for job_json in readset_json[vb.JOB]:
                job = Job(
                    name=job_json[vb.JOB_NAME],
                    status=StatusEnum(job_json[vb.JOB_STATUS]),
                    start=datetime.strptime(job_json[vb.JOB_START], vb.DATE_LONG_FMT),
                    stop=datetime.strptime(job_json[vb.JOB_STOP], vb.DATE_LONG_FMT),
                    operation=operation
                    )
                for file_json in job_json[vb.FILE]:
                    suffixes = Path(file_json[vb.FILE_NAME]).suffixes
                    file_type = os.path.splitext(file_json[vb.FILE_NAME])[-1][1:]
                    if ".gz" in suffixes:
                        file_type = "".join(suffixes[-2:])
                    # Need to have an the following otherwise assigning extra_metadata to None converts null into json in the db
                    if vb.FILE_EXTRA_METADATA in file_json.keys():
                        file = File(
                            name=file_json[vb.FILE_NAME],
                            type=file_type,
                            extra_metadata=file_json[vb.FILE_EXTRA_METADATA],
                            readsets=[readset],
                            jobs=[job]
                            )
                    else:
                        file = File(
                            name=file_json[vb.FILE_NAME],
                            type=file_type,
                            readsets=[readset],
                            jobs=[job]
                            )
                    location = Location.from_uri(uri=file_json[vb.LOCATION_URI], file=file, session=session)
                if vb.METRIC in job_json.keys():
                    for metric_json in job_json[vb.METRIC]:
                        Metric(
                            name=metric_json[vb.METRIC_NAME],
                            value=metric_json[vb.METRIC_VALUE],
                            flag=FlagEnum(metric_json[vb.METRIC_FLAG]),
                            job=job,
                            readsets=[readset]
                            )

                session.add(job)
                session.flush()

    operation_id = operation.id
    job_ids = [job.id for job in operation.jobs]
    try:
        session.commit()
    except exc.SQLAlchemyError as error:
        logger.error("Error: %s", error)
        session.rollback()

    # operation
    operation = session.scalars(select(Operation).where(Operation.id == operation_id)).first()
    # jobs
    jobs = [session.scalars(select(Job).where(Job.id == job_id)).first() for job_id in job_ids]

    return [operation, jobs]
