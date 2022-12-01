import inspect
import re
import json
import os
import logging
import csv

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
    Readset,
    Operation,
    OperationConfig,
    Job,
    Metric,
    Bundle,
    File
    )

logger = logging.getLogger(__name__)


def fix_db_from_file_system(project_name, ingest_data):
    """ Use Ingest Data scaped for the FS to fix/set the database

    """
    pass

def projects(project = None):
    """Fetchin all projects in database
    """
    session = database.get_session()
    if project is None:
        return session.scalars((select(Project))).all()
    else:
        return [i[0] for i in session.scalars((select(Project).where(Project.name.in_(project))))]

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
        logger.warning(f"Could no commit {project_name}: {error}")
        session.rollback()

    return session.scalars(select(Project).where(Project.name == project_name)).one()

def create_project(project_name, fms_id=None, session=None):
    """Creating new project"""
    if not session:
        session = database.get_session()

    project = Project(name=project_name, fms_id=fms_id)

    session.add(project)

    try:
        session.commit()
    except exc.SQLAlchemyError as error:
        logger.error("Error: %s", error)
        session.rollback()

def ingest_run_processing(project_name, ingest_data, session=None):
    """Ingesting run for MoH"""
    if not isinstance(ingest_data, dict):
        ingest_data = json.loads(ingest_data)

    if not session:
        session = database.get_session()

    project = session.scalars(select(Project).where(Project.name == project_name)).first()

    bundle_config = Bundle(uri=ingest_data[vb.BUNDLE_URI])
    file_config = File(
        content=ingest_data[vb.FILE_CONFIG_CONTENT],
        type=ingest_data[vb.FILE_CONFIG_TYPE],
        bundle=bundle_config
        )
    operation_config = OperationConfig(
        name="ingestion",
        version="0.1",
        bundle=bundle_config
        )
    operation = Operation(
        platform="abacus",
        name="ingestion",
        status=StatusEnum("DONE"),
        operation_config=operation_config,
        project=project
        )
    job = Job(
        name="run_processing_parsing",
        status=StatusEnum("DONE"),
        start=datetime.now(),
        stop=datetime.now(),
        operation=operation
        )
    session.add(file_config)
    # Defining Experiment
    experiment = session.scalars(
        select(Experiment)
        .where(Experiment.sequencing_technology == ingest_data[vb.EXPERIMENT_SEQUENCING_TECHNOLOGY])
        .where(Experiment.type == ingest_data[vb.EXPERIMENT_TYPE])
        .where(Experiment.library_kit == ingest_data[vb.EXPERIMENT_LIBRARY_KIT])
        .where(Experiment.kit_expiration_date == ingest_data[vb.EXPERIMENT_KIT_EXPIRATION_DATE])
        ).first()
    if not experiment:
        experiment = Experiment(
            sequencing_technology=ingest_data[vb.EXPERIMENT_SEQUENCING_TECHNOLOGY],
            type=ingest_data[vb.EXPERIMENT_TYPE],
            library_kit=ingest_data[vb.EXPERIMENT_LIBRARY_KIT],
            kit_expiration_date=datetime.strptime(ingest_data[vb.EXPERIMENT_KIT_EXPIRATION_DATE], "%d/%m/%Y")
            )
    # Defining Run
    run = session.scalars(
        select(Run)
        .where(Run.fms_id == ingest_data[vb.RUN_FMS_ID])
        .where(Run.name == ingest_data[vb.RUN_NAME])
        .where(Run.instrument == ingest_data[vb.RUN_INSTRUMENT])
        .where(Run.date == ingest_data[vb.RUN_DATE])
        ).first()
    if not run:
        run = Run(
            fms_id=ingest_data[vb.RUN_FMS_ID],
            name=ingest_data[vb.RUN_NAME],
            instrument=ingest_data[vb.RUN_INSTRUMENT],
            date=datetime.strptime(ingest_data[vb.RUN_DATE], "%d/%m/%Y %H:%M:%S")
            )
    # Defining Bundle
    bundle = Bundle(uri=ingest_data[vb.BUNDLE_URI], job=job)
    for patient_json in ingest_data[vb.PATIENT]:
        patient = Patient(name=patient_json[vb.PATIENT_NAME], cohort=patient_json[vb.PATIENT_COHORT], institution=patient_json[vb.PATIENT_INSTITUTION], project=project)
        for sample_json in patient_json[vb.SAMPLE]:
            sample = Sample(
                name=sample_json[vb.SAMPLE_NAME],
                tumour=sample_json[vb.SAMPLE_TUMOUR],
                patient=patient
                )
            for readset_json in sample_json[vb.READSET]:
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
                    operation=[operation],
                    job=[job]
                    )
                for file_json in readset_json[vb.FILE]:
                    suffixes = Path(file_json[vb.FILE_CONTENT]).suffixes
                    file_type = os.path.splitext(file_json[vb.FILE_CONTENT])[-1][1:]
                    if ".gz" in suffixes:
                        file_type = "".join(suffixes)[1:]
                    try:
                        File(
                            content=file_json[vb.FILE_CONTENT],
                            type=file_type,
                            extra_metadata=file_json[vb.FILE_EXTRA_METADATA],
                            bundle=bundle,
                            readset=[readset]
                            )
                    except KeyError:
                        File(
                            content=file_json[vb.FILE_CONTENT],
                            type=file_type,
                            bundle=bundle,
                            readset=[readset]
                            )
                for metric_json in readset_json[vb.METRIC]:
                    Metric(
                        name=metric_json[vb.METRIC_NAME],
                        value=metric_json[vb.METRIC_VALUE],
                        job=job,
                        readset=[readset]
                        )

        session.add(readset)
        session.flush()
        operation_id = operation.id

    try:
        session.commit()
    except exc.SQLAlchemyError as error:
        logger.error("Error: %s", error)
        session.rollback()

    return session.scalars(select(Operation).where(Operation.id == operation_id)).one()

def digest_readset(run_name, output_file, session=None):
    """Creating readset file for GenPipes"""
    if not session:
        session = database.get_session()

    readset_header = [
        "Sample",
        "Readset",
        "LibraryType",
        "RunType",
        "Run",
        "Lane",
        "Adapter1",
        "Adapter2",
        "QualityOffset",
        "BED",
        "FASTQ1",
        "FASTQ2",
        "BAM"
        ]

    readsets = session.scalars(select(Readset).where(Run.name == run_name).join(Run)).all()

    with open(output_file, "w", encoding="utf-8") as out_readset_file:
        tsv_writer = csv.DictWriter(out_readset_file, delimiter='\t', fieldnames=readset_header)
        tsv_writer.writeheader()
        for readset in readsets:
            files = readset.file
            bed = ""
            for file in files:
                if file.type in ["fastq", "fq", "fq.gz", "fastq.gz"]:
                    bam = ""
                    if file.extra_metadata == "R1":
                        fastq1 = file.content
                    elif file.extra_metadata == "R2":
                        fastq2 = file.content
                elif file.type == "bam":
                    bam = file.content
                    fastq1 = ""
                    fastq2 = ""
                if file.type == "bed":
                    bed = file.content
            readset_line = {
                "Sample": readset.sample.name,
                "Readset": readset.name,
                "LibraryType": readset.experiment.library_kit,
                "RunType": readset.sequencing_type.value,
                "Run": run_name,
                "Lane": readset.lane.value,
                "Adapter1": readset.adapter1,
                "Adapter2": readset.adapter2,
                "QualityOffset": readset.quality_offset,
                "BED": bed,
                "FASTQ1": fastq1,
                "FASTQ2": fastq2,
                "BAM": bam
                }
            tsv_writer.writerow(readset_line)

def digest_pair(run_name, output_file, session=None):
    """Creating pair file for GenPipes Tumour Pair pipeline"""
    if not session:
        session = database.get_session()

    patients = session.scalars(select(Patient).select_from(Run).where(Run.name == run_name)).all()

    with open(output_file, "w", encoding="utf-8") as out_pair_file:
        csv_writer = csv.writer(out_pair_file, delimiter=',')
        for patient in patients:
            if len(patient.sample) > 1:
                tumour = []
                normal = []
                for sample in patient.sample:
                    if sample.tumour:
                        tumour.append(sample.name)
                    else:
                        normal.append(sample.name)
                combinations = [(patient.name, x, y) for x in normal for y in tumour]
                csv_writer.writerows(combinations)
