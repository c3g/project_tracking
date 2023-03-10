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

def ingest_run_processing(project_name, ingest_data, session=None):
    """Ingesting run for MoH"""
    if not isinstance(ingest_data, dict):
        ingest_data = json.loads(ingest_data)

    if not session:
        session = database.get_session()

    project = session.scalars(select(Project).where(Project.name == project_name)).first()

    bundle_config = Bundle(uri=ingest_data[vb.BUNDLE_CONFIG_URI])
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
    # Defining Run
    run = session.scalars(
        select(Run)
        .where(Run.fms_id == ingest_data[vb.RUN_FMS_ID])
        .where(Run.name == ingest_data[vb.RUN_NAME])
        .where(Run.instrument == ingest_data[vb.RUN_INSTRUMENT])
        .where(Run.date == str(datetime.strptime(ingest_data[vb.RUN_DATE], "%d/%m/%Y %H:%M:%S")))
        ).first()
    if not run:
        run = Run(
            fms_id=ingest_data[vb.RUN_FMS_ID],
            name=ingest_data[vb.RUN_NAME],
            instrument=ingest_data[vb.RUN_INSTRUMENT],
            date=datetime.strptime(ingest_data[vb.RUN_DATE], "%d/%m/%Y %H:%M:%S")
            )

    for patient_json in ingest_data[vb.PATIENT]:
        patient = Patient(name=patient_json[vb.PATIENT_NAME], cohort=patient_json[vb.PATIENT_COHORT], institution=patient_json[vb.PATIENT_INSTITUTION], project=project)
        for sample_json in patient_json[vb.SAMPLE]:
            sample = Sample(
                name=sample_json[vb.SAMPLE_NAME],
                tumour=sample_json[vb.SAMPLE_TUMOUR],
                patient=patient
                )
            for readset_json in sample_json[vb.READSET]:
                # Defining Experiment
                experiment = session.scalars(
                    select(Experiment)
                    .where(Experiment.sequencing_technology == readset_json[vb.EXPERIMENT_SEQUENCING_TECHNOLOGY])
                    .where(Experiment.type == readset_json[vb.EXPERIMENT_TYPE])
                    .where(Experiment.library_kit == readset_json[vb.EXPERIMENT_LIBRARY_KIT])
                    .where(Experiment.kit_expiration_date == str(datetime.strptime(readset_json[vb.EXPERIMENT_KIT_EXPIRATION_DATE], "%d/%m/%Y")))
                    ).first()
                if not experiment:
                    experiment = Experiment(
                        sequencing_technology=readset_json[vb.EXPERIMENT_SEQUENCING_TECHNOLOGY],
                        type=readset_json[vb.EXPERIMENT_TYPE],
                        library_kit=readset_json[vb.EXPERIMENT_LIBRARY_KIT],
                        kit_expiration_date=datetime.strptime(readset_json[vb.EXPERIMENT_KIT_EXPIRATION_DATE], "%d/%m/%Y")
                        )
                # Defining Bundle
                bundle = session.scalars(
                    select(Bundle)
                    .where(Bundle.uri == readset_json[vb.BUNDLE_URI])
                    ).first()
                if not bundle:
                    bundle = Bundle(
                        uri=readset_json[vb.BUNDLE_URI],
                        job=job
                        )
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

    stmt = select(Patient).join(Patient.sample).join(Sample.readset).join(Readset.run).where(Run.name == run_name)
    patients = session.scalars(stmt).unique().all()

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

def ingest_old_database(old_database, run_dict, session=None):
    """Ingesting MoH old database"""
    if not session:
        session = database.get_session()

    project = Project(name="MoH-Q")

    samples_dict = {}
    patients_list = []
    connection = sqlite3.connect(old_database)
    cur = connection.cursor()
    cur.execute("""SELECT * FROM Samples""")
    samples_table = cur.fetchall()
    for line in samples_table:
        if line[0] not in ("NA", ""):
            if line[0] == line[1]:
                patient = Patient(
                    name=line[0],
                    institution=line[2],
                    cohort=line[3],
                    project=project,
                    )
            else:
                patient = Patient(
                    name=line[0],
                    alias=line[1],
                    institution=line[2],
                    cohort=line[3],
                    project=project,
                    )
            if line[4] not in ("NA", ""):
                cur.execute(f"""SELECT Run_Proc_BAM_DNA_N FROM Timestamps WHERE Sample = \"{line[0]}\"""")
                try:
                    dna_n_creation = datetime.strptime(cur.fetchone()[0], "%Y/%m/%d")
                except (ValueError, TypeError):
                    dna_n_creation = datetime.now()
                if line[4] == line[5]:
                    dna_n = Sample(name=line[4], creation=dna_n_creation)
                else:
                    dna_n = Sample(name=line[4], alias=line[5], creation=dna_n_creation)
                patient.sample.append(dna_n)
                samples_dict[line[4]] = dna_n
            if line[6] not in ("NA", ""):
                cur.execute(f"""SELECT Run_Proc_BAM_DNA_T FROM Timestamps WHERE Sample = \"{line[0]}\"""")
                try:
                    dna_t_creation = datetime.strptime(cur.fetchone()[0], "%Y/%m/%d")
                except ValueError:
                    dna_t_creation = datetime.now()
                if line[6] == line[7]:
                    dna_t = Sample(name=line[6], tumour=True, creation=dna_n_creation)
                else:
                    dna_t = Sample(name=line[6], tumour=True, alias=line[7], creation=dna_n_creation)
                patient.sample.append(dna_t)
                samples_dict[line[6]] = dna_t
            if line[8] not in ("NA", ""):
                cur.execute(f"""SELECT Run_Proc_fastq_1_RNA FROM Timestamps WHERE Sample = \"{line[0]}\"""")
                try:
                    rna_creation = datetime.strptime(cur.fetchone()[0], "%Y/%m/%d")
                except ValueError:
                    rna_creation = datetime.now()
                if line[8] == line[9]:
                    rna = Sample(name=line[8], tumour=True, creation=rna_creation)
                else:
                    rna = Sample(name=line[8], tumour=True, alias=line[9], creation=rna_creation)
                patient.sample.append(rna)
                samples_dict[line[8]] = rna
            patient.creation = min(dna_n_creation, dna_t_creation, rna_creation)
            patients_list.append(patient)
    for run_name in run_dict:
        year = run_name.split("_")[0][0:2]
        bundle_config = Bundle(
            uri=f"abacus:///lb/robot/research/processing/novaseq/20{year}/{run_name}",
            creation=datetime.strptime(run_name.split("_")[0], "%y%m%d")
            )
        file_config = File(
            content="",
            type="event",
            bundle=bundle_config,
            creation=datetime.strptime(run_name.split("_")[0], "%y%m%d")
            )
        operation_config = OperationConfig(
            name="ingestion",
            version="0.1",
            bundle=bundle_config,
            creation=datetime.strptime(run_name.split("_")[0], "%y%m%d")
            )
        operation = Operation(
            platform="abacus",
            name="ingestion",
            status=StatusEnum("DONE"),
            operation_config=operation_config,
            project=project,
            creation=datetime.strptime(run_name.split("_")[0], "%y%m%d")
            )
        job = Job(
            name="run_processing_parsing",
            status=StatusEnum("DONE"),
            start=datetime.strptime(run_name.split("_")[0], "%y%m%d"),
            stop=datetime.strptime(run_name.split("_")[0], "%y%m%d"),
            operation=operation,
            creation=datetime.strptime(run_name.split("_")[0], "%y%m%d")
            )
        session.add(file_config)
        # Defining Run
        run = session.scalars(
            select(Run)
            .where(Run.name == run_name)
            .where(Run.instrument == run_name.split("-")[-1])
            .where(Run.date == datetime.strptime(run_name.split("_")[0], "%y%m%d"))
            ).first()
        if not run:
            run = Run(
                name=run_name,
                instrument=run_name.split("-")[-1],
                date=datetime.strptime(run_name.split("_")[0], "%y%m%d")
                )
        session.add(run)
        for readset_line in run_dict[run_name]:
            experiment = session.scalars(
                select(Experiment)
                .where(Experiment.type == readset_line[8])
                ).first()
            if not experiment:
                experiment = Experiment(
                    type=readset_line[8],
                    creation=datetime.strptime(run_name.split("_")[0], "%y%m%d")
                    )
            try:
                readset = Readset(
                        name=f"{readset_line[6]}_{readset_line[2]}",
                        lane=LaneEnum(readset_line[2]),
                        adapter1=readset_line[27],
                        adapter2=readset_line[28],
                        sequencing_type=SequencingTypeEnum(readset_line[3]),
                        quality_offset="33",
                        sample=samples_dict[readset_line[6]],
                        experiment=experiment,
                        run=run,
                        operation=[operation],
                        job=[job]
                        )
                session.add(readset)
            except KeyError as error:
                # Logging samples in MAIN/raw_reads but not in any run file
                logger.debug(error)

            session.flush()

    # To know if there are samples not associated to any readset
    # sample_l = session.scalars(select(Sample)).all()
    # for sampl in sample_l:
    #     if sampl.readset == []:
    #         logger.debug(sampl.name)

    patients = session.scalars(select(Patient).select_from(Run).where(Run.name == run_name)).all()

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

    readset_list = session.scalars(select(Readset)).all()
    for readset in readset_list:
        # TODO: define "transfer" and "genpipes" operations
        if readset.sample.name.endwith("RT"):
            operation_config = OperationConfig(
                name="ingestion",
                version="0.1",
                bundle=bundle_config,
                creation=datetime.strptime(run_name.split("_")[0], "%y%m%d")
                )
            operation = Operation(
                platform="beluga",
                name="ingestion",
                status=StatusEnum("DONE"),
                operation_config=operation_config,
                project=project,
                creation=datetime.strptime(run_name.split("_")[0], "%y%m%d")
                )


    try:
        session.commit()
    except exc.SQLAlchemyError as error:
        logger.error("Error: %s", error)
        session.rollback()
