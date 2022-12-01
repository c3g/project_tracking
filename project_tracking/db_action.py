import inspect
import re
import json
import os
import logging
from sqlalchemy import select, exc
from datetime import datetime

from . import database
from .model import (
    LaneEnum,
    SequencingTypeEnum,
    StatusEnum,
    FlagEnum,
    AggregateEnum,
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

def projects():
    """Fetchin all projects in database for testing"""
    session = database.get_session()
    return [i[0] for i in session.execute((select(Project))).fetchall()]

def ingest_run_processing(project_name, ingest_data, session=None):
    """Ingesting run for MoH"""
    if not isinstance(ingest_data, dict):
        ingest_data = json.loads(ingest_data)

    if not session:
        session = database.get_session()

    project = session.execute(select(Project).where(Project.name == project_name)).first()[0]
    # print(project[0])

    bundle_config = Bundle(uri="abacus://lb/robot/research/processing/novaseq/2022/220511_A01433_0166_BHM3YVDSX2_MoHRun74-novaseq")
    file_config = File(content="filename", type="event", bundle=bundle_config)
    operation_config = OperationConfig(name="ingestion", version="0.1", bundle=bundle_config)
    operation = Operation(platform="abacus", name="ingestion", status=StatusEnum("DONE"), operation_config=operation_config, project=project)
    job = Job(name="run_processing_parsing", status=StatusEnum("DONE"), start=datetime.now(), stop=datetime.now(), operation=operation)
    session.add(file_config)
    for line in ingest_data:
        # print(line)
        sample_name = line["Sample Name"]
        result = re.search(r"^((MoHQ-(JG|CM|GC|MU|MR|XX)-\w+)-\w+)-\w+-\w+(D|R)(T|N)", sample_name)
        patient_name = result.group(1)
        cohort = result.group(2)
        institution = result.group(3)
        tumour = False
        if sample_name.endswith("DT"):
            tumour = True
        patient = Patient(name=patient_name, cohort=cohort, institution=institution, project=project)
        sample = Sample(name=sample_name, tumour=tumour, patient=patient)
        if session.execute(select(Experiment).where(Experiment.type == line["Library Type"])).first():
            experiment = session.execute(select(Experiment).where(Experiment.type == line["Library Type"])).first()[0]
        else:
            experiment = Experiment(type=line["Library Type"])
        run_name = line["Processing Folder Name"].split("_")[-1].split("-")[0]
        instrument = line["Processing Folder Name"].split("_")[-1].split("-")[-1]
        run = Run(name=run_name, instrument=instrument)
        if session.execute(select(Experiment).where(Experiment.type == line["Library Type"])).first():
            run = session.execute(select(Run).where(Run.name == run_name).where(Run.instrument == instrument)).first()[0]
        else:
            run = Run(name=run_name, instrument=instrument)
        readset = Readset(
            name=f"{sample_name}_{line['Library ID']}_{line['Lane']}",
            lane=LaneEnum(line['Lane']),
            adapter1=line["i7 Adapter Sequence"],
            adapter2=line["i5 Adapter Sequence"],
            sequencing_type=SequencingTypeEnum(line["Run Type"]),
            quality_offset="33",
            sample=sample,
            experiment=experiment,
            run=run,
            operation=[operation],
            job=[job])
        bundle = Bundle(uri="abacus://lb/robot/research/processing/novaseq/2022/220511_A01433_0166_BHM3YVDSX2_MoHRun74-novaseq", job=job)
        content = os.path.basename(line["Path"])
        file_type = os.path.splitext(line["Path"])[-1]
        file = File(content=content, type=file_type, bundle=bundle)
        Metric(name="Clusters", value=line["Clusters"], job=job, readset=[readset])
        Metric(name="Bases", value=line["Bases"], job=job, readset=[readset])
        Metric(name="Avg. Qual", value=line["Avg. Qual"], job=job, readset=[readset])
        Metric(name="Dup. Rate (%)", value=line["Dup. Rate (%)"], job=job, readset=[readset])

        session.add(readset)
        session.add(file)
        session.flush()

    try:
        session.commit()
    except exc.SQLAlchemyError as error:
        logger.error("Error: %s", error)
        session.rollback()

    return operation
