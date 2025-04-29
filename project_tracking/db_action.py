import inspect
import re
import json
import os
import logging
import csv
import sqlite3

from datetime import datetime
from sqlalchemy import select
from sqlalchemy import delete as sql_delete
from sqlalchemy.exc import (
    SQLAlchemyError,
    IntegrityError
    )
from pathlib import Path

from . import vocabulary as vb
from . import database
from .model import (
    LaneEnum,
    NucleicAcidTypeEnum,
    SequencingTypeEnum,
    StateEnum,
    StatusEnum,
    FlagEnum,
    AggregateEnum,
    readset_file,
    Project,
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


class Error(Exception):
    """Generic error for db_action"""
    status_code = 400

    def __init__(self, message, status_code=None, payload=None):
        super().__init__()
        self.message = message
        if status_code is not None:
            self.status_code = status_code
        self.payload = payload

    def to_dict(self):
        rv = dict(self.payload or ())
        rv['DB_ACTION_ERROR'] = self.message
        return rv

class DidNotFindError(Error):
    """DidNotFindError"""
    def __init__(self, message=None, table=None, attribute=None, query=None):
        super().__init__(message)
        if message:
            self.message = message
        else:
            self.message = f"'{table}' with '{attribute}' '{query}' doesn't exist in the database"

class RequestError(Error):
    """RequestError"""
    def __init__(self, message=None, argument=None):
        super().__init__(message)
        if message:
            self.message = message
        else:
            self.message = f"For current request '{argument}' is required"

class UniqueConstraintError(Error):
    """UniqueConstraintError"""
    def __init__(self, message=None, entity=None, attribute=None, value=None):
        super().__init__(message)
        if message:
            self.message = message
        else:
            self.message = f"'{entity}' with '{attribute}' '{value}' already exists in the database and '{attribute}' has to be unique"

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
                    readset = session.query(Readset).filter(Readset.name == readset_name).first()
                    if readset:
                        ret.append(f"'Readset' with 'name' '{readset_name}' already exists in the database and 'name' has to be unique")
    return ret


def name_to_id(model_class, name, session=None):
    """
    Converting a given name into its id(s) for a given model_class.
    """
    if session is None:
        session = database.get_session()

    from . import model
    the_class = getattr(model, model_class)

    if isinstance(name, str):
        name = [name]

    ids = session.query(the_class.id).filter(the_class.name.in_(name)).all()

    return [id[0] for id in ids]

def fetch_specimen_by_attr(session, attr, value):
    """
    Fetch a specimen by a given attribute and value.
    """
    return session.query(Specimen).filter(getattr(Specimen, attr) == value).first()

def fetch_sample_by_attr(session, attr, value):
    """
    Fetch a sample by a given attribute and value.
    """
    return session.query(Sample).filter(getattr(Sample, attr) == value).first()


def fetch_readset_by_attr(session, attr, value):
    """
    Fetch a readset by a given attribute and value.
    """
    return session.query(Readset).filter(getattr(Readset, attr) == value).first()

def select_samples_from_specimens(session, ret, digest_data, nucleic_acid_type):
    """Returning Samples Objects based on requested specimens in digest_data"""
    specimens = []
    samples = []

    if vb.SPECIMEN_NAME in digest_data:
        for specimen_name in digest_data[vb.SPECIMEN_NAME]:
            specimen = fetch_specimen_by_attr(session, 'name', specimen_name)
            if specimen:
                specimens.append(specimen)
            else:
                raise DidNotFindError(table="Specimen", attribute="name", query=specimen_name)
    if vb.SPECIMEN_ID in digest_data:
        for specimen_id in digest_data[vb.SPECIMEN_ID]:
            specimen = fetch_specimen_by_attr(session, 'id', specimen_id)
            if specimen:
                specimens.append(specimen)
            else:
                raise DidNotFindError(table="Specimen", attribute="id", query=specimen_id)
    if specimens:
        for specimen in set(specimens):
            for sample in specimen.samples:
                if sample.readsets[0].experiment.nucleic_acid_type == nucleic_acid_type and not sample.deprecated and not sample.deleted:
                    samples.append(sample)
                else:
                    ret["DB_ACTION_WARNING"].append(f"'Sample' with 'name' '{sample.name}' only exists with 'nucleic_acid_type' '{sample.readsets[0].experiment.nucleic_acid_type.value}' on database. Skipping...")
    return samples

def select_samples_from_samples(session, ret, digest_data, nucleic_acid_type):
    """Returning Samples Objects based on requested samples in digest_data"""
    samples = []

    if vb.SAMPLE_NAME in digest_data.keys():
        for sample_name in digest_data[vb.SAMPLE_NAME]:
            sample = fetch_sample_by_attr(session, 'name', sample_name)
            if sample:
                samples.append(sample)
            else:
                raise DidNotFindError(table="Sample", attribute="name", query=sample_name)
    if vb.SAMPLE_ID in digest_data.keys():
        for sample_id in digest_data[vb.SAMPLE_ID]:
            sample = fetch_sample_by_attr(session, 'name', sample_id)
            if sample:
                samples.append(sample)
            else:
                raise DidNotFindError(table="Sample", attribute="id", query=sample_id)
    if samples:
        for sample in set(samples):
            if sample.readsets[0].experiment.nucleic_acid_type != nucleic_acid_type or sample.deprecated or sample.deleted:
                samples.remove(sample)
                ret["DB_ACTION_WARNING"].append(f"'Sample' with 'name' '{sample.name}' only exists with 'nucleic_acid_type' '{sample.readsets[0].experiment.nucleic_acid_type.value}' on database. Skipping...")
    return samples

def select_samples_from_readsets(session, ret, digest_data, nucleic_acid_type):
    """Returning Samples Objects based on requested readsets in digest_data"""
    samples = []
    readsets = []

    if vb.READSET_NAME in digest_data.keys():
        for readset_name in digest_data[vb.READSET_NAME]:
            readset = fetch_readset_by_attr(session, 'name', readset_name)
            if readset:
                readsets.append(readset)
            else:
                raise DidNotFindError(table="Readset", attribute="name", query=readset_name)
    if vb.READSET_ID in digest_data.keys():
        for readset_id in digest_data[vb.READSET_ID]:
            readset = fetch_readset_by_attr(session, 'id', readset_id)
            if readset:
                readsets.append(readset)
            else:
                raise DidNotFindError(table="Readset", attribute="id", query=readset_name)
    if readsets:
        for readset in set(readsets):
            if readset.experiment.nucleic_acid_type == nucleic_acid_type and not readset.deprecated and not readset.deleted:
                if readset.sample not in samples:
                    samples.append(readset.sample)
                else:
                    ret["DB_ACTION_WARNING"].append(f"'Sample' with 'name' '{readset.sample.name}' only exists with 'nucleic_acid_type' '{readset.experiment.nucleic_acid_type.value}' on database. Skipping...")
    return samples

def select_readsets_from_specimens(session, ret, digest_data, nucleic_acid_type):
    """Returning Readsets Objects based on requested specimens in digest_data"""
    specimens = []
    readsets = []

    if vb.SPECIMEN_NAME in digest_data:
        for specimen_name in digest_data[vb.SPECIMEN_NAME]:
            specimen = fetch_specimen_by_attr(session, 'name', specimen_name)
            if specimen:
                specimens.append(specimen)
            else:
                raise DidNotFindError(table="Specimen", attribute="name", query=specimen_name)

    if vb.SPECIMEN_ID in digest_data:
        for specimen_id in digest_data[vb.SPECIMEN_ID]:
            specimen = fetch_specimen_by_attr(session, 'id', specimen_id)
            if specimen:
                specimens.append(specimen)
            else:
                raise DidNotFindError(table="Specimen", attribute="id", query=specimen_id)
    if specimens:
        for specimen in set(specimens):
            for sample in specimen.samples:
                for readset in sample.readsets:
                    if readset.experiment.nucleic_acid_type == nucleic_acid_type and not readset.deprecated and not readset.deleted:
                        readsets.append(readset)
                    else:
                        ret["DB_ACTION_WARNING"].append(f"'Readset' with 'name' '{readset.name}' only exists with 'nucleic_acid_type' '{readset.experiment.nucleic_acid_type.value}' on database. Skipping...")
    return readsets

def select_readsets_from_samples(session, ret, digest_data, nucleic_acid_type):
    """Returning Readsets Objects based on requested samples in digest_data"""
    samples = []
    readsets = []

    if vb.SAMPLE_NAME in digest_data.keys():
        for sample_name in digest_data[vb.SAMPLE_NAME]:
            sample = fetch_sample_by_attr(session, 'name', sample_name)
            if sample:
                samples.append(sample)
            else:
                raise DidNotFindError(table="Sample", attribute="name", query=sample_name)
    if vb.SAMPLE_ID in digest_data.keys():
        for sample_id in digest_data[vb.SAMPLE_ID]:
            sample = fetch_sample_by_attr(session, 'id', sample_id)
            if sample:
                samples.append(sample)
            else:
                raise DidNotFindError(table="Sample", attribute="id", query=sample_id)
    if samples:
        for sample in set(samples):
            for readset in sample.readsets:
                if readset.experiment.nucleic_acid_type == nucleic_acid_type and not readset.deprecated and not readset.deleted:
                    readsets.append(readset)
                else:
                    ret["DB_ACTION_WARNING"].append(f"'Readset' with 'name' '{readset.name}' only exists with 'nucleic_acid_type' '{readset.experiment.nucleic_acid_type.value}' on database. Skipping...")
    return readsets

def select_readsets_from_readsets(session, ret, digest_data, nucleic_acid_type):
    """Returning Readsets Objects based on requested readsets in digest_data"""
    readsets = []

    if vb.READSET_NAME in digest_data.keys():
        for readset_name in digest_data[vb.READSET_NAME]:
            readset = fetch_readset_by_attr(session, 'name', readset_name)
            if readset:
                readsets.append(readset)
            else:
                raise DidNotFindError(table="Readset", attribute="name", query=readset_name)
    if vb.READSET_ID in digest_data.keys():
        for readset_id in digest_data[vb.READSET_ID]:
            readset = fetch_readset_by_attr(session, 'id', readset_id)
            if readset:
                readsets.append(readset)
            else:
                raise DidNotFindError(table="Readset", attribute="id", query=readset_id)
    if readsets:
        for readset in set(readsets):
            if readset.experiment.nucleic_acid_type != nucleic_acid_type or readset.deprecated or readset.deleted:
                readsets.remove(readset)
                ret["DB_ACTION_WARNING"].append(f"'Readset' with 'name' '{readset.name}' only exists with 'nucleic_acid_type' '{readset.experiment.nucleic_acid_type.value}' on database. Skipping...")

    return readsets

def projects(project_id=None, session=None):
    """
    Fetching all projects in the database.
    """
    if session is None:
        session = database.get_session()

    if project_id is None:
        projects = session.query(Project).all()
    else:
        if isinstance(project_id, str):
            project_id = [project_id]

        projects = session.query(Project).filter(
            Project.id.in_(project_id),
            Project.deprecated.is_(False),
            Project.deleted.is_(False)
        ).all()

    return projects

def metrics_deliverable(project_id: str, deliverable: bool, specimen_id=None, sample_id=None, readset_id=None, metric_id=None, deprecated=False, deleted=False):
    """
    deliverable = True: Returns only specimens that have a tumor and a normal sample
    deliverable = False, Tumor = False: Returns specimens that only have normal samples
    """
    session = database.get_session()
    if isinstance(project_id, str):
        project_id = [project_id]

    query = session.query(Metric).filter(
        Metric.deliverable == deliverable,
        Metric.deprecated.is_(deprecated),
        Metric.deleted.is_(deleted)
    ).join(Metric.readsets).join(Readset.sample).join(Sample.specimen).join(Specimen.project).filter(
        Project.id.in_(project_id)
    )

    if metric_id:
        if isinstance(metric_id, int):
            metric_id = [metric_id]
        query = query.filter(Metric.id.in_(metric_id))
    elif specimen_id:
        if isinstance(specimen_id, int):
            specimen_id = [specimen_id]
        query = query.filter(Specimen.id.in_(specimen_id))
    elif sample_id:
        if isinstance(sample_id, int):
            sample_id = [sample_id]
        query = query.filter(Sample.id.in_(sample_id))
    elif readset_id:
        if isinstance(readset_id, int):
            readset_id = [readset_id]
        query = query.filter(Readset.id.in_(readset_id))

    return query.all()



def metrics(project_id=None, specimen_id=None, sample_id=None, readset_id=None, metric_id=None, deprecated=False, deleted=False):
    """
    Fetching all metrics that are part of the project or specimen or sample or readset.
    """
    session = database.get_session()
    if isinstance(project_id, str):
        project_id = [project_id]

    query = session.query(Metric).filter(
        Metric.deprecated.is_(deprecated),
        Metric.deleted.is_(deleted)
    ).join(Metric.readsets).join(Readset.sample).join(Sample.specimen).join(Specimen.project).filter(
        Project.id.in_(project_id)
    )

    if metric_id:
        if isinstance(metric_id, int):
            metric_id = [metric_id]
        query = query.filter(Metric.id.in_(metric_id))
    elif specimen_id:
        if isinstance(specimen_id, int):
            specimen_id = [specimen_id]
        query = query.filter(Specimen.id.in_(specimen_id))
    elif sample_id:
        if isinstance(sample_id, int):
            sample_id = [sample_id]
        query = query.filter(Sample.id.in_(sample_id))
    elif readset_id:
        if isinstance(readset_id, int):
            readset_id = [readset_id]
        query = query.filter(Readset.id.in_(readset_id))

    return query.all()


def files_deliverable(project_id: str, deliverable: bool, specimen_id=None, sample_id=None, readset_id=None, file_id=None, deprecated=False, deleted=False):
    """
    deliverable = True: Returns only files labelled as deliverable
    deliverable = False: Returns only files NOT labelled as deliverable
    """
    session = database.get_session()
    if isinstance(project_id, str):
        project_id = [project_id]

    query = session.query(File).filter(
        File.deliverable == deliverable,
        File.deprecated.is_(deprecated),
        File.deleted.is_(deleted)
    ).join(File.readsets).join(Readset.sample).join(Sample.specimen).join(Specimen.project).filter(
        Project.id.in_(project_id)
    )

    if file_id:
        if isinstance(file_id, int):
            file_id = [file_id]
        query = query.filter(File.id.in_(file_id))
    elif specimen_id:
        if isinstance(specimen_id, int):
            specimen_id = [specimen_id]
        query = query.filter(Specimen.id.in_(specimen_id))
    elif sample_id:
        if isinstance(sample_id, int):
            sample_id = [sample_id]
        query = query.filter(Sample.id.in_(sample_id))
    elif readset_id:
        if isinstance(readset_id, int):
            readset_id = [readset_id]
        query = query.filter(Readset.id.in_(readset_id))

    return query.all()


def files(project_id=None, specimen_id=None, sample_id=None, readset_id=None, file_id=None, deprecated=False, deleted=False):
    """
    Fetching all files that are linked to readset.
    """
    session = database.get_session()

    if isinstance(project_id, str):
        project_id = [project_id]

    query = session.query(File).filter(
        File.deprecated.is_(deprecated),
        File.deleted.is_(deleted)
    ).join(File.readsets).join(Readset.sample).join(Sample.specimen).join(Specimen.project).filter(
        Project.id.in_(project_id)
    )

    if file_id:
        if isinstance(file_id, int):
            file_id = [file_id]
        query = query.filter(File.id.in_(file_id))
    elif specimen_id:
        if isinstance(specimen_id, int):
            specimen_id = [specimen_id]
        query = query.filter(Specimen.id.in_(specimen_id))
    elif sample_id:
        if isinstance(sample_id, int):
            sample_id = [sample_id]
        query = query.filter(Sample.id.in_(sample_id))
    elif readset_id:
        if isinstance(readset_id, int):
            readset_id = [readset_id]
        query = query.filter(Readset.id.in_(readset_id))

    return query.all()


def readsets(project_id=None, sample_id=None, readset_id=None, deprecated=False, deleted=False):
    """
    Fetching all readsets that are part of the project or sample.
    """
    session = database.get_session()

    if isinstance(project_id, str):
        project_id = [project_id]

    query = session.query(Readset).filter(
        Readset.deprecated.is_(deprecated),
        Readset.deleted.is_(deleted)
    )

    if project_id is None and sample_id is None and readset_id is None:
        # Fetch all readsets
        return query.all()
    elif project_id and sample_id is None and readset_id is None:
        # Fetch readsets by project_id
        query = query.join(Readset.sample).join(Sample.specimen).join(Specimen.project).filter(
            Project.id.in_(project_id)
        )
    elif sample_id and project_id:
        if isinstance(sample_id, int):
            sample_id = [sample_id]
        # Fetch readsets by sample_id and project_id
        query = query.join(Readset.sample).filter(
            Sample.id.in_(sample_id),
            Project.id.in_(project_id)
        ).join(Sample.specimen).join(Specimen.project).filter(
            Project.id.in_(project_id)
        )
    elif readset_id and project_id:
        if isinstance(readset_id, int):
            readset_id = [readset_id]
        # Fetch readsets by readset_id and project_id
        query = query.filter(
            Readset.id.in_(readset_id)
        ).join(Readset.sample).join(Sample.specimen).join(Specimen.project).filter(
            Project.id.in_(project_id)
        )

    return query.all()


def specimen_pair(project_id: str, pair: bool, specimen_id=None, tumor: bool=True, deprecated=False, deleted=False):
    """
    Pair = True: Returns only specimens that have a tumor and a normal sample
    Pair = False, Tumor = True: Returns specimens that only have tumor samples
    Pair = False, Tumor = False: Returns specimens that only have normal samples
    """
    session = database.get_session()
    if isinstance(project_id, str):
        project_id = [project_id]

    query_tumor = session.query(Specimen).filter(
        Specimen.deprecated.is_(deprecated),
        Specimen.deleted.is_(deleted)
    ).join(Specimen.samples).filter(
        Sample.tumour.is_(True)
    ).join(Specimen.project).filter(
        Project.id.in_(project_id)
    )

    query_normal = session.query(Specimen).filter(
        Specimen.deprecated.is_(deprecated),
        Specimen.deleted.is_(deleted)
    ).join(Specimen.samples).filter(
        Sample.tumour.is_(False)
    ).join(Specimen.project).filter(
        Project.id.in_(project_id)
    )

    if specimen_id:
        if isinstance(specimen_id, int):
            specimen_id = [specimen_id]
        query_tumor = query_tumor.filter(Specimen.id.in_(specimen_id))
        query_normal = query_normal.filter(Specimen.id.in_(specimen_id))

    specimens_tumor = set(query_tumor.all())
    specimens_normal = set(query_normal.all())

    if pair:
        return specimens_normal.intersection(specimens_tumor)
    if tumor:
        return specimens_tumor.difference(specimens_normal)
    return specimens_normal.difference(specimens_tumor)


def specimens(project_id=None, specimen_id=None, deprecated=False, deleted=False):
    """
    Fetching all specimens from projects or selected specimen by id.
    """
    session = database.get_session()
    if isinstance(project_id, str):
        project_id = [project_id]

    query = session.query(Specimen).filter_by(deprecated=deprecated, deleted=deleted)

    if project_id is None and specimen_id is None:
        # Fetch all specimens
        return query.all()
    if specimen_id is None and project_id:
        # Fetch specimens by project_id
        query = query.join(Specimen.project).filter(
            Project.id.in_(project_id)
        )
    else:
        if isinstance(specimen_id, int):
            specimen_id = [specimen_id]
        # Fetch specimens by specimen_id and project_id
        query = query.filter(
            Specimen.id.in_(specimen_id)
        ).join(Specimen.project).filter(
            Project.id.in_(project_id)
        )

    return query.all()


def samples(project_id=None, sample_id=None, deprecated=False, deleted=False):
    """
    Fetching all projects in database still need to check if sample are part of project when both are provided
    """
    session = database.get_session()
    if isinstance(project_id, str):
        project_id = [project_id]

    query = session.query(Sample).filter_by(deprecated=deprecated, deleted=deleted)

    if project_id:
        if isinstance(project_id, str):
            project_id = [project_id]
        query = query.join(Sample.specimen).join(Specimen.project).filter(Project.id.in_(project_id))

    if sample_id:
        if isinstance(sample_id, int):
            sample_id = [sample_id]
        query = query.filter(Sample.id.in_(sample_id))

    return query.all()


def create_project(project_name, ext_id=None, ext_src=None, session=None):
    """
    Creating new project
    Returns project even if it already exist
    """
    if not session:
        session = database.get_session()

    # Check if the project already exists
    project = session.query(Project).filter_by(name=project_name).first()

    if not project:
        # Create a new project if it doesn't exist
        project = Project(name=project_name, ext_id=ext_id, ext_src=ext_src)
        session.add(project)

        try:
            session.commit()
        except SQLAlchemyError as error:
            logger.warning(f"Could not commit {project_name}: {error}")
            session.rollback()

    return project


def ingest_run_processing(project_id: str, ingest_data, session=None):
    """Ingesting run for MoH"""
    if not isinstance(ingest_data, dict):
        ingest_data = json.loads(ingest_data)

    if not session:
        session = database.get_session()

    ret = {
        "DB_ACTION_WARNING": [],
        "DB_ACTION_OUTPUT": []
        }

    project = projects(project_id=project_id, session=session)[0]

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
            sample = Sample.from_name(
                name=sample_json[vb.SAMPLE_NAME],
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
                        readsets=[readset]
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
    operation = session.query(Operation).filter_by(id=operation_id).first()
    # job
    job = session.query(Job).filter_by(id=job_id).first()

    ret["DB_ACTION_OUTPUT"].append(operation)
    # If no warning
    if not ret["DB_ACTION_WARNING"]:
        ret.pop("DB_ACTION_WARNING")
    # return [operation, jobs]

    return ret


def ingest_transfer(project_id: str, ingest_data, session=None, check_readset_name=True):
    """Ingesting transfer"""
    if not isinstance(ingest_data, dict):
        ingest_data = json.loads(ingest_data)

    if not session:
        session = database.get_session()

    ret = {
        "DB_ACTION_WARNING": [],
        "DB_ACTION_OUTPUT": []
    }

    project = projects(project_id=project_id, session=session)[0]

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
        readset = session.query(Readset).filter_by(
            name=readset_name,
            deprecated=False,
            deleted=False
        ).first()
        readset_list.append(readset)
        for file_json in readset_json[vb.FILE]:
            src_uri = file_json[vb.SRC_LOCATION_URI]
            dest_uri = file_json[vb.DEST_LOCATION_URI]
            if check_readset_name:
                file = session.query(File).filter(
                    File.deprecated.is_(False),
                    File.deleted.is_(False)
                ).join(File.readsets).filter(Readset.name == readset_name).join(File.locations).filter(Location.uri == src_uri).first()
                if not file:
                    raise DidNotFindError(f"No 'File' with 'uri' '{src_uri}' and 'Readset' with 'name' '{readset_name}'")
            else:
                file = session.query(File).filter(
                    File.deprecated.is_(False),
                    File.deleted.is_(False)
                ).join(File.readsets).filter(Readset.name == readset_name).join(File.locations).filter(Location.uri == src_uri).first()
                if not file:
                    raise DidNotFindError(f"No 'File' with 'uri' '{src_uri}'")

            new_location = Location.from_uri(uri=dest_uri, file=file, session=session)
            file.jobs.append(job)
            session.add(new_location)
    operation.readsets = readset_list

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
    operation = session.query(Operation).filter_by(id=operation_id).first()
    # job
    job = session.query(Job).filter_by(id=job_id).first()

    ret["DB_ACTION_OUTPUT"].append(operation)
    # If no warning
    if not ret["DB_ACTION_WARNING"]:
        ret.pop("DB_ACTION_WARNING")
    # return [operation, jobs]
    return ret


def digest_readset_file(project_id: str, digest_data, session=None):
    """Digesting readset file fields for GenPipes"""
    if not session:
        session = database.get_session()

    readsets = []
    ret = {
        "DB_ACTION_WARNING": [],
        "DB_ACTION_OUTPUT": []
        }

    location_endpoint = None
    if vb.LOCATION_ENDPOINT in digest_data.keys():
        location_endpoint = digest_data[vb.LOCATION_ENDPOINT]

    if vb.EXPERIMENT_NUCLEIC_ACID_TYPE in digest_data.keys():
        nucleic_acid_type = NucleicAcidTypeEnum(digest_data[vb.EXPERIMENT_NUCLEIC_ACID_TYPE])
    else:
        raise RequestError(argument="experiment_nucleic_acid_type")

    readsets += select_readsets_from_specimens(session, ret, digest_data, nucleic_acid_type)
    readsets += select_readsets_from_samples(session, ret, digest_data, nucleic_acid_type)
    readsets += select_readsets_from_readsets(session, ret, digest_data, nucleic_acid_type)

    logging.debug(f"Readsets: {readsets}")

    if readsets:
        for readset in readsets:
            readset_files = []
            bed = None
            fastq1 = None
            fastq2 = None
            bam = None
            for operation in [operation for operation in readset.operations if operation.name == 'run_processing']:
                for job in operation.jobs:
                    for file in job.files:
                        if file in readset.files:
                            readset_files.append(file)
            for file in readset_files:
                if file.type in ["fastq", "fq", "fq.gz", "fastq.gz"]:
                    if file.extra_metadata["read_type"] == "R1":
                        if location_endpoint:
                            for location in file.locations:
                                if location_endpoint == location.endpoint:
                                    fastq1 = location.uri.split("://")[-1]
                            if not fastq1:
                                ret["DB_ACTION_WARNING"].append(f"Looking for R1 fastq 'File' for 'Sample' with 'name' '{readset.sample.name}' and 'Readset' with 'name' '{readset.name}' in '{location_endpoint}', file only exists on {[l.endpoint for l in file.locations]}. The readset file might be corrupted.")
                    elif file.extra_metadata["read_type"] == "R2":
                        if location_endpoint:
                            for location in file.locations:
                                if location_endpoint == location.endpoint:
                                    fastq2 = location.uri.split("://")[-1]
                            if not fastq2:
                                ret["DB_ACTION_WARNING"].append(f"Looking for R2 fastq 'File' for 'Sample' with 'name' '{readset.sample.name}' and 'Readset' with 'name' '{readset.name}' in '{location_endpoint}', file only exists on {[l.endpoint for l in file.locations]}. The readset file might be corrupted.")
                elif file.type == "bam":
                    if location_endpoint:
                        for location in file.locations:
                            if location_endpoint == location.endpoint:
                                bam = location.uri.split("://")[-1]
                        if not bam:
                            ret["DB_ACTION_WARNING"].append(f"Looking for bam 'File' for 'Sample' with 'name' '{readset.sample.name}' and 'Readset'  with 'name' '{readset.name}' in '{location_endpoint}', file only exists on {[l.endpoint for l in file.locations]}. The readset file might be corrupted.")
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
                "QualityOffset": "33",
                "BED": bed,
                "FASTQ1": fastq1,
                "FASTQ2": fastq2,
                "BAM": bam
                }
            ret["DB_ACTION_OUTPUT"].append(readset_line)
    # If no warning
    if not ret["DB_ACTION_WARNING"]:
        ret.pop("DB_ACTION_WARNING")
    # if warnings["DB_ACTION_WARNING"]:
    #     ret = warnings
    # else:
    #     ret = output
    return json.dumps(ret)

def digest_pair_file(project_id: str, digest_data, session=None):
    """Digesting pair file fields for GenPipes"""
    if not session:
        session = database.get_session()

    pair_dict = {}
    samples = []
    ret = {
        "DB_ACTION_WARNING": [],
        "DB_ACTION_OUTPUT": []
        }

    if vb.EXPERIMENT_NUCLEIC_ACID_TYPE in digest_data.keys():
        nucleic_acid_type = NucleicAcidTypeEnum(digest_data[vb.EXPERIMENT_NUCLEIC_ACID_TYPE])
    else:
        raise RequestError(argument="experiment_nucleic_acid_type")

    samples += select_samples_from_specimens(session, ret, digest_data, nucleic_acid_type)
    samples += select_samples_from_samples(session, ret, digest_data, nucleic_acid_type)
    samples += select_samples_from_readsets(session, ret, digest_data, nucleic_acid_type)

    if samples:
        for sample in samples:
            if not sample.specimen.name in pair_dict.keys():
                pair_dict[sample.specimen.name] = {
                    "T": None,
                    "N": None
                    }
            if sample.tumour:
                pair_dict[sample.specimen.name]["T"] = sample.name
            else:
                pair_dict[sample.specimen.name]["N"] = sample.name
    if pair_dict:
        for specimen_name, dict_tn in pair_dict.items():
            pair_line = {
                "Specimen": specimen_name,
                "Sample_N": dict_tn["N"],
                "Sample_T": dict_tn["T"]
                }
            ret["DB_ACTION_OUTPUT"].append(pair_line)

    # If no warning
    if not ret["DB_ACTION_WARNING"]:
        ret.pop("DB_ACTION_WARNING")
    # if warnings["DB_ACTION_WARNING"]:
    #     ret = warnings
    # else:
    #     ret = output
    return json.dumps(ret)

def ingest_genpipes(project_id: str, ingest_data, session=None):
    """Ingesting GenPipes run"""
    if not isinstance(ingest_data, dict):
        ingest_data = json.loads(ingest_data)

    if not session:
        session = database.get_session()

    ret = {
        "DB_ACTION_WARNING": [],
        "DB_ACTION_OUTPUT": []
        }

    project = projects(project_id=project_id, session=session)[0]

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
        operation_config=operation_config
    )
    if warning:
        ret["DB_ACTION_WARNING"].append(warning)

    readset_list = []
    if not ingest_data[vb.SAMPLE]:
        raise RequestError("No 'Sample' found, this json won't be ingested.")
    for sample_json in ingest_data[vb.SAMPLE]:
        sample = session.query(Sample).filter_by(
            deprecated=False,
            deleted=False,
            name=sample_json[vb.SAMPLE_NAME]
        ).first()
        if not sample:
            raise DidNotFindError(f"No Sample named '{sample_json[vb.SAMPLE_NAME]}'")
        for readset_json in sample_json[vb.READSET]:
            readset = session.query(Readset).filter_by(
                deprecated=False,
                deleted=False,
                name=readset_json[vb.READSET_NAME]
            ).first()
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
                                    jobs=[job]
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
                                    jobs=[job]
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
                                readsets=[readset]
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
    operation = session.query(Operation).filter_by(id=operation_id).first()
    # jobs
    ret["DB_ACTION_OUTPUT"].append(operation)
    # If no warning
    if not ret["DB_ACTION_WARNING"]:
        ret.pop("DB_ACTION_WARNING")
    return ret


def digest_unanalyzed(project_id: str, digest_data, session=None):
    """
    Getting unanalyzed samples or readsets
    """
    if not session:
        session = database.get_session()

    session = database.get_session()

    if isinstance(project_id, str):
        project_id = [project_id]

    sample_name_flag = digest_data["sample_name"]
    sample_id_flag = digest_data["sample_id"]
    readset_name_flag = digest_data["readset_name"]
    readset_id_flag = digest_data["readset_id"]
    run_id = digest_data["run_id"]
    run_name = digest_data["run_name"]
    if run_name:
        try:
            run_id = name_to_id("Run", run_name)[0]
        except:
            raise DidNotFindError(f"'Run' with 'name' '{run_name}' doesn't exist on database")
    experiment_nucleic_acid_type = digest_data["experiment_nucleic_acid_type"]
    location_endpoint = digest_data["location_endpoint"]

    if sample_name_flag:
        query = session.query(Sample.name).filter_by(deprecated=False, deleted=False).join(Sample.readsets)
        key = "sample_name"
    elif sample_id_flag:
        query = session.query(Sample.id).filter_by(deprecated=False, deleted=False).join(Sample.readsets)
        key = "sample_id"
    elif readset_name_flag:
        query = session.query(Readset.name).filter_by(deprecated=False, deleted=False)
        key = "readset_name"
    elif readset_id_flag:
        query = session.query(Readset.id).filter_by(deprecated=False, deleted=False)
        key = "readset_id"

    query = query.filter(Readset.state == StateEnum("VALID")).join(Readset.operations).filter(Operation.name.notilike("%genpipes%")).join(Sample.specimen).join(Specimen.project).filter(Project.id.in_(project_id))

    if run_id:
        query = query.filter(Run.id == run_id).join(Readset.run)
    if experiment_nucleic_acid_type:
        query = query.filter(Experiment.nucleic_acid_type == experiment_nucleic_acid_type).join(Readset.experiment)

    output = {
        "location_endpoint": location_endpoint,
        "experiment_nucleic_acid_type": experiment_nucleic_acid_type,
        key: query.all()
    }

    return json.dumps(output)

def digest_delivery(project_id: str, digest_data, session=None):
    """
    Getting delivery samples or readsets
    """
    if not session:
        session = database.get_session()

    location_endpoint = None
    if vb.LOCATION_ENDPOINT in digest_data.keys():
        location_endpoint = digest_data[vb.LOCATION_ENDPOINT]

    specimens = []
    samples = []
    readsets = []
    ret = {
        "DB_ACTION_WARNING": [],
        "DB_ACTION_OUTPUT": {
            "location_endpoint": location_endpoint,
            "experiment_nucleic_acid_type": digest_data[vb.EXPERIMENT_NUCLEIC_ACID_TYPE],
            "operation": [],
            "specimen": []
            }
        }

    if vb.EXPERIMENT_NUCLEIC_ACID_TYPE in digest_data.keys():
        nucleic_acid_type = NucleicAcidTypeEnum(digest_data[vb.EXPERIMENT_NUCLEIC_ACID_TYPE])
    else:
        raise RequestError(argument="experiment_nucleic_acid_type")

    readsets += select_readsets_from_specimens(session, ret, digest_data, nucleic_acid_type)
    readsets += select_readsets_from_samples(session, ret, digest_data, nucleic_acid_type)
    readsets += select_readsets_from_readsets(session, ret, digest_data, nucleic_acid_type)

    operation_config_ids = []
    if readsets:
        for readset in readsets:
            readset_files = []
            readset_metrics = []
            for operation in readset.operations:
                operation_cmd_line = getattr(operation, "cmd_line", None)
                operation_config_data = getattr(operation.operation_config, "data", None)
                operation_config_id = getattr(operation.operation_config, "id", None)
                if operation_config_id not in operation_config_ids:
                    if operation_cmd_line and operation_config_data:
                        operation_json = {
                            "cmd_line": operation_cmd_line,
                            "config_data": operation_config_data.decode("utf-8")
                            }
                        ret["DB_ACTION_OUTPUT"]["operation"].append(operation_json)
                    operation_config_ids.append(operation_config_id)
            for file in readset.files:
                if file.deliverable and file.deleted is False:
                    if location_endpoint:
                        # logger.debug(f"File: {file}")
                        for location in file.locations:
                            # logger.debug(f"Location: {location}")
                            if location_endpoint == location.endpoint:
                                file_deliverable = location.uri.split("://")[-1]
                                if location.deleted is False:
                                    if not file_deliverable:
                                        ret["DB_ACTION_WARNING"].append(f"Looking for 'File' with 'name' '{file.name}' for 'Sample' with 'name' '{readset.sample.name}' and 'Readset' with 'name' '{readset.name}' in '{location_endpoint}', file only exists on {[l.endpoint for l in file.locations]}.")
                                    else:
                                        readset_files.append({
                                            "name": file.name,
                                            "location": file_deliverable
                                            })
            for metric in readset.metrics:
                if metric.deliverable:
                    readset_metrics.append({
                        "name": metric.name,
                        "value": metric.value,
                        "aggregate": metric.aggregate,
                        "flag": metric.flag.to_json()
                        })
            sample = {
                "name": readset.sample.name,
                "tumour": readset.sample.tumour,
                "readset": [{
                    "name": readset.name,
                    "file": readset_files,
                    "metric": readset_metrics
                }]
            }
            specimen = {
                "name": readset.sample.specimen.name,
                "cohort": readset.sample.specimen.cohort,
                "institution": readset.sample.specimen.institution,
                "sample": [sample]
            }
            # Check if specimen already exists
            specimen_exists = False
            for existing_specimen in ret["DB_ACTION_OUTPUT"]["specimen"]:
                if existing_specimen["name"] == specimen["name"]:
                    specimen_exists = True
                    # Check if sample already exists
                    sample_exists = False
                    for existing_sample in existing_specimen["sample"]:
                        if existing_sample["name"] == sample["name"]:
                            sample_exists = True
                            existing_sample["readset"].append(sample["readset"][0])
                            break
                    if not sample_exists:
                        existing_specimen["sample"].append(sample)
                    break

            if not specimen_exists:
                ret["DB_ACTION_OUTPUT"]["specimen"].append(specimen)
            # logger.debug(f"Sample: {readset.sample.name}, Specimen: {readset.sample.specimen.name}")
            # ret["DB_ACTION_OUTPUT"]["patient"].append({
            #     "name": readset.name,
            #     "file": readset_files,
            #     "metric": readset_metrics
            #     })

    # If no warning
    if not ret["DB_ACTION_WARNING"]:
        ret.pop("DB_ACTION_WARNING")

    return json.dumps(ret)

def edit(ingest_data, session=None):
    """Edition of the database based on ingested_data"""
    if not isinstance(ingest_data, dict):
        ingest_data = json.loads(ingest_data)

    if not session:
        session = database.get_session()

    ret = {
        "DB_ACTION_WARNING": [],
        "DB_ACTION_OUTPUT": []
        }

    if not ingest_data[vb.MODIFICATION]:
        raise RequestError("No 'table' provided under 'modification' list, this json is malformed.")

    for table in ingest_data[vb.MODIFICATION]:
        from . import model
        the_table = getattr(model, table[vb.TABLE].title())
        for current_id in set(table[vb.ID]):
            selected_table = session.query(the_table).filter_by(id=current_id).first()
            if not selected_table:
                raise DidNotFindError(table=table[vb.TABLE], attribute="id", query=current_id)
            old = getattr(selected_table, table[vb.COLUMN])
            latest_modification = selected_table.modification
            # Skip the edit if the new value is the same as the old one
            if old == table[vb.NEW]:
                ret["DB_ACTION_WARNING"].append(f"Table '{table[vb.TABLE]}' with id '{current_id}' already has '{table[vb.COLUMN]}' with value '{old}'. Skipping...")
            else:
                setattr(selected_table, table[vb.COLUMN], table[vb.NEW])
                new = getattr(selected_table, table[vb.COLUMN])
                ret["DB_ACTION_OUTPUT"].append(f"Table '{table[vb.TABLE]}' edited: column '{table[vb.COLUMN]}' with id '{current_id}' changes from '{old}' to '{new}' (previous edit done '{latest_modification}').")

    try:
        session.commit()
    except SQLAlchemyError as error:
        logger.error("Error: %s", error)
        session.rollback()

    # If no warning
    if not ret["DB_ACTION_WARNING"]:
        ret.pop("DB_ACTION_WARNING")

    return ret

def delete(ingest_data, session=None):
    """deletion of the database based on ingested_data"""
    if not isinstance(ingest_data, dict):
        ingest_data = json.loads(ingest_data)

    if not session:
        session = database.get_session()

    ret = {
        "DB_ACTION_WARNING": [],
        "DB_ACTION_OUTPUT": []
        }

    if not ingest_data[vb.MODIFICATION]:
        raise RequestError("No 'table' provided under 'modification' list, this json is malformed.")

    for table in ingest_data[vb.MODIFICATION]:
        from . import model
        the_table = getattr(model, table[vb.TABLE].title())
        for current_id in set(table[vb.ID]):
            selected_table = session.query(the_table).filter_by(id=current_id).first()
            if not selected_table:
                raise DidNotFindError(table=table[vb.TABLE], attribute="id", query=current_id)
            if selected_table.deleted is True:
                ret["DB_ACTION_WARNING"].append(f"'{table[vb.TABLE]}' with id '{current_id}' already deleted. Skipping...")
            else:
                selected_table.deleted = True
                ret["DB_ACTION_OUTPUT"].append(f"'{table[vb.TABLE]}' with id '{current_id}' deleted.")

    try:
        session.commit()
    except SQLAlchemyError as error:
        logger.error("Error: %s", error)
        session.rollback()

    # If no warning
    if not ret["DB_ACTION_WARNING"]:
        ret.pop("DB_ACTION_WARNING")

    return ret

def undelete(ingest_data, session=None):
    """revert deletion of the database based on ingested_data"""
    if not isinstance(ingest_data, dict):
        ingest_data = json.loads(ingest_data)

    if not session:
        session = database.get_session()

    ret = {
        "DB_ACTION_WARNING": [],
        "DB_ACTION_OUTPUT": []
        }

    if not ingest_data[vb.MODIFICATION]:
        raise RequestError("No 'table' provided under 'modification' list, this json is malformed.")

    for table in ingest_data[vb.MODIFICATION]:
        from . import model
        the_table = getattr(model, table[vb.TABLE].title())
        for current_id in set(table[vb.ID]):
            selected_table = session.query(the_table).filter_by(id=current_id).first()
            if not selected_table:
                raise DidNotFindError(table=table[vb.TABLE], attribute="id", query=current_id)
            if selected_table.deleted is False:
                ret["DB_ACTION_WARNING"].append(f"'{table[vb.TABLE]}' with id '{current_id}' already undeleted. Skipping...")
            else:
                selected_table.deleted = False
                ret["DB_ACTION_OUTPUT"].append(f"'{table[vb.TABLE]}' with id '{current_id}' undeleted.")

    try:
        session.commit()
    except SQLAlchemyError as error:
        logger.error("Error: %s", error)
        session.rollback()

    # If no warning
    if not ret["DB_ACTION_WARNING"]:
        ret.pop("DB_ACTION_WARNING")

    return ret

def deprecate(ingest_data, session=None):
    """deprecation of the database based on ingested_data"""
    if not isinstance(ingest_data, dict):
        ingest_data = json.loads(ingest_data)

    if not session:
        session = database.get_session()

    ret = {
        "DB_ACTION_WARNING": [],
        "DB_ACTION_OUTPUT": []
        }

    if not ingest_data[vb.MODIFICATION]:
        raise RequestError("No 'table' provided under 'modification' list, this json is malformed.")

    for table in ingest_data[vb.MODIFICATION]:
        from . import model
        the_table = getattr(model, table[vb.TABLE].title())
        for current_id in set(table[vb.ID]):
            selected_table = session.query(the_table).filter_by(id=current_id).first()
            if not selected_table:
                raise DidNotFindError(table=table[vb.TABLE], attribute="id", query=current_id)
            if selected_table.deprecated is True:
                ret["DB_ACTION_WARNING"].append(f"'{table[vb.TABLE]}' with id '{current_id}' already deprecated. Skipping...")
            else:
                selected_table.deprecated = True
                ret["DB_ACTION_OUTPUT"].append(f"'{table[vb.TABLE]}' with id '{current_id}' deprecated.")

    try:
        session.commit()
    except SQLAlchemyError as error:
        logger.error("Error: %s", error)
        session.rollback()

    # If no warning
    if not ret["DB_ACTION_WARNING"]:
        ret.pop("DB_ACTION_WARNING")

    return ret

def undeprecate(ingest_data, session=None):
    """revert deprecation of the database based on ingested_data"""
    if not isinstance(ingest_data, dict):
        ingest_data = json.loads(ingest_data)

    if not session:
        session = database.get_session()

    ret = {
        "DB_ACTION_WARNING": [],
        "DB_ACTION_OUTPUT": []
        }

    if not ingest_data[vb.MODIFICATION]:
        raise RequestError("No 'table' provided under 'modification' list, this json is malformed.")

    for table in ingest_data[vb.MODIFICATION]:
        from . import model
        the_table = getattr(model, table[vb.TABLE].title())
        for current_id in set(table[vb.ID]):
            selected_table = session.query(the_table).filter_by(id=current_id).first()
            if not selected_table:
                raise DidNotFindError(table=table[vb.TABLE], attribute="id", query=current_id)
            if selected_table.deprecated is False:
                ret["DB_ACTION_WARNING"].append(f"'{table[vb.TABLE]}' with id '{current_id}' already undeprecated. Skipping...")
            else:
                selected_table.deprecated = False
                ret["DB_ACTION_OUTPUT"].append(f"'{table[vb.TABLE]}' with id '{current_id}' undeprecated.")

    try:
        session.commit()
    except SQLAlchemyError as error:
        logger.error("Error: %s", error)
        session.rollback()

    # If no warning
    if not ret["DB_ACTION_WARNING"]:
        ret.pop("DB_ACTION_WARNING")

    return ret

def curate(ingest_data, session=None):
    """curate the database based on ingested_data"""
    if not isinstance(ingest_data, dict):
        ingest_data = json.loads(ingest_data)

    if not session:
        session = database.get_session()

    ret = {
        "DB_ACTION_WARNING": [],
        "DB_ACTION_OUTPUT": []
        }

    if not ingest_data[vb.MODIFICATION]:
        raise RequestError("No 'table' provided under 'modification' list, this json is malformed.")

    for table in ingest_data[vb.MODIFICATION]:
        from . import model
        the_table = getattr(model, table[vb.TABLE].title())
        for current_id in set(table[vb.ID]):
            selected_table = session.query(the_table).filter_by(id=current_id).first()
            if not selected_table:
                raise DidNotFindError(table=table[vb.TABLE], attribute="id", query=current_id)
            session.delete(selected_table)
            ret["DB_ACTION_OUTPUT"].append(f"'{table[vb.TABLE]}' with id '{current_id}' deleted.")

    try:
        session.commit()
    except SQLAlchemyError as error:
        logger.error("Error: %s", error)
        session.rollback()

    # If no warning
    if not ret["DB_ACTION_WARNING"]:
        ret.pop("DB_ACTION_WARNING")

    return ret

def get_location(ingest_data, session=None):
    """Get the location ID from its endpoint and file name"""
    if not isinstance(ingest_data, dict):
        ingest_data = json.loads(ingest_data)

    if not session:
        session = database.get_session()

    ret = {
        "DB_ACTION_WARNING": [],
        "DB_ACTION_OUTPUT": []
        }

    file_name = ingest_data[vb.FILE_NAME]
    location_endpoint = ingest_data[vb.LOCATION_ENDPOINT]
    location_id = session.query(Location.id).filter_by(deleted=False, deprecated=False).join(File.locations).filter(File.name == file_name).filter(Location.endpoint == location_endpoint).first()
    if not location_id:
        raise DidNotFindError(f"No 'Location' with 'endpoint' '{location_endpoint}' linked to a 'File' with 'name' '{file_name}'")
    else:
        ret["DB_ACTION_OUTPUT"].append(str(location_id[0]))

    # If no warning
    if not ret["DB_ACTION_WARNING"]:
        ret.pop("DB_ACTION_WARNING")

    return ret
