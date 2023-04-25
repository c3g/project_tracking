import csv
import json
import os
import tempfile
import logging
from pathlib import Path
import pytest

import sqlalchemy

from project_tracking import create_app, model
from project_tracking.database import get_session, init_db, close_db

logger = logging.getLogger(__name__)

@pytest.fixture
def pre_filled_model():

    project_name = 'Conglomerate of Good Health'
    project = model.Project(name=project_name)
    op_config_version = 0.1
    op_config_name = 'generic_index'
    op_c = model.OperationConfig(name=op_config_name, version=op_config_version)
    op_name = 'ingest'
    op = model.Operation(name=op_name,
                         status=model.StatusEnum.DONE,
                         operation_config=op_c,
                         project=project)

    sequencing_technology = 'Fancy Buzzword'
    exp = model.Experiment(sequencing_technology=sequencing_technology)
    pa_name = "P_O"
    pa = model.Patient(name=pa_name, project=project)
    sa_name = 'gros_bobo'
    sa = model.Sample(name=sa_name, patient=pa)
    ru_name = "cure the Conglomerat old director's partner 01"
    instrument = 'Grosse machine du 6e'
    ru = model.Run(instrument=instrument, name=ru_name)
    re1_name = 'goble_goble'
    re2_name = 'goble_dable'
    re1 = model.Readset(name=re1_name, sample=sa, experiment=exp, run=ru)
    re2 = model.Readset(name=re2_name, sample=sa, experiment=exp, run=ru)
    job1 = model.Job(operation=op, status=model.StatusEnum.DONE, readsets=[re1])
    job2 = model.Job(operation=op, status=model.StatusEnum.DONE, readsets=[re2])
    me1_value = 'SHALLOW'
    me2_value = 'PRETTY DEEP'
    metric_name = 'trucmuche'
    metric1 = model.Metric(value=me1_value, job=job1, name=metric_name, readsets=[re1])
    metric2 = model.Metric(value=me2_value, job=job2, name=metric_name, readsets=[re2])
    b1_uri = "beluga://project/rrg-bourqueg/MOH/RAW/data/my.fastq"
    b2_uri = "beluga://project/rrg-bourqueg/MOH/PROCESS/data/your.fastq"
    location1 = model.Location(uri=b1_uri)
    location2 = model.Location(uri=b2_uri)
    file1 = model.File(name='my.fastq', locations=[location1])
    file2 = model.File(name='your.fastq', locations=[location2])
    entry_dict = {key: val for key, val in locals().items() if isinstance(val, str) and not key.startswith('_')}
    model_dict = {key: val for key, val in locals().items() if isinstance(val, sqlalchemy.orm.DeclarativeBase)}
    return entry_dict, model_dict




@pytest.fixture
def app():
    if os.getenv('DEBUG'):
        db_path = Path(os.path.join("instance", "test_db.sql"))
        db_path.unlink(missing_ok=True)
        logger.debug("DB is here %s", db_path)
    else:
        db_fd, db_path = tempfile.mkstemp()

    app = create_app({
        'TESTING': True,
        'SQLALCHEMY_DATABASE_URI': f"sqlite:///{db_path}",
    })

    with app.app_context():
        init_db()

    yield app

    if not os.getenv('DEBUG'):
        os.unlink(db_path)
        os.close(db_fd)

@pytest.fixture
def not_app_db():
    if os.getenv('DEBUG'):
        db_path = Path(os.path.join("instance", "test_db.sql"))
        db_path.unlink(missing_ok=True)
        logger.debug("DB is here %s", db_path)
    else:
        db_fd, db_path = tempfile.mkstemp()
    db_uri = f'sqlite:///{db_path}'
    db = get_session(no_app=True, db_uri=db_uri)
    init_db(db_uri)

    try:
        yield db
    finally:
        close_db(no_app=True)
        if not os.getenv('DEBUG'):
            os.unlink(db_path)
            os.close(db_fd)


@pytest.fixture
def client(app):
    return app.test_client()


@pytest.fixture
def runner(app):
    return app.test_cli_runner()

@pytest.fixture()
def run_processing_json():
    with open(os.path.join(os.path.dirname(__file__), 'data/run_processing.json'), 'r') as file:
        data = json.load(file)
    return data

@pytest.fixture()
def readset_file_json():
    with open(os.path.join(os.path.dirname(__file__), 'data/readset_file.json'), 'r') as file:
        data = json.load(file)
    return data

@pytest.fixture()
def transfer_json():
    with open(os.path.join(os.path.dirname(__file__), 'data/transfer.json'), 'r') as file:
        data = json.load(file)
    return data

@pytest.fixture()
def genpipes_json():
    with open(os.path.join(os.path.dirname(__file__), 'data/genpipes_rnaseqlight.json'), 'r') as file:
        data = json.load(file)
    return data
