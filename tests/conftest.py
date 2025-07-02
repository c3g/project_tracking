"""This module contains fixtures for setting up the testing environment for the project_tracking application.
It includes pre-filled models, application context, database sessions, and JSON data for testing various functionalities."""
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
    """
    Create a pre-filled model for testing.
    This fixture initializes a set of model instances with predefined values
    and returns them as dictionaries for easy access in tests.
    Returns:
        tuple: A tuple containing two dictionaries:
            - entry_dict: A dictionary with string values for easy comparison.
            - model_dict: A dictionary with SQLAlchemy model instances.
    """

    project_name = 'Project Name'
    project = model.Project(name=project_name)
    op_config_version = 1.0
    op_config_name = 'Operation Config Name'
    op_c = model.OperationConfig(name=op_config_name, version=op_config_version)
    op_name = 'Operation Name'
    op = model.Operation(name=op_name,
                         status=model.StatusEnum.COMPLETED,
                         operation_config=op_c,
                         project=project)

    sequencing_technology = 'Sequencing Tech'
    exp = model.Experiment(nucleic_acid_type=model.NucleicAcidTypeEnum.DNA)
    specimen_name = "Specimen Name"
    specimen = model.Specimen(name=specimen_name, project=project)
    sample_name = 'Sample Name'
    sample = model.Sample(name=sample_name, specimen=specimen)
    ru_name = "Run Name"
    instrument = 'Instrument'
    ru = model.Run(instrument=instrument, name=ru_name)
    re1_name = 'Readset 1 Name'
    re2_name = 'Readset 2 Name'
    re1 = model.Readset(name=re1_name, sample=sample, experiment=exp, run=ru)
    re2 = model.Readset(name=re2_name, sample=sample, experiment=exp, run=ru)
    job1 = model.Job(operation=op, status=model.StatusEnum.COMPLETED, readsets=[re1])
    job2 = model.Job(operation=op, status=model.StatusEnum.COMPLETED, readsets=[re2])
    me1_value = 'Metric Value 1'
    me2_value = 'Metric Value 2'
    metric_name = 'Metric Name'
    metric1 = model.Metric(value=me1_value, job=job1, name=metric_name, readsets=[re1])
    metric2 = model.Metric(value=me2_value, job=job2, name=metric_name, readsets=[re2])
    l1_uri = "somewhere://path/to/file1.extension"
    l2_uri = "somewhere://path/to/file2.extension"
    location1 = model.Location(uri=l1_uri)
    location2 = model.Location(uri=l2_uri)
    file1 = model.File(name='file1.extension', locations=[location1])
    file2 = model.File(name='file2.extension', locations=[location2])
    entry_dict = {key: val for key, val in locals().items() if isinstance(val, str) and not key.startswith('_')}
    model_dict = {key: val for key, val in locals().items() if isinstance(val, sqlalchemy.orm.DeclarativeBase)}
    return entry_dict, model_dict


@pytest.fixture
def app():
    """
    Create a Flask application for testing with a temporary database.
    This fixture sets up the application context, initializes the database,
    and yields the application instance for use in tests.
    If the DEBUG environment variable is set, it uses a predefined database path;
    otherwise, it creates a temporary database file.
    """
    if os.getenv('DEBUG'):
        db_path = Path(os.path.join("instance", "test_db.sql"))
        db_path.unlink(missing_ok=True)
        logger.debug("DB is here %s", db_path)
    else:
        db_fd, db_path = tempfile.mkstemp()

    app = create_app({
        'TESTING': True,
        'SECRET_KEY': 'test-secret-key',
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
    """
    Create a database session without an application context.
    This fixture initializes a database session using a temporary SQLite database.
    If the DEBUG environment variable is set, it uses a predefined database path;
    otherwise, it creates a temporary database file.
    It yields the database session for use in tests and ensures proper cleanup after tests.
    """
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
    """
    This fixture initializes the Flask application and returns a test client
    that can be used to make requests to the application during testing.
    Args:
        app (Flask): The Flask application instance.
    Returns:
        FlaskClient: A test client for the Flask application.
    """
    return app.test_client()

@pytest.fixture
def runner(app):
    """
    This fixture initializes the Flask application and returns a test runner
    that can be used to invoke command-line commands during testing.
    Args:
        app (Flask): The Flask application instance.
    Returns:
        FlaskCliRunner: A test runner for the Flask application.
    """
    return app.test_cli_runner()

@pytest.fixture()
def run_processing_json():
    """
    Load the run processing JSON data from a file for testing.
    This fixture reads the JSON data from a file located in the 'data' directory
    relative to the current file's directory and returns it as a Python dictionary.
    Returns:
        dict: The JSON data loaded from the file.
    """
    with open(os.path.join(os.path.dirname(__file__), 'data/run_processing.json'), 'r') as file:
        data = json.load(file)
    return data

@pytest.fixture()
def readset_file_json():
    """
    Load the readset file JSON data from a file for testing.
    This fixture reads the JSON data from a file located in the 'data' directory
    relative to the current file's directory and returns it as a Python dictionary.
    Returns:
        dict: The JSON data loaded from the file.
    """
    with open(os.path.join(os.path.dirname(__file__), 'data/readset_file.json'), 'r') as file:
        data = json.load(file)
    return data

@pytest.fixture()
def transfer_json():
    """
    Load the transfer JSON data from a file for testing.
    This fixture reads the JSON data from a file located in the 'data' directory
    relative to the current file's directory and returns it as a Python dictionary.
    Returns:
        dict: The JSON data loaded from the file.
    """
    with open(os.path.join(os.path.dirname(__file__), 'data/transfer.json'), 'r') as file:
        data = json.load(file)
    return data

@pytest.fixture()
def genpipes_json():
    """
    Load the genpipes JSON data from a file for testing.
    This fixture reads the JSON data from a file located in the 'data' directory
    relative to the current file's directory and returns it as a Python dictionary.
    Returns:
        dict: The JSON data loaded from the file.
    """
    with open(os.path.join(os.path.dirname(__file__), 'data/genpipes_rnaseqlight.json'), 'r') as file:
        data = json.load(file)
    return data
