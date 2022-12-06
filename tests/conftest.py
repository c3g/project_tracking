import csv
import json
import os
import tempfile
import logging

from pathlib import Path

import pytest
from project_tracking import create_app, api
from project_tracking.database import get_session, init_db, close_db

logger = logging.getLogger(__name__)

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

    db = get_session(no_app=True, db_uri=f'sqlite:///{db_path}')
    init_db()

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
def ingestion_csv():
    data = []
    with open(os.path.join(os.path.dirname(__file__), 'data/event.csv'), 'r') as fp:
        csvReader = csv.DictReader(fp)

        for row in csvReader:
            # Assuming a column named 'No' to
            # be the primary key
            data.append(row)

    return json.dumps(data)

@pytest.fixture()
def ingestion_json():
    with open(os.path.join(os.path.dirname(__file__), 'data/event.json'), 'r') as file:
        data = json.load(file)
    return data
