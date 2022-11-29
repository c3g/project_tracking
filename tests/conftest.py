import csv
import json
import os
import tempfile
import logging

import pytest
from project_tracking import create_app, api
from project_tracking.database import get_session, init_db, close_db



@pytest.fixture
def app():
    db_fd, db_path = tempfile.mkstemp()

    app = create_app({
        'TESTING': True,
        'SQLALCHEMY_DATABASE_URI': "sqlite:///{}".format(db_path),
    })

    with app.app_context():
        init_db()

    yield app

    os.close(db_fd)
    os.unlink(db_path)

@pytest.fixture
def not_app_db():
    db_fd, db_path = tempfile.mkstemp()

    db = get_session(no_app=True, db_uri="sqlite:///{}".format(db_path))
    init_db()

    try:
        yield db
    finally:
        close_db(no_app=True)
        os.close(db_fd)
        os.unlink(db_path)


@pytest.fixture
def client(app):
    return app.test_client()


@pytest.fixture
def runner(app):
    return app.test_cli_runner()


@pytest.fixture()
def ingestion_json():
    data = []
    with open(os.path.join(os.path.dirname(__file__), 'data/event.csv'), 'r') as fp:
        csvReader = csv.DictReader(fp)

        for row in csvReader:
            # Assuming a column named 'No' to
            # be the primary key
            data.append(row)

    return json.dumps(data)
