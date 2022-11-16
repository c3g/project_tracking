import os
import tempfile

import pytest
from c3g_project_tracking import create_app, api
from c3g_project_tracking.database import get_session, init_db

with open(os.path.join(os.path.dirname(__file__), 'data/event_file.csv'), 'rb') as f:
    _data_csv = f.read().decode('utf8')



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
def client(app):
    return app.test_client()


@pytest.fixture
def runner(app):
    return app.test_cli_runner()
