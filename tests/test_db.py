import pytest
import sqlalchemy

from project_tracking.database import get_session


def test_get_session(app):
    with app.app_context():
        session = get_session()
        assert session is get_session()