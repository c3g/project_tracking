"""Test database session management in the project_tracking application."""
from project_tracking.database import get_session


def test_get_session(app):
    """Test that get_session returns the same session within the app context."""
    with app.app_context():
        session = get_session()
        assert session is get_session()
