"""Test database session management in the project_tracking application."""
import os
import tempfile
import contextlib

import pytest

from project_tracking import model
from project_tracking.database import get_session, init_db, session_scope, set_project_db_from_name


def test_get_session(app):
    """Test that get_session returns the same session within the app context."""
    with app.app_context():
        session = get_session()
        assert session is get_session()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

@contextlib.contextmanager
def _tmp_db():
    """Context manager that creates a temporary SQLite file and yields its URI."""
    fd, path = tempfile.mkstemp()
    uri = f"sqlite:///{path}"
    try:
        yield uri
    finally:
        os.close(fd)
        os.unlink(path)


def _make_app(default_uri, project_databases):
    from project_tracking import create_app
    return create_app({
        'TESTING': True,
        'SECRET_KEY': 'test-secret-key',
        'SQLALCHEMY_DATABASE_URI': default_uri,
        'PROJECT_DATABASES': project_databases,
    })


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_exact_name_routes_to_correct_database():
    """PROJ-A is mapped by its full exact name → distinct DB from the default."""
    with _tmp_db() as default_uri, _tmp_db() as prja_uri:
        app = _make_app(default_uri, {'PROJ-A': prja_uri})

        with app.app_context():
            init_db(default_uri)
            init_db(prja_uri)

            # Seed PROJ-A DB with one project row.
            with session_scope(no_app=True, db_uri=prja_uri) as s:
                s.add(model.Project(name='PROJ-A'))

            # Simulate convcheck_project resolving "PROJ-A" before any query.
            set_project_db_from_name('PROJ-A')

            session = get_session()
            names = [p.name for p in session.query(model.Project).all()]

        assert names == ['PROJ-A']


def test_two_exact_names_can_route_to_same_database():
    """Two exact names can share one DB without relying on prefixes.

    Both project names resolve to the same DB URI so they see each other's
    rows — exactly the behaviour you want when one DB holds multiple projects.
    """
    with _tmp_db() as default_uri, _tmp_db() as shared_uri:
        app = _make_app(default_uri, {
            'PROJECT ALPHA': shared_uri,
            'Project Beta': shared_uri,
        })

        with app.app_context():
            init_db(default_uri)
            init_db(shared_uri)

            # Seed the shared DB with both projects.
            with session_scope(no_app=True, db_uri=shared_uri) as s:
                s.add(model.Project(name='PROJECT ALPHA'))
                s.add(model.Project(name='PROJECT BETA'))

            # Resolving either exact project name should land on the shared DB.
            for project_name in ('PROJECT ALPHA', 'project beta'):
                # Each request gets a fresh app context (simulate new HTTP request).
                with app.app_context():
                    set_project_db_from_name(project_name)
                    session = get_session()
                    names = sorted(p.name for p in session.query(model.Project).all())

                assert names == ['PROJECT ALPHA', 'PROJECT BETA'], (
                    f"Expected both projects visible when routing via '{project_name}'"
                )


def test_partial_name_does_not_match():
    """A partial name should not route; only exact matches are supported."""
    with _tmp_db() as default_uri, _tmp_db() as mapped_uri, _tmp_db() as other_uri:
        app = _make_app(default_uri, {
            'PROJECT': mapped_uri,
            'PROJECT ALPHA': other_uri,
        })

        with app.app_context():
            init_db(default_uri)
            init_db(mapped_uri)
            init_db(other_uri)

            # Seed each DB with a sentinel row.
            with session_scope(no_app=True, db_uri=default_uri) as s:
                s.add(model.Project(name='in-default-db'))
            with session_scope(no_app=True, db_uri=mapped_uri) as s:
                s.add(model.Project(name='in-mapped-db'))
            with session_scope(no_app=True, db_uri=other_uri) as s:
                s.add(model.Project(name='in-other-db'))

        # Exact match should route correctly.
        with app.app_context():
            set_project_db_from_name('PROJECT')
            names = [p.name for p in get_session().query(model.Project).all()]
        assert names == ['in-mapped-db']

        # Partial name should NOT match PROJECT nor PROJECT ALPHA.
        with app.app_context():
            set_project_db_from_name('PROJECT-XYZ')
            names = [p.name for p in get_session().query(model.Project).all()]
        assert names == ['in-default-db']


def test_unknown_project_falls_back_to_default_database():
    """A project name with no mapping falls through to the app default DB."""
    with _tmp_db() as default_uri, _tmp_db() as other_uri:
        app = _make_app(default_uri, {'OTHER': other_uri})

        with app.app_context():
            init_db(default_uri)

            with session_scope(no_app=True, db_uri=default_uri) as s:
                s.add(model.Project(name='DEFAULT-PROJECT'))

            set_project_db_from_name('UNKNOWN-XYZ')
            names = [p.name for p in get_session().query(model.Project).all()]

        assert names == ['DEFAULT-PROJECT']
