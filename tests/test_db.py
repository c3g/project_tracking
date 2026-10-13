"""Test database session management in the project_tracking application."""
import os
import tempfile
import contextlib

import pytest
import sqlalchemy
from sqlalchemy.exc import OperationalError

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


def _make_app(default_uri, project_databases, **extra_config):
    from project_tracking import create_app
    config = {
        'TESTING': True,
        'SECRET_KEY': 'test-secret-key',
        'SQLALCHEMY_DATABASE_URI': default_uri,
        'PROJECT_DATABASES': project_databases,
    }
    config.update(extra_config)
    return create_app(config)


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


def test_cross_project_query_within_same_database_uses_project_name_filter():
    """One anchor project can query multiple projects that share the same DB."""
    with _tmp_db() as default_uri, _tmp_db() as shared_uri:
        app = _make_app(default_uri, {
            'PROJECT ALPHA': shared_uri,
            'PROJECT BETA': shared_uri,
        })

        with app.app_context():
            init_db(default_uri)
            init_db(shared_uri)

            with session_scope(no_app=True, db_uri=shared_uri) as s:
                s.add(model.Project(name='PROJECT ALPHA'))
                s.add(model.Project(name='PROJECT BETA'))

        client = app.test_client()
        response = client.get(
            '/project/PROJECT ALPHA?json={"project_name":"PROJECT ALPHA,PROJECT BETA"}'
        )

        assert response.status_code == 200
        names = sorted(project['name'] for project in response.get_json()['DB_ACTION_OUTPUT'])
        assert names == ['PROJECT ALPHA', 'PROJECT BETA']


def test_cross_db_projects_route_aggregates_results():
    """Admin projects route fans out across all configured databases."""
    with _tmp_db() as default_uri, _tmp_db() as shared_uri:
        app = _make_app(default_uri, {'PROJECT ALPHA': shared_uri})

        with app.app_context():
            init_db(default_uri)
            init_db(shared_uri)

            with session_scope(no_app=True, db_uri=default_uri) as s:
                s.add(model.Project(name='DEFAULT PROJECT'))
            with session_scope(no_app=True, db_uri=shared_uri) as s:
                s.add(model.Project(name='PROJECT ALPHA'))

        client = app.test_client()

        response = client.get('/admin/projects/across-databases')
        assert response.status_code == 200
        payload = response.get_json()['DB_ACTION_OUTPUT']
        names = sorted(project['name'] for project in payload)

        assert names == ['DEFAULT PROJECT', 'PROJECT ALPHA']
        assert all('_database' in project for project in payload)


def test_public_project_route_rejects_numeric_project_identifier():
    """Public /project routes must use project names, not numeric IDs."""
    with _tmp_db() as default_uri:
        app = _make_app(default_uri, {})

        with app.app_context():
            init_db(default_uri)

        client = app.test_client()
        response = client.get('/project/1')

        assert response.status_code == 400
        payload = response.get_json()
        assert 'DB_ACTION_ERROR' in payload
        assert any('numeric' in msg.lower() and '/project' in msg.lower() for msg in payload['DB_ACTION_ERROR'])


def test_admin_project_by_id_route_requires_database_selector_and_returns_scoped_result():
    """Admin /projects/by-id requires explicit db selector and scopes lookup to that DB."""
    with _tmp_db() as default_uri, _tmp_db() as shared_uri:
        app = _make_app(default_uri, {'PROJECT ALPHA': shared_uri})

        with app.app_context():
            init_db(default_uri)
            init_db(shared_uri)

            with session_scope(no_app=True, db_uri=default_uri) as s:
                s.add(model.Project(name='DEFAULT PROJECT'))
            with session_scope(no_app=True, db_uri=shared_uri) as s:
                s.add(model.Project(name='PROJECT ALPHA'))

        client = app.test_client()

        missing_database = client.get('/admin/projects/by-id/1')
        assert missing_database.status_code == 400

        lookup_default = client.get('/admin/projects/by-id/1?database=' + default_uri)
        assert lookup_default.status_code == 200
        default_rows = lookup_default.get_json()['DB_ACTION_OUTPUT']
        assert len(default_rows) == 1
        assert default_rows[0]['name'] == 'DEFAULT PROJECT'

        lookup_shared = client.get('/admin/projects/by-id/1?database=' + shared_uri)
        assert lookup_shared.status_code == 200
        shared_rows = lookup_shared.get_json()['DB_ACTION_OUTPUT']
        assert len(shared_rows) == 1
        assert shared_rows[0]['name'] == 'PROJECT ALPHA'


def test_missing_database_operational_error_returns_user_friendly_message():
    """OperationalError for missing DB returns clear DB_ACTION_ERROR payload."""
    with _tmp_db() as default_uri:
        app = _make_app(default_uri, {})

        @app.route('/_raise_missing_db')
        def _raise_missing_db():
            from flask import g
            g.requested_project = 'CPHI-A'
            g.project_db_uri = 'postgresql+psycopg2://user:pw@localhost/cphi'
            raise OperationalError(
                statement='SELECT 1',
                params={},
                orig=Exception('FATAL:  database "cphi" does not exist')
            )

        client = app.test_client()
        response = client.get('/_raise_missing_db')

        assert response.status_code == 503
        payload = response.get_json()
        assert 'DB_ACTION_ERROR' in payload
        assert any('cphi' in msg.lower() and 'does not exist' in msg.lower() for msg in payload['DB_ACTION_ERROR'])
        assert any('project' in msg.lower() and 'cphi-a' in msg.lower() and 'database' in msg.lower() and 'cphi' in msg.lower() for msg in payload['DB_ACTION_ERROR'])


def _app_raising(orig_message):
    """App whose test route raises an OperationalError wrapping orig_message."""
    tmp = tempfile.NamedTemporaryFile(suffix='.sql', delete=False)
    app = _make_app(f"sqlite:///{tmp.name}", {})

    @app.route('/_raise_operational')
    def _raise_operational():
        from flask import g
        g.requested_project = 'PROJ-A'
        raise OperationalError(statement='SELECT 1', params={}, orig=Exception(orig_message))

    return app, tmp.name


def test_connection_refused_operational_error_returns_503():
    app, path = _app_raising(
        'connection to server at "localhost" (127.0.0.1), port 5432 failed: Connection refused'
    )
    try:
        response = app.test_client().get('/_raise_operational')
        assert response.status_code == 503
        assert 'Database connection failed' in response.get_json()['DB_ACTION_ERROR'][0]
    finally:
        os.unlink(path)


def test_query_level_operational_error_is_not_reported_as_connection_failure():
    """e.g. a PostgreSQL statement timeout must surface its own message, not a connectivity hint."""
    app, path = _app_raising('canceling statement due to statement timeout')
    try:
        response = app.test_client().get('/_raise_operational')
        assert response.status_code == 500
        errors = response.get_json()['DB_ACTION_ERROR']
        assert errors[0] == 'Database error: canceling statement due to statement timeout'
        assert not any('connection failed' in msg.lower() for msg in errors)
        assert any("'PROJ-A'" in msg for msg in errors)
    finally:
        os.unlink(path)


def _alembic_head():
    from alembic.config import Config as AlembicConfig
    from alembic.script import ScriptDirectory
    return ScriptDirectory.from_config(AlembicConfig('alembic.ini')).get_current_head()


def _alembic_revision(db_uri):
    from sqlalchemy import create_engine, text
    engine = create_engine(db_uri)
    try:
        with engine.connect() as connection:
            return connection.execute(text("SELECT version_num FROM alembic_version")).scalar_one_or_none()
    finally:
        engine.dispose()


def test_init_db_stamps_only_databases_it_creates():
    """A fresh (or flushed) DB is stamped at head; an existing DB keeps its revision so upgrades still run."""
    from sqlalchemy import create_engine, text
    with _tmp_db() as db_uri:
        runner = _make_app(db_uri, {}).test_cli_runner()

        result = runner.invoke(args=['init-db', '--db-uri', db_uri])
        assert result.exit_code == 0, result.output
        assert _alembic_revision(db_uri) == _alembic_head()

        # Simulate a database created before the sample_match migration
        engine = create_engine(db_uri)
        with engine.begin() as connection:
            connection.execute(text("DROP TABLE sample_match"))
            connection.execute(text("UPDATE alembic_version SET version_num = '64a560017524'"))
        result = runner.invoke(args=['init-db', '--db-uri', db_uri])
        assert result.exit_code == 0, result.output
        assert 'already has tables: left unchanged' in result.output
        assert _alembic_revision(db_uri) == '64a560017524'
        # The pending migration must still be able to create it
        with engine.connect() as connection:
            assert 'sample_match' not in sqlalchemy.inspect(connection).get_table_names()
        engine.dispose()

        result = runner.invoke(args=['init-db', '--db-uri', db_uri, '--flush'])
        assert result.exit_code == 0, result.output
        assert _alembic_revision(db_uri) == _alembic_head()


def test_db_revisions_reports_each_database_state():
    from sqlalchemy import create_engine, text
    with _tmp_db() as default_uri, _tmp_db() as project_uri:
        runner = _make_app(default_uri, {'PROJECT ALPHA': project_uri}).test_cli_runner()
        assert runner.invoke(args=['init-all-dbs']).exit_code == 0
        default_label = default_uri.rsplit('/', 1)[-1]
        project_label = project_uri.rsplit('/', 1)[-1]

        result = runner.invoke(args=['db-revisions'])
        assert result.exit_code == 0, result.output
        assert result.output.count('(up to date)') == 2

        engine = create_engine(project_uri)
        with engine.begin() as connection:
            connection.execute(text("UPDATE alembic_version SET version_num = '64a560017524'"))
        result = runner.invoke(args=['db-revisions'])
        assert result.exit_code == 1
        assert f'{project_label}: 64a560017524 (BEHIND' in result.output
        assert f'{default_label}: {_alembic_head()} (up to date)' in result.output

        with engine.begin() as connection:
            connection.execute(text("DELETE FROM alembic_version"))
        engine.dispose()
        result = runner.invoke(args=['db-revisions'])
        assert result.exit_code == 1
        assert f'{project_label}: - (NOT STAMPED' in result.output


def test_db_revisions_reports_unreachable_database_without_credentials():
    with _tmp_db() as default_uri:
        missing = "postgresql+psycopg2://someone:s3cret@127.0.0.1:1/project_tracking_missing?connect_timeout=2"
        runner = _make_app(default_uri, {'PROJECT ALPHA': missing}).test_cli_runner()
        runner.invoke(args=['init-db'])
        result = runner.invoke(args=['db-revisions'])
        assert result.exit_code == 1
        assert 'project_tracking_missing: UNREACHABLE' in result.output
        assert 's3cret' not in result.output
