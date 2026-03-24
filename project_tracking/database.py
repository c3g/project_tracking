"""Module providing database tables and operations support."""
import logging
import os
from contextlib import contextmanager

import click
import flask
from sqlalchemy import (
    create_engine,
    )

from sqlalchemy.orm import sessionmaker, scoped_session


class Engine:
    ENGINES = {}
    NO_APP_SESSION_FACTORIES = {}


def _configured_db_uris(config):
    """Collect unique DB URIs from default config and PROJECT_DATABASES mapping."""
    uris = {config['SQLALCHEMY_DATABASE_URI']}
    for uri in config.get('PROJECT_DATABASES', {}).values():
        uris.add(uri)
    return sorted(uris)


def resolve_db_uri(db_uri=None, project_id=None):
    """Resolve DB URI from, in order of priority:
    1. Explicit db_uri argument.
    2. flask.g.project_db_uri set by set_project_db_from_name() for the current request.
    3. Legacy project_id lookup (kept for backwards compat in no-app contexts).
    4. App-level SQLALCHEMY_DATABASE_URI default.
    """
    if db_uri is not None:
        return db_uri

    # Primary: project name was resolved before the first DB access (see convcheck_project).
    project_db_uri = flask.g.get('project_db_uri')
    if project_db_uri is not None:
        return project_db_uri

    # Legacy fallback: avoid breakage if project_id is passed directly.
    if project_id is not None:
        mapping = flask.current_app.config.get('PROJECT_DATABASES', {})
        resolved = mapping.get(str(project_id))
        if resolved is not None:
            return resolved

    return flask.current_app.config["SQLALCHEMY_DATABASE_URI"]


def set_project_db_from_name(project_name: str) -> None:
    """Set the DB URI for this request based on the project *name* from the URL.

    Must be called before opening any session.  Stored in flask.g so every
    session_scope() call in the same request automatically uses it.

    Resolution order:
    1. Exact match in PROJECT_DATABASES (case-insensitive).
    2. Fall through: leave flask.g untouched → app default will be used.

    Example config (instance/config.py or C3G_ env vars)::

        PROJECT_DATABASES = {
            # Exact names only (no prefix matching).
            'PROJ-A':    'postgresql+psycopg2://user:pw@host/prja_db',
            'ANY FREE PROJECT NAME': 'postgresql+psycopg2://user:pw@host/shared_db',
        }
    """
    if not project_name:
        return
    mapping = flask.current_app.config.get('PROJECT_DATABASES', {})
    if not mapping:
        return

    upper = project_name.upper()

    # 1. Exact name match (case-insensitive).
    # Normalize config keys here so operators can keep natural casing in config.
    normalized_mapping = {str(name).upper(): uri for name, uri in mapping.items()}
    resolved = normalized_mapping.get(upper)
    if resolved is not None:
        flask.g.project_db_uri = resolved


def get_engine(db_uri):
    logging.debug('Connecting to {}'.format(db_uri))

    if db_uri not in Engine.ENGINES:
        Engine.ENGINES[db_uri] = create_engine(db_uri, echo=False)

    return Engine.ENGINES[db_uri]


def get_session(no_app=False, db_uri=None, project_id=None):  # noqa: project_id kept for compat
    """
    The no app option is a convenience to get a DB session outside of a flask app
    """

    if no_app:
        if db_uri is None:
            db_uri = os.getenv("SQLALCHEMY_DATABASE_URI", default="sqlite+pysqlite:///:memory:")
        factory = Engine.NO_APP_SESSION_FACTORIES.get(db_uri)
        if factory is None:
            factory = sessionmaker(
                bind=get_engine(db_uri),
                autoflush=False,
                autocommit=False
            )
            Engine.NO_APP_SESSION_FACTORIES[db_uri] = factory
        return factory()

    db_uri = resolve_db_uri(db_uri=db_uri, project_id=project_id)  # project_id compat

    sessions = flask.g.get('sessions')
    if sessions is None:
        sessions = {}
        flask.g.sessions = sessions

    if db_uri not in sessions:
        sessions[db_uri] = scoped_session(
            sessionmaker(
                bind=get_engine(db_uri=db_uri),
                autoflush=False,
                autocommit=False
            )
        )
    flask.g.session = sessions[db_uri]

    if 'base_query_bound' not in flask.g:
        from .model import Base
        Base.query = flask.g.session.query_property()
        flask.g.base_query_bound = True

    return sessions[db_uri]

@contextmanager
def session_scope(no_app=False, db_uri=None, project_id=None, dry_run=False):
    session = get_session(no_app=no_app, db_uri=db_uri, project_id=project_id)
    try:
        yield session
        if not dry_run:
            session.commit()
        else:
            session.rollback()
    except:
        session.rollback()
        raise
    finally:
        session.close()


def init_db(db_uri=None, flush=False):
    """
    db_uri is required if db is initialised outside of the flask app
    """
    from . import model
    if db_uri is None:
        try:
            db_uri = flask.current_app.config["SQLALCHEMY_DATABASE_URI"]
        except RuntimeError as e:
            logging.error(f"It seems that you are initialising the db outside of an app, please provide "
                          f"the db_uri")
            raise e
    engine = get_engine(db_uri)

    if flush:
        model.Base.metadata.drop_all(engine)
    model.Base.metadata.create_all(engine)


def close_db(no_app=False):
    if no_app:
        Engine.NO_APP_SESSION_FACTORIES = {}
        return

    sessions = flask.g.pop('sessions', {})
    for session in sessions.values():
        session.remove()

    flask.g.pop('session', None)
    flask.g.pop('base_query_bound', None)
    flask.g.pop('project_db_uri', None)

@click.command('init-db')
@click.option('--db-uri', default=None)
@click.option('--flush', is_flag=True)
def init_db_command(db_uri=None, flush=False):
    """Create new tables
     WARNING: flush existing data if flush is true
     """
    if db_uri is None:
        db_uri = flask.current_app.config["SQLALCHEMY_DATABASE_URI"]
    init_db(db_uri, flush)
    click.echo('Database initialized')

@click.command('version')
def version_command():
    """Print the version of the API of the database"""
    from . import __version__
    click.echo(f"{__version__.__version__}")


@click.command('init-all-dbs')
@click.option('--flush', is_flag=True,
              help='Drop all tables before creating them on every configured database.')
@click.option('--yes-i-really-mean-flush', is_flag=True,
              help='Required with --flush to acknowledge destructive operation.')
@flask.cli.with_appcontext
def init_all_dbs_command(flush, yes_i_really_mean_flush):
    """Run 'init-db' semantics against every configured project database.

    The set of databases is:
      - SQLALCHEMY_DATABASE_URI  (app default / fallback)
      - all values in PROJECT_DATABASES  (de-duplicated)
    """
    if flush and not yes_i_really_mean_flush:
        raise click.UsageError(
            'Refusing to run --flush across all databases without '
            '--yes-i-really-mean-flush'
        )

    uris = _configured_db_uris(flask.current_app.config)

    for uri in uris:
        if flush:
            click.echo(f'  Flushing and initializing {uri!r} ...')
        else:
            click.echo(f'  Initializing {uri!r} ...')
        init_db(db_uri=uri, flush=flush)

    if flush:
        click.echo(f'All {len(uris)} database(s) flushed and initialized.')
    else:
        click.echo(f'All {len(uris)} database(s) initialized.')


@click.command('upgrade-all-dbs')
@click.option('--revision', default='head', show_default=True,
              help='Alembic revision target applied to every configured database.')
@flask.cli.with_appcontext
def upgrade_all_dbs_command(revision):
    """Run 'alembic upgrade <revision>' against every configured project database.

    All databases always receive the same revision so the schema stays identical
    across projects.  The set of databases is:
      - SQLALCHEMY_DATABASE_URI  (app default / fallback)
      - all values in PROJECT_DATABASES  (de-duplicated)

    Usage::

        flask upgrade-all-dbs                 # upgrade all to head
        flask upgrade-all-dbs --revision abc  # upgrade all to a specific rev
    """
    from alembic.config import Config as AlembicConfig
    from alembic import command as alembic_command

    uris = _configured_db_uris(flask.current_app.config)

    alembic_cfg = AlembicConfig('alembic.ini')

    for uri in uris:
        click.echo(f'  Upgrading {uri!r} → {revision} ...')
        # Pass the URI through config.attributes so env.py picks it up
        # without touching environment variables.
        alembic_cfg.attributes['db_uri'] = uri
        try:
            alembic_command.upgrade(alembic_cfg, revision)
        except Exception as exc:  # noqa: BLE001
            click.echo(f'  ERROR on {uri!r}: {exc}', err=True)
            raise

    click.echo(f'All {len(uris)} database(s) upgraded to {revision!r}.')


def init_app(app):
    app.teardown_appcontext(close_db)
    app.cli.add_command(init_db_command)
    app.cli.add_command(init_all_dbs_command)
    app.cli.add_command(version_command)
    app.cli.add_command(upgrade_all_dbs_command)
