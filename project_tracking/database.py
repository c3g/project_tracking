"""Module providing database tables and operations support."""
import logging
import os
from contextlib import contextmanager
from urllib.parse import urlparse

import click
import flask
from sqlalchemy import (
    create_engine,
    inspect as sqlalchemy_inspect,
    )

from sqlalchemy.orm import sessionmaker, scoped_session


class Engine:
    ENGINES = {}
    NO_APP_SESSION_FACTORIES = {}


def db_label(db_uri: str) -> str:
    """Return a short human-readable label for a database URI."""
    parsed = urlparse(db_uri)
    if parsed.scheme.startswith("sqlite"):
        return parsed.path.rsplit("/", maxsplit=1)[-1] or db_uri
    return parsed.path.lstrip("/") or parsed.netloc or db_uri


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
    logging.debug('Connecting to %s', db_label(db_uri))

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
    Creates the schema only on an empty (or flushed) database and returns True in that case.
    A database that already has tables is left untouched: create_all would add new tables but not
    new columns, and the matching migration would then fail on the already existing table.
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
    if not flush and set(sqlalchemy_inspect(engine).get_table_names()) - {"alembic_version"}:
        return False
    model.Base.metadata.create_all(engine)
    return True


def stamp_db_head(db_uri):
    """
    Record the latest migration revision on a database whose schema was just created from the models,
    so later 'upgrade-all-dbs' runs don't replay the whole migration history on it.
    """
    from alembic.config import Config as AlembicConfig
    from alembic import command as alembic_command

    alembic_cfg = AlembicConfig('alembic.ini')
    alembic_cfg.attributes['db_uri'] = db_uri
    alembic_command.stamp(alembic_cfg, 'head', purge=True)


def init_and_stamp_db(db_uri, flush=False):
    """
    init_db, then stamp head only if the schema was created from scratch: stamping an existing,
    possibly outdated, database would make alembic skip its pending migrations.
    """
    if init_db(db_uri, flush):
        stamp_db_head(db_uri)
        return True
    click.echo(
        f"  {db_label(db_uri)!r} already has tables: left unchanged, "
        f"run 'upgrade-all-dbs' to apply pending migrations."
    )
    return False


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
    if init_and_stamp_db(db_uri, flush):
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
            click.echo(f'  Flushing and initializing {db_label(uri)!r} ...')
        else:
            click.echo(f'  Initializing {db_label(uri)!r} ...')
        init_and_stamp_db(uri, flush=flush)

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
        click.echo(f'  Upgrading {db_label(uri)!r} → {revision} ...')
        # Pass the URI through config.attributes so env.py picks it up
        # without touching environment variables.
        alembic_cfg.attributes['db_uri'] = uri
        try:
            alembic_command.upgrade(alembic_cfg, revision)
        except Exception as exc:  # noqa: BLE001
            click.echo(f'  ERROR on {db_label(uri)!r}: {exc}', err=True)
            raise

    click.echo(f'All {len(uris)} database(s) upgraded to {revision!r}.')


def project_db_uri(config, project_name):
    """The database URI the routes use for project_name: its PROJECT_DATABASES entry (any case), else the default."""
    mapping = {str(name).upper(): uri for name, uri in config.get('PROJECT_DATABASES', {}).items()}
    return mapping.get(str(project_name).upper(), config['SQLALCHEMY_DATABASE_URI'])


def misplaced_projects(config, project_names_by_uri):
    """
    Projects stored in a database other than the one their name routes to, e.g. left in the default database after
    being given a PROJECT_DATABASES entry: the routes never reach them, and a same-named project in the routed database
    would take their requests. project_names_by_uri maps each reachable database URI to its project names.
    Returns one message per misplaced project.
    """
    problems = []
    for uri, names in project_names_by_uri.items():
        for name in sorted(names):
            routed = project_db_uri(config, name)
            if routed != uri:
                problems.append(f"'Project' '{name}' is in '{db_label(uri)}' but its name routes to '{db_label(routed)}'.")
    return problems


@click.command('db-revisions')
@flask.cli.with_appcontext
def db_revisions_command():
    """Show the alembic revision of every configured database (labels only, no credentials).

    Exits with an error when a database is behind, not stamped or unreachable.
    """
    logging.getLogger('alembic').setLevel(logging.WARNING)
    from alembic.config import Config as AlembicConfig
    from alembic.runtime.migration import MigrationContext
    from alembic.script import ScriptDirectory
    from sqlalchemy import select
    from .model import Project

    head =ScriptDirectory.from_config(AlembicConfig('alembic.ini')).get_current_head()
    click.echo(f'Latest revision (head): {head}')

    problems = 0
    project_names = {}
    for uri in sorted(_configured_db_uris(flask.current_app.config), key=db_label):
        try:
            with get_engine(uri).connect() as connection:
                revision = MigrationContext.configure(connection).get_current_revision()
                if revision is not None:
                    project_names[uri] = connection.execute(select(Project.name)).scalars().all()
        except Exception as exc:  # noqa: BLE001
            first_line = str(getattr(exc, 'orig', exc)).strip().splitlines()[0]
            click.echo(f'  {db_label(uri)}: UNREACHABLE ({first_line})')
            problems += 1
            continue
        if revision == head:
            state = 'up to date'
        elif revision is None:
            state = "NOT STAMPED: run 'alembic stamp <revision>' matching its schema (see README)"
            problems += 1
        else:
            state = "BEHIND: run 'upgrade-all-dbs'"
            problems += 1
        click.echo(f'  {db_label(uri)}: {revision or "-"} ({state})')

    misplaced = misplaced_projects(flask.current_app.config, project_names)
    if misplaced:
        click.echo('Projects outside the database their name routes to:')
        for message in misplaced:
            click.echo(f'  {message}')

    if problems or misplaced:
        raise click.ClickException(
            f'{problems} database(s) not at the latest revision, {len(misplaced)} misplaced project(s).'
        )


def init_app(app):
    app.teardown_appcontext(close_db)
    app.cli.add_command(init_db_command)
    app.cli.add_command(init_all_dbs_command)
    app.cli.add_command(version_command)
    app.cli.add_command(upgrade_all_dbs_command)
    app.cli.add_command(db_revisions_command)
    from .dbml import dbml_command
    app.cli.add_command(dbml_command)
