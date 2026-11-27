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
    text,
    )
from sqlalchemy.exc import OperationalError
from sqlalchemy.pool import NullPool

from sqlalchemy.orm import sessionmaker, scoped_session


class Engine:
    ENGINES = {}
    # Engines of the write lock, without a pool: a connection of its own per write (see write_lock)
    LOCK_ENGINES = {}
    NO_APP_SESSION_FACTORIES = {}


# Connection pool of each PostgreSQL database, per gunicorn worker: DB_POOL_SIZE connections kept open, up to
# DB_MAX_OVERFLOW more under load, renewed after DB_POOL_RECYCLE seconds. Every project database has its own pool, so
# the API holds up to workers x databases x (size + overflow) connections (README, Concurrent writes).
POOL_DEFAULTS = {
    "DB_POOL_SIZE": 2,
    "DB_MAX_OVERFLOW": 4,
    "DB_POOL_RECYCLE": 1800,
}


def db_label(db_uri: str) -> str:
    """Return a short human-readable label for a database URI."""
    parsed = urlparse(db_uri)
    if parsed.scheme.startswith("sqlite"):
        return parsed.path.rsplit("/", maxsplit=1)[-1] or "sqlite"
    # Never the netloc or the URI: they can hold the password
    return parsed.path.lstrip("/") or parsed.hostname or "unknown"


def _configured_db_uris(config):
    """The databases: SQLALCHEMY_DATABASE_URI and the PROJECT_DATABASES list, without duplicates."""
    return sorted({config['SQLALCHEMY_DATABASE_URI'], *(config.get('PROJECT_DATABASES') or [])})


def validate_project_databases(config):
    """
    Fail at startup on a PROJECT_DATABASES that isn't a list of URIs (before 4.0.0 it mapped project names to URIs:
    projects are now found in the database that holds their row), or on two databases with the same label: OPA rights
    and create_project's 'database' name a database by its label.
    """
    databases = config.get('PROJECT_DATABASES') or []
    if not isinstance(databases, (list, tuple)) or not all(isinstance(uri, str) for uri in databases):
        raise RuntimeError(
            "PROJECT_DATABASES must be a JSON list of database URIs: since 4.0.0 a project is found in the database "
            "that holds its row, so the configuration no longer names projects (README, Adding a project)."
        )
    labels = {}
    for uri in _configured_db_uris(config):
        labels.setdefault(db_label(uri).lower(), []).append(uri)
    shared = sorted(label for label, uris in labels.items() if len(uris) > 1)
    if shared:
        raise RuntimeError(f"Several databases have the label {shared}: give each database its own name.")


def database_by_label(config, label):
    """The URI of the configured database with this label (any case), or None."""
    return next((uri for uri in _configured_db_uris(config) if db_label(uri).lower() == str(label).lower()), None)


def create_project_database(config, payload):
    """
    The URI of the database create_project writes to: the one its payload's 'database' names (a label), the only one
    when there is one, else None.
    """
    label = (payload or {}).get("database") if isinstance(payload, dict) else None
    if label:
        return database_by_label(config, label)
    databases = _configured_db_uris(config)
    return databases[0] if len(databases) == 1 else None


def databases_holding_project(config, name):
    """
    (URIs of the databases with a project named name, ignoring case; URIs of the databases that couldn't be read).
    One small query per database, on every call: no cache, so OPA always decides on the database that holds the
    project now.
    """
    from sqlalchemy import func, select
    from .model import Project

    found, unreachable = [], []
    for uri in _configured_db_uris(config):
        try:
            with get_engine(uri).connect() as connection:
                stmt = select(Project.id).where(func.lower(Project.name) == name.lower()).limit(1)
                if connection.execute(stmt).first() is not None:
                    found.append(uri)
        except OperationalError as error:
            logging.error("Project lookup: %s not read: %s", db_label(uri), str(getattr(error, 'orig', error)).strip())
            unreachable.append(uri)
    return found, unreachable


def project_database(config, name):
    """
    The URI of the database that holds the project named name (any case). With one database, that database.
    Raises DidNotFindError (404) when no database has it, UniqueConstraintError (409) when several do, and
    DatabaseUnavailableError (503) when it is in none of the databases that could be read.
    """
    from .db_actions.errors import DatabaseUnavailableError, DidNotFindError, UniqueConstraintError

    databases = _configured_db_uris(config)
    if len(databases) == 1:
        return databases[0]
    found, unreachable = databases_holding_project(config, name)
    if len(found) > 1:
        raise UniqueConstraintError(
            f"'Project' '{name}' is in several databases {sorted(db_label(uri) for uri in found)}: an admin has to "
            f"remove all but one."
        )
    if found:
        return found[0]
    if unreachable:
        raise DatabaseUnavailableError(
            f"'Project' '{name}' wasn't found, and databases {sorted(db_label(uri) for uri in unreachable)} couldn't "
            f"be read: try again later."
        )
    raise DidNotFindError(f"'Project' '{name}' doesn't exist.", status_code=404)


def resolve_db_uri(db_uri=None, project_id=None):  # noqa: project_id kept for compat
    """
    The database of a session: db_uri, else the one set_project_db_from_name() chose for this request, else
    SQLALCHEMY_DATABASE_URI. With several databases, a request must have chosen one: the default database is
    another project's database, never a fallback.
    """
    if db_uri is not None:
        return db_uri

    project_db_uri = flask.g.get('project_db_uri')
    if project_db_uri is not None:
        return project_db_uri

    config = flask.current_app.config
    if flask.has_request_context() and len(_configured_db_uris(config)) > 1:
        raise RuntimeError(f"No database chosen for {flask.request.method} {flask.request.path}: pass db_uri or call "
                           f"set_project_db_from_name() first.")
    return config["SQLALCHEMY_DATABASE_URI"]


def set_project_db_from_name(project_name: str) -> None:
    """
    Choose the database of this request: the one that holds the project named project_name (project_database). Must
    be called before opening any session. Looked up once per request (auth and convcheck_project both call it), never
    cached across requests.
    """
    if not project_name:
        return
    if flask.g.get('project_db_uri') is not None and flask.g.get('project_db_name') == project_name:
        return
    flask.g.project_db_uri = project_database(flask.current_app.config, project_name)
    flask.g.project_db_name = project_name


def get_engine(db_uri):
    logging.debug('Connecting to %s', db_label(db_uri))

    if db_uri not in Engine.ENGINES:
        Engine.ENGINES[db_uri] = create_engine(db_uri, echo=False, **_pool_options(db_uri))

    return Engine.ENGINES[db_uri]


def _pool_options(db_uri):
    """Pool settings of a PostgreSQL engine, from the app's DB_POOL_* config (POOL_DEFAULTS outside an app)."""
    if not db_uri.startswith("postgresql"):
        return {}
    config = {**POOL_DEFAULTS, **(flask.current_app.config if flask.has_app_context() else {})}
    return {
        "pool_size": int(config["DB_POOL_SIZE"]),
        "max_overflow": int(config["DB_MAX_OVERFLOW"]),
        "pool_recycle": int(config["DB_POOL_RECYCLE"]),
        # A connection PostgreSQL closed (restart, recycle) is replaced instead of failing a request
        "pool_pre_ping": True,
    }


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

# Key of the write lock. PostgreSQL advisory locks are per database, so one key gives each database its own lock.
# Hold it in psql to keep the API's writes out of a database during manual changes:
#   SELECT pg_advisory_lock(8103206596171625573);  ...  SELECT pg_advisory_unlock(8103206596171625573);
WRITE_LOCK_KEY = 8103206596171625573  # b"pt_write" as a big-endian integer


@contextmanager
def write_lock(db_uri=None):
    """
    Run a write request alone on its database: take the database's write lock (WRITE_LOCK_KEY) on a connection of its
    own, waiting at most WRITE_LOCK_TIMEOUT seconds, else raise DatabaseBusyError (503 with Retry-After). The lock is
    released when the block ends, or by PostgreSQL if the connection dies. Reads never take it. A no-op on SQLite,
    which locks the whole file for a write anyway.
    """
    db_uri = resolve_db_uri(db_uri=db_uri)
    engine = get_engine(db_uri)
    if engine.dialect.name != "postgresql":
        yield
        return
    # Not at the top: db_actions imports this module
    from .db_actions.errors import DatabaseBusyError

    config = flask.current_app.config
    timeout = int(config["WRITE_LOCK_TIMEOUT"])
    # Not from the pool: writes waiting for the lock would hold the connections reads need
    if db_uri not in Engine.LOCK_ENGINES:
        Engine.LOCK_ENGINES[db_uri] = create_engine(db_uri, poolclass=NullPool)
    connection = Engine.LOCK_ENGINES[db_uri].connect()
    try:
        try:
            # SET LOCAL: the timeout ends with this transaction, the lock (session level) stays
            connection.execute(text(f"SET LOCAL lock_timeout = '{timeout}s'"))
            connection.execute(text("SELECT pg_advisory_lock(:key)"), {"key": WRITE_LOCK_KEY})
            connection.commit()
        except OperationalError as error:
            connection.rollback()
            # 55P03 lock_not_available: lock_timeout reached
            if getattr(error.orig, "pgcode", None) == "55P03":
                raise DatabaseBusyError(db_label(db_uri), timeout, int(config["WRITE_LOCK_RETRY_AFTER"])) from error
            raise
        try:
            yield
        finally:
            try:
                connection.execute(text("SELECT pg_advisory_unlock(:key)"), {"key": WRITE_LOCK_KEY})
                connection.commit()
            except Exception:  # pylint: disable=broad-except
                # Drop the connection: closing it releases the lock
                logging.exception("Couldn't release the write lock of %s", db_label(db_uri))
                connection.invalidate()
    finally:
        connection.close()


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


def duplicate_projects(project_names_by_uri):
    """
    Projects whose name (ignoring case) is in several databases: the routes refuse them (409). project_names_by_uri
    maps each reachable database URI to its project names. Returns one message per name.
    """
    where = {}
    for uri, names in project_names_by_uri.items():
        for name in names:
            where.setdefault(name.lower(), []).append(f"'{name}' in '{db_label(uri)}'")
    return [f"Project {', '.join(sorted(places))}." for _, places in sorted(where.items()) if len(places) > 1]


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

    duplicates = duplicate_projects(project_names)
    if duplicates:
        click.echo('Projects in several databases (the routes refuse them):')
        for message in duplicates:
            click.echo(f'  {message}')

    if problems or duplicates:
        raise click.ClickException(
            f'{problems} database(s) not at the latest revision, {len(duplicates)} project(s) in several databases.'
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
