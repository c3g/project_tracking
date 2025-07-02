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
    ENGINE = None
    SESSION = None
    URI = None


def get_engine(db_uri):

    logging.debug('Connecting to {}'.format(db_uri))

    # in tests the engines can be multiple...
    if Engine.ENGINE is None or Engine.URI != db_uri:
        Engine.ENGINE = create_engine(db_uri, echo=False)
        Engine.URI = db_uri

    return Engine.ENGINE


def get_session(no_app=False, db_uri=None):
    """
    The no app option is a convenience to get a DB session outside of a flask app
    """

    if no_app:
        if Engine.SESSION is None:
            if db_uri is None:
                db_uri = os.getenv("SQLALCHEMY_DATABASE_URI", default="sqlite+pysqlite:///:memory:")
            Engine.SESSION = sessionmaker(
                bind=get_engine(db_uri),
                autoflush=False,
                autocommit=False
                )
        return Engine.SESSION()

    if 'session' not in flask.g:
        if db_uri is None:
            db_uri = flask.current_app.config["SQLALCHEMY_DATABASE_URI"]
        flask.g.session = scoped_session(
            sessionmaker(
                bind=get_engine(db_uri=db_uri),
                autoflush=False,
                autocommit=False
                )
            )
        from .model import Base
        Base.query = flask.g.session.query_property()
    return flask.g.session

@contextmanager
def session_scope(no_app=False, db_uri=None):
    session = get_session(no_app=no_app, db_uri=db_uri)
    try:
        yield session
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()  # Works for both scoped_session and regular session


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

    if Engine.SESSION is not None:
        Engine.SESSION = None
        if no_app:
            return

    session = flask.g.pop('session', None)
    if session is not None:
        session.remove()

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


def init_app(app):
    app.teardown_appcontext(close_db)
    app.cli.add_command(init_db_command)
    app.cli.add_command(version_command)
