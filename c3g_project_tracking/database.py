"""Module providing database tables and operations support."""
import click
import logging
import os

import flask
from sqlalchemy import (
    create_engine,
    )

from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.ext.declarative import declarative_base


class Engine:
    ENGINE = None
    SESSION = None

def get_engine(db_uri):

    logging.info('Connecting to {}'.format(db_uri))

    if Engine.ENGINE is None:
        Engine.ENGINE = create_engine(db_uri, echo=False)

    return Engine.ENGINE


def get_session(no_app=False):
    """
    The no app option is a convenience to get a DB session outside of a flask app
    """

    if no_app:
        if Engine.SESSION is None:
            db_uri = os.getenv("SQLALCHEMY_DATABASE_URI", default="sqlite+pysqlite:///:memory:")
            Engine.SESSION = sessionmaker(bind=get_engine(db_uri),
                                          autoflush=False,
                                          autocommit=False)
        return Engine.SESSION

    if 'session' not in flask.g:
        db_uri = flask.current_app.config["SQLALCHEMY_DATABASE_URI"]
        flask.g.session = scoped_session(sessionmaker(bind=get_engine(db_uri=db_uri),
                                                      autoflush=False,
                                                      autocommit=False))
        Base = declarative_base()
        Base.query = flask.g.session.query_property()
    return flask.g.session


def init_db(db_uri=None):
    from . import model
    if db_uri is None:
        db_uri = flask.current_app.config["SQLALCHEMY_DATABASE_URI"]
    engine = get_engine(db_uri)
    model.reg.metadata.create_all(engine)


def close_db(e=None):
    session = flask.g.pop('session', None)
    if session is not None:
        session.remove()

@click.command('init-db')
def init_db_command():
    """Clear the existing data and create new tables."""
    init_db()
    click.echo('Database initialized')


@click.command('add-random-project')
def add_random_project_command():
    import random
    import string
    from . import model
    d = {'description': 'cte projet là, yé ben ben beau'}
    session = get_session()
    # printing lowercase
    letters = string.ascii_lowercase
    s = ''.join(random.choice(letters) for i in range(10))
    p = model.Project(name=s, extra_metadata=d)
    session.add(p)
    session.commit()


def init_app(app):
    app.teardown_appcontext(close_db)
    app.cli.add_command(init_db_command)
    app.cli.add_command(add_random_project_command)
