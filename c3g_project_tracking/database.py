"""Module providing database tables and operations support."""
import logging
import click

import flask
from sqlalchemy import (
    create_engine,
    )

from sqlalchemy.orm import sessionmaker


def get_engine(db_uri=None):

    if db_uri:
        pass
    elif flask.current_app.config["SQLALCHEMY_DATABASE_URI"]:
        db_uri = flask.current_app.config["SQLALCHEMY_DATABASE_URI"]
    else:
        db_uri = '"sqlite+pysqlite:///:memory:"'

    logging.info('Connecting to {}'.format(db_uri))

    if 'engine' not in flask.g:
        flask.g.engine = create_engine(db_uri, echo=True)
    return flask.g.engine


def get_session():
    if 'session' not in flask.g:
        flask.g.session = sessionmaker()(bind=get_engine(), autoflush=False, autocommit=False)
    return flask.g.session


def init_db():
    from . import model
    engine = get_engine()
    model.mapper_registry.metadata.create_all(engine)


def close_db(e=None):
    engine = flask.g.pop('engine', None)
    if engine is not None:
        engine.dispose()


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
