import logging
import os

from flask import Flask

from . import api
from . import database



def create_app(test_config=None):
    # create and configure the app
    app = Flask(__name__, instance_relative_config=True)

    logging.basicConfig(level=logging.DEBUG,
                        format=f'%(asctime)s %(levelname)s %(name)s %(threadName)s : %(message)s')

    app.config.from_mapping(
        INGEST_FOLDER=app.instance_path,
        SECRET_KEY='dev',
        SQLALCHEMY_DATABASE_URI='sqlite:///{}'.format(os.path.join(app.instance_path, "tracking_db.sql")),
         # SQLALCHEMY_DATABASE_URI="postgresql+psycopg2://postgres:toto@localhost/c3g_track?client_encoding=utf8",
    )

    if test_config is None:
        # load the instance config, if it exists, when not testing
        app.config.from_pyfile('config.py', silent=True)
    else:
        # load the test config if passed in
        app.config.from_mapping(test_config)

    # ensure the instance folder exists
    try:
        os.makedirs(app.instance_path)
    except OSError:
        pass

    @app.route('/')
    def welcome():
        return 'Welcome to the TechDev tracking API!'

    app.register_blueprint(api.bp)
    database.init_app(app)

    return app
