import logging
import os
import datetime

from flask import Flask, request

from . import api
from . import database



def create_app(test_config=None):
    # create and configure the app
    app = Flask(__name__, instance_relative_config=True)


    if app.config['DEBUG']:
        level = logging.DEBUG
    else:
        level = os.getenv('LOG_LEVEL', logging.INFO)


    logging.basicConfig(level=level,
                        format=f'%(asctime)s %(levelname)s %(name)s %(threadName)s : %(message)s')

    app.config.from_mapping(
        INGEST_FOLDER=app.instance_path,
        SECRET_KEY='dev',
        SQLALCHEMY_DATABASE_URI='sqlite:///{}'.format(os.path.join(app.instance_path, "tracking_db.sql")),
         # SQLALCHEMY_DATABASE_URI="postgresql+psycopg2://postgres:toto@localhost/c3g_track?client_encoding=utf8",
    )
    app.config.from_prefixed_env("C3G")

    logging.debug('SQLALCHEMY_DATABASE_URI: {}'.format(app.config['SQLALCHEMY_DATABASE_URI']))

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

    @app.after_request
    def after_request(response):
        """
         Logging after every request.
        """
        logger = logging.getLogger("app.access")
        logger.info(
        "%s [%s] %s %s %s %s %s %s %s",
            request.remote_addr,
            datetime.datetime.utcnow().strftime("%d/%b/%Y:%H:%M:%S.%f")[:-3],
            request.method,
            request.path,
            request.scheme,
            response.status,
            response.content_length,
            request.referrer,
            request.user_agent,
            )
        return response

    @app.route('/')
    def welcome():
        return 'Welcome to the TechDev tracking API!'

    # Loadding the api, look at the api/__init__.py file to see
    # what is being registered
    for bp in api.blueprints:
        app.register_blueprint(bp)

    database.init_app(app)

    return app
