import logging
import os
import datetime

from flask import Flask, request, Response,make_response

from . import api
from . import database


def create_app(test_config=None):
    # create and configure the app
    app = Flask(__name__, instance_relative_config=True,static_folder=None)
    app.url_map.strict_slashes = False

    if app.config['DEBUG']:
        level = logging.DEBUG
    else:
        level = os.getenv('LOG_LEVEL', logging.INFO)


    logging.basicConfig(level=level,
                        format=f'%(asctime)s %(levelname)s %(name)s %(threadName)s : %(message)s')

    app.config.from_mapping(
        INGEST_FOLDER=app.instance_path,
        SQLALCHEMY_DATABASE_URI='sqlite:///{}'.format(os.path.join(app.instance_path, "tracking_db.sql")),
    )
    # Will overwrite default SQLALCHEMY_DATABASE_URI if C3G_SQLALCHEMY_DATABASE_URI env var is set
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
            datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")[:-3],
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
        """
        Welcome page
        """
        return 'Welcome to the TechDev tracking API!\n'

    # Loadding the api, look at the api/__init__.py file to see
    # what is being registered
    for bp in api.blueprints:
        app.register_blueprint(bp)

    @app.route('/help')
    def help():
        """
        Documentation function
        """
        from collections import defaultdict
        endpoint = defaultdict(lambda: defaultdict(list))

        links = []
        for rule in app.url_map.iter_rules():
            endpoint[rule.endpoint]['rule'].append(rule.rule)
            endpoint[rule.endpoint]['doc'] = app.view_functions[rule.endpoint].__doc__

        for key,value in endpoint.items():
            links.append(
"""----------
URL: 
        {}
DOC: {}
""".format('\n\t'.join(value['rule']), value['doc'])
                         )

        response = make_response('\n'.join(links), 200)
        response.headers["content-type"] = "text/plain"
        return response


    database.init_app(app)

    return app
