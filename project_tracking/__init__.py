import logging
import os
import datetime

from flask import Flask, request, Response, make_response, json, jsonify

from . import db_action
from . import api
from . import database

# Define the variable '__version__':
try:
    # If setuptools_scm is installed (e.g. in a development environment with
    # an editable install), then use it to determine the version dynamically.
    from setuptools_scm import get_version

    # This will fail with LookupError if the package is not installed in
    # editable mode or if Git is not installed.
    __version__ = get_version(root="..", relative_to=__file__)
except (ImportError, LookupError):
    # As a fallback, use the version that is hard-coded in the file.
    try:
        from hatch_vcs_footgun_example._version import __version__  # noqa: F401
    except ModuleNotFoundError:
        # The user is probably trying to run this without having installed
        # the package, so complain.
        raise RuntimeError(
            "Hatch VCS Footgun Example is not correctly installed. "
            "Please install it with pip."
        )

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

    @app.errorhandler(db_action.Error)
    def handle_exception(e):
        """"""
        return jsonify(e.to_dict())

    @app.route('/')
    def welcome():
        """
        Welcome page
        """
        return 'Welcome to the TechDev tracking API!\n'

    @app.route('/version')
    def version():
        """
        Version page
        """
        return f'project_tracking version {__version__}\n'

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
