"""
TechDev tracking API
"""
import logging
import os
import datetime
from collections import defaultdict
from datetime import datetime, timezone

from flask import Flask, request, Response, make_response, json, jsonify, g

from . import db_actions
from . import api
from . import database


def create_app(test_config=None):
    app = Flask(__name__, instance_relative_config=True, static_folder=None)
    app.url_map.strict_slashes = False

    if app.config['DEBUG']:
        level = logging.DEBUG
    else:
        level = logging.getLevelName(os.getenv('LOG_LEVEL', 'INFO'))

    logging.basicConfig(
        level=level,
        format='%(asctime)s %(levelname)s %(name)s %(threadName)s: %(message)s'
    )

    app.config.from_mapping(
        INGEST_FOLDER=app.instance_path,
        SQLALCHEMY_DATABASE_URI=f'sqlite:///{os.path.join(app.instance_path, "tracking_db.sql")}',
    )
    app.config.from_prefixed_env("C3G")

    logging.debug(f'SQLALCHEMY_DATABASE_URI: {app.config["SQLALCHEMY_DATABASE_URI"]}')

    if test_config is None:
        app.config.from_pyfile('config.py', silent=True)
    else:
        app.config.from_mapping(test_config)

    try:
        os.makedirs(app.instance_path)
    except OSError:
        pass

    @app.before_request
    def capture_source_file():
        """
        Extract _source_file from JSON payload if present.
        """
        if request.is_json:
            data = request.get_json(silent=True)
            if data and '_source_file' in data:
                g.source_file = data['_source_file']
            else:
                g.source_file = 'unknown_file'
        else:
            g.source_file = 'no_json'

    @app.after_request
    def after_request(response):
        """
        Logging after every request.
        """
        logger = logging.getLogger("app.access")
        source_file = getattr(g, 'source_file', 'no_source_file')

        logger.info(
            "%s [%s] %s %s %s %s %s %s %s [source_file=%s]",
            request.remote_addr,
            datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")[:-3],
            request.method,
            request.path,
            request.scheme,
            response.status,
            response.content_length,
            request.referrer,
            request.user_agent,
            source_file
        )
        return response

    @app.errorhandler(db_actions.Error)
    def handle_exception(e):
        return jsonify(e.to_dict())

    @app.route('/')
    def welcome():
        return 'Welcome to the TechDev tracking API!\n'

    for bp in api.blueprints:
        app.register_blueprint(bp)

    @app.route('/help')
    def help():
        endpoint = defaultdict(lambda: defaultdict(list))
        links = []
        for rule in app.url_map.iter_rules():
            endpoint[rule.endpoint]['rule'].append(rule.rule)
            endpoint[rule.endpoint]['doc'] = app.view_functions[rule.endpoint].__doc__

        for _, value in endpoint.items():
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
