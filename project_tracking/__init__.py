"""
TechDev tracking API
"""
import logging
import os
import re
from collections import defaultdict
from urllib.parse import urlparse, urlunparse

from flask import Flask, request, Response, make_response, json, jsonify, g
from sqlalchemy import text
from sqlalchemy.exc import OperationalError, SQLAlchemyError
from werkzeug.exceptions import HTTPException

from . import db_actions
from .db_actions.errors import error_reference
from . import api
from .api.decorators import TEXT_RESPONSE, openapi_doc
from . import auth
from . import database
from .analysis import DEFAULT_ANALYSIS_OPERATIONS, validate_analysis_operations
from .metrics import DEFAULT_METRIC_VOCABULARY, validate_metric_vocabulary


def _load_prefixed_file_env(app, prefix="C3G"):
    """Load <PREFIX>_*_FILE values directly into app config.

    Example:
        C3G_PROJECT_DATABASES_FILE=/run/secrets/C3G_PROJECT_DATABASES
    will populate:
        C3G_PROJECT_DATABASES=<content of file>

    A *_FILE variable wins over the plain variable of the same name, as in entrypoint.sh: it is always
    set on purpose by the deployment, while the plain one may be an image default (the Containerfile
    sets a SQLite C3G_SQLALCHEMY_DATABASE_URI).
    """
    prefix_value = f"{prefix}_"
    for key, file_path in list(os.environ.items()):
        if not (key.startswith(prefix_value) and key.endswith("_FILE")):
            continue

        target_key = key[:-5]

        # Convert C3G_SQLALCHEMY_DATABASE_URI -> SQLALCHEMY_DATABASE_URI
        config_key = target_key[len(prefix_value):]

        try:
            with open(file_path, "r", encoding="utf-8") as handle:
                raw_value = handle.read().strip()
                try:
                    value = app.json.loads(raw_value)
                except Exception:
                    value = raw_value
                app.config[config_key] = value
        except OSError as exc:
            raise RuntimeError(
                f"Could not read secret file '{file_path}' for env var '{target_key}'."
            ) from exc


# Query-level database failures named in the response; any other driver message stays in the log
_KNOWN_DB_FAILURES = (
    "canceling statement due to statement timeout",
    "canceling statement due to lock timeout",
    "canceling statement due to user request",
    "deadlock detected",
    "could not obtain lock",
)


_CONNECTION_ERROR_MARKERS = (
    "connection to server",
    "could not connect",
    "connection refused",
    "could not translate host name",
    "timeout expired",
    "password authentication failed",
    "no pg_hba.conf entry",
    "server closed the connection",
    "terminating connection",
    "the database system is starting up",
    "the database system is shutting down",
    "too many clients",
    "unable to open database file",
)


def _sanitize_db_uri(db_uri: str) -> str:
    """Mask credentials in a DB URI while keeping host/db/query visible."""
    parsed = urlparse(db_uri)
    if not parsed.scheme or parsed.scheme.startswith("sqlite"):
        return db_uri

    host = parsed.hostname or ''
    if parsed.port is not None:
        host = f"{host}:{parsed.port}"

    if parsed.username is not None and parsed.password is not None:
        netloc = f"***:***@{host}"
    elif parsed.username is not None:
        netloc = f"***@{host}"
    else:
        netloc = host

    return urlunparse((
        parsed.scheme,
        netloc,
        parsed.path,
        parsed.params,
        parsed.query,
        parsed.fragment,
    ))


def _log_database_configuration(app: Flask) -> None:
    """Log a safe summary of configured databases without credentials."""
    # In Flask debug mode, the reloader imports the app twice: once in the
    # parent watcher process and once in the serving child. Only log from the
    # serving child to avoid duplicate startup lines.
    if app.debug and os.environ.get("WERKZEUG_RUN_MAIN") != "true":
        return

    default_db_uri = app.config["SQLALCHEMY_DATABASE_URI"]
    default_db_label = database.db_label(default_db_uri)
    configured_databases = [
        {
            "label": database.db_label(uri),
            "uri": _sanitize_db_uri(uri),
        }
        for uri in database._configured_db_uris(app.config)
    ]
    logging.debug(
        "Database routing configured: default_db=%s default_db_uri=%s configured_databases=%s",
        default_db_label,
        _sanitize_db_uri(default_db_uri),
        configured_databases,
    )


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

    # Werkzeug access lines include their own bracketed timestamp in the message,
    # which duplicates the logger timestamp. Keep only app.access request logs.
    logging.getLogger('werkzeug').setLevel(logging.WARNING)

    app.config.from_mapping(
        INGEST_FOLDER=app.instance_path,
        ANALYSIS_OPERATIONS=DEFAULT_ANALYSIS_OPERATIONS,
        METRIC_VOCABULARY=DEFAULT_METRIC_VOCABULARY,
        SQLALCHEMY_DATABASE_URI=f'sqlite:///{os.path.join(app.instance_path, "tracking_db.sql")}',
        # Seconds a write waits for another write to the same database before a 503 (database.write_lock)
        WRITE_LOCK_TIMEOUT=60,
        WRITE_LOCK_RETRY_AFTER=30,
        **database.POOL_DEFAULTS,
    )
    app.config.from_prefixed_env("C3G")
    _load_prefixed_file_env(app, "C3G")

    if test_config is None:
        app.config.from_pyfile('config.py', silent=True)
    else:
        app.config.from_mapping(test_config)

    # Fail at startup rather than at the first analysis ingest
    validate_analysis_operations(app.config["ANALYSIS_OPERATIONS"])
    validate_metric_vocabulary(app.config["METRIC_VOCABULARY"])
    database.validate_project_databases(app.config)
    _log_database_configuration(app)

    try:
        os.makedirs(app.instance_path)
    except OSError:
        pass

    @app.before_request
    def capture_source_file():
        """
        Extract _source_file from JSON payload if present. pt-cli sends its JSON without a JSON content type: any
        body that parses as JSON counts.
        """
        data = request.get_json(force=True, silent=True) if request.get_data(cache=True) else None
        if data is not None:
            if isinstance(data, dict) and '_source_file' in data:
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
        requested_project = getattr(g, 'requested_project', None)
        resolved_db_uri = getattr(g, 'project_db_uri', None)

        if resolved_db_uri:
            resolved_db_label = database.db_label(resolved_db_uri)
            db_route_source = 'project'
        elif len(database._configured_db_uris(app.config)) > 1:
            # Several databases: a request without a project used none, or each one it named itself
            resolved_db_label, db_route_source = '-', 'none'
        else:
            resolved_db_uri = app.config.get('SQLALCHEMY_DATABASE_URI', 'unknown')
            resolved_db_label = database.db_label(resolved_db_uri)
            db_route_source = 'default'

        if requested_project is None:
            requested_project = '-'

        logger.info(
            "%s %s %s %s %s %s %s %s [source_file=%s project=%s db=%s db_route=%s user=%s client=%s]",
            request.remote_addr,
            request.method,
            request.path,
            request.scheme,
            response.status,
            response.content_length,
            request.referrer,
            request.user_agent,
            source_file,
            requested_project,
            resolved_db_label,
            db_route_source,
            # Who: set by auth (AUTH_MODE oidc) for an accepted token; '-' otherwise
            g.get('user') or '-',
            g.get('client_id') or '-',
        )
        # Why a request failed, as the caller was told: the access lines' readers see it without the response.
        # The details the response leaves out are logged where they happen, under the reference it gives.
        if response.status_code >= 400 and response.is_json:
            errors = (response.get_json(silent=True) or {}).get('DB_ACTION_ERROR') if not response.is_streamed else None
            if errors:
                logger.warning(
                    "%s %s %s %s: %s [project=%s user=%s]",
                    request.method, request.path, response.status_code,
                    "failed" if response.status_code >= 500 else "refused",
                    " | ".join(errors) if isinstance(errors, list) else errors,
                    requested_project, g.get('user') or '-',
                )
        return response

    @app.errorhandler(db_actions.Error)
    def handle_exception(e):
        # Refused requests get a real status (400, or 409 for a duplicate): nothing was saved, as every action
        # commits once, at its end
        return jsonify(e.to_dict()), e.status_code, getattr(e, "headers", None) or {}

    @app.errorhandler(OperationalError)
    def handle_operational_error(e):
        """
        Return clearer API errors for common DB connectivity failures. The driver's message goes to the log only
        (it can hold host, role or SQL): the response names known failures, or gives the log's reference.
        """
        raw_error = str(getattr(e, 'orig', e))
        lowered = raw_error.lower()
        reference = error_reference()
        app.logger.error("Database error %s on %s %s (project %s, database %s): %s", reference, request.method,
                         request.path, getattr(g, 'requested_project', None),
                         database.db_label(getattr(g, 'project_db_uri', None) or app.config.get('SQLALCHEMY_DATABASE_URI', '')),
                         raw_error, exc_info=e)
        requested_project = getattr(g, 'requested_project', None)
        resolved_db_uri = getattr(g, 'project_db_uri', None)
        route_source = 'project' if resolved_db_uri else 'default'
        resolved_db_label = database.db_label(resolved_db_uri) if resolved_db_uri else database.db_label(
            app.config.get('SQLALCHEMY_DATABASE_URI', 'unknown')
        )

        context_msg = None
        if requested_project:
            context_msg = (
                f"Requested project '{requested_project}' resolves to database '{resolved_db_label}' via {route_source}."
            )

        db_match = re.search(r'database\s+"([^"]+)"\s+does\s+not\s+exist', lowered)
        if db_match:
            db_name = db_match.group(1)
            errors = [
                (
                    f"Database '{db_name}' does not exist. Create the database and run migrations/init-db for it, or update PROJECT_DATABASES to a valid URI."
                )
            ]
            if context_msg:
                errors.append(context_msg)
            return jsonify({
                'DB_ACTION_ERROR': errors
            }), 503

        if re.search(r'role\s+"([^"]+)"\s+does\s+not\s+exist', lowered):
            errors = [
                f"The database role does not exist (logged, reference {reference}). Create the PostgreSQL role or "
                "update the DB URI credentials."
            ]
            if context_msg:
                errors.append(context_msg)
            return jsonify({
                'DB_ACTION_ERROR': errors
            }), 503

        # OperationalError also covers query-level failures (statement timeout, lock timeout, ...)
        if e.connection_invalidated or any(marker in lowered for marker in _CONNECTION_ERROR_MARKERS):
            errors = ['Database connection failed. Verify SQLALCHEMY_DATABASE_URI/PROJECT_DATABASES and PostgreSQL availability.']
            status = 503
        else:
            known = next((failure for failure in _KNOWN_DB_FAILURES if failure in lowered), None)
            errors = [f"Database error: {known}" if known else f"Database error (logged, reference {reference})"]
            status = 500
        if context_msg:
            errors.append(context_msg)

        return jsonify({
            'DB_ACTION_ERROR': errors
        }), status

    @app.errorhandler(Exception)
    def handle_unexpected_error(e):
        """
        Any other error is a bug: still a 500, but in the response envelope, so a pt-cli user gets a message
        instead of Flask's HTML page. Only the error's type and a reference go back (its message can hold a URI,
        a path or SQL); the traceback goes to the log under that reference.
        """
        if isinstance(e, HTTPException):
            return e
        reference = error_reference()
        app.logger.exception("Exception %s on %s [%s]", reference, request.path, request.method)
        return jsonify({
            'DB_ACTION_ERROR': [
                f"Internal error on {request.method} {request.path} ({type(e).__name__}, reference {reference}). "
                "This is a bug of the API: report it with the reference and your payload, and check what was saved "
                "before sending it again."
            ]
        }), 500

    @app.route('/')
    @openapi_doc(responses=TEXT_RESPONSE)
    def welcome():
        """
        Welcome message: the API is up
        """
        return 'Welcome to the TechDev tracking API!\n'

    @app.route('/health')
    @openapi_doc(responses={
        "200": {"description": "The API can serve requests", "content": {"application/json": {}}},
        "503": {"description": "The default database or the OPA sidecar can't be reached", "content": {"application/json": {}}},
    })
    def health():
        """
        Whether the API can serve requests: its default database answers and, with AUTH_MODE 'oidc', its OPA sidecar
        too. No detail, so it needs no login (for podman's health check and monitoring).
        """
        try:
            with database.get_engine(app.config['SQLALCHEMY_DATABASE_URI']).connect() as connection:
                connection.execute(text('SELECT 1'))
            healthy = auth.auth_mode(app) == 'none' or auth.opa_reachable(app)
        except SQLAlchemyError:
            healthy = False
        return jsonify({"status": "ok" if healthy else "unavailable"}), 200 if healthy else 503

    for bp in api.blueprints:
        app.register_blueprint(bp)

    from .openapi import openapi_command
    app.cli.add_command(openapi_command)

    @app.route('/help')
    @openapi_doc(responses=TEXT_RESPONSE)
    def help():
        """
        Every route with its documentation, as plain text (used by 'pt-cli help')
        """
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
    # Last: it registers the login routes and exempts the public routes from OPA
    auth.init_app(app)

    return app
