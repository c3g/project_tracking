"""
Admin API
"""
import logging
from typing import Any, cast

from flask import Blueprint, current_app, g, jsonify, request

from .. import db_actions

from ..database import _configured_db_uris, db_label, session_scope, set_project_db_from_name
from ..schema import serialize
from .decorators import TEXT_RESPONSE, openapi_doc, parse_json_input

log = logging.getLogger(__name__)

bp = Blueprint('admin_api', __name__, url_prefix='/admin/')


def _resolve_db_uri_from_selector(selector: str) -> tuple[str | None, str | None]:
    """Resolve a DB URI from a caller-provided selector.

    Selector can be either:
    - an exact configured DB URI
    - a DB label returned by db_label()
    """
    configured = _configured_db_uris(current_app.config)
    matches = []

    for uri in configured:
        if selector == uri or selector.lower() == db_label(uri).lower():
            matches.append(uri)

    if not matches:
        return None, f"Unknown database selector '{selector}'. Use a configured DB label or URI."
    if len(matches) > 1:
        labels = sorted({db_label(uri) for uri in matches})
        return None, (
            f"Ambiguous database selector '{selector}'. Matches multiple configured databases with labels {labels}; pass the exact DB URI."
        )

    return matches[0], None


@bp.route('/')
@openapi_doc(responses=TEXT_RESPONSE)
def admin_root():
    """
    Admin api root
    """
    return 'Welcome to the Admin API!'


@bp.route('/projects/across-databases', methods=['GET'])
def list_projects_across_databases():
    """List projects across all configured databases.

    The response includes a `_database` field on each serialized project so
    callers can tell which configured database produced the row.
    """
    output = []

    for db_uri in _configured_db_uris(current_app.config):
        with session_scope(db_uri=db_uri, dry_run=True) as session:
            result = db_actions.projects(project_id=None, session=session)
            serialized = serialize(
                result.get("DB_ACTION_OUTPUT", []),
                include_relationships=False,
                context={"session": session}
            )
            if not serialized:
                continue
            rows = cast(list[dict[str, Any]], serialized if isinstance(serialized, list) else [serialized])
            label = db_label(db_uri)
            for row in rows:
                row["_database"] = label
                output.append(row)

    return jsonify({"DB_ACTION_OUTPUT": output})


@bp.route('/projects/by-id/<int:project_id>', methods=['GET'])
@openapi_doc(parameters=[{
    "name": "database", "in": "query", "required": True, "schema": {"type": "string"},
    "description": "Database to look in: its label or full configured URI",
}])
def project_by_id(project_id: int):
    """Admin-only explicit project lookup by numeric ID within a selected database.

    Query params:
        database: required DB selector (DB label or full configured DB URI)
    """
    database_selector = request.args.get('database', '').strip()
    if not database_selector:
        return jsonify({
            "DB_ACTION_ERROR": [
                "Missing required query parameter 'database'. Use a DB label or full configured DB URI."
            ]
        }), 400

    db_uri, resolve_error = _resolve_db_uri_from_selector(database_selector)
    if resolve_error is not None or db_uri is None:
        return jsonify({"DB_ACTION_ERROR": [resolve_error or "Could not resolve database selector."]}), 400

    with session_scope(db_uri=db_uri, dry_run=True) as session:
        result = db_actions.projects(project_id=str(project_id), session=session)
        serialized = serialize(
            result.get("DB_ACTION_OUTPUT", []),
            include_relationships=False,
            context={"session": session}
        )

    rows = cast(list[dict[str, Any]], serialized if isinstance(serialized, list) else ([serialized] if serialized else []))
    label = db_label(db_uri)
    for row in rows:
        row["_database"] = label

    result["DB_ACTION_OUTPUT"] = rows
    return jsonify(result)


@bp.route('/create_project/<string:project_name>', methods=['POST'])
@parse_json_input(expected_keys={"ext_id", "ext_src"})
def create_project(project_name: str, ingest_data=None):
    """
    Create or fetch a project by name.

    Path parameter:
        project_name (str): Project name to create. If it already exists,
            the existing row is returned.

    Optional metadata (preferred source is JSON body):
        ext_id (int): External identifier for the project.
        ext_src (str): External source/system label for ext_id.

    Request body (application/json):
        {
            "ext_id": 123,
            "ext_src": "lims"
        }

    Returns:
        200: JSON payload with DB_ACTION_OUTPUT containing one serialized
             project object.
        400: JSON payload with DB_ACTION_ERROR if ext_id is not an integer.

    Notes:
        - project_name is stored exactly as provided in the route.
        - Optional metadata must be sent in the JSON body.
        - The project is created in its PROJECT_DATABASES database, where the /project/<project_name>/ routes look
          for it; a name without an entry goes to the default database, with a warning.
    """
    payload = ingest_data or {}

    ext_id_raw = payload.get("ext_id")
    ext_src = payload.get("ext_src")

    ext_id = None
    if ext_id_raw not in (None, ""):
        try:
            ext_id = int(ext_id_raw)
        except (TypeError, ValueError):
            return jsonify({"DB_ACTION_ERROR": ["ext_id must be an integer when provided."]}), 400

    # Same database as the /project/<project_name>/ routes (convcheck_project)
    g.requested_project = project_name
    set_project_db_from_name(project_name)
    unmapped = current_app.config.get("PROJECT_DATABASES") and not g.get("project_db_uri")

    with session_scope() as session:
        result = db_actions.create_project(
            project_name=project_name,
            session=session,
            ext_id=ext_id,
            ext_src=ext_src,
        )

        output = result.get("DB_ACTION_OUTPUT", [])
        if output:
            serialized = serialize(
                output,
                include_relationships=False,
                context={"session": session}
            )
            result["DB_ACTION_OUTPUT"] = serialized if isinstance(serialized, list) else [serialized]
        else:
            result["DB_ACTION_OUTPUT"] = []

    if unmapped:
        result.setdefault("DB_ACTION_WARNING", []).append(
            f"'Project' '{project_name}' has no PROJECT_DATABASES entry: it is in the default database "
            f"'{db_label(current_app.config['SQLALCHEMY_DATABASE_URI'])}'."
        )
    return jsonify(result)
