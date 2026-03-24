"""
Admin API
"""
import logging

from flask import Blueprint, jsonify, request

from .. import db_actions

from ..database import session_scope
from ..schema import serialize
from .decorators import parse_json_input

log = logging.getLogger(__name__)

bp = Blueprint('admin_api', __name__, url_prefix='/admin/')


@bp.route('/')
def admin_root():
    """
    Admin api root
    """
    return 'Welcome to the Admin API!'


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

    return jsonify(result)
