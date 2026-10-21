"""
Modification API
"""
import logging
from flask import Blueprint, request, jsonify
from werkzeug.exceptions import BadRequest

from ..database import session_scope
from .. import db_actions
from .decorators import openapi_doc

logger = logging.getLogger(__name__)

bp = Blueprint('modification', __name__, url_prefix='/modification')


def modification_body(edit=False):
    """
    OpenAPI request body of the modification routes; edit also takes the column and its new value.
    """
    item = {
        "type": "object",
        "required": ["table", "id"],
        "properties": {
            "table": {"type": "string", "description": "Table or class name, e.g. 'sample' or 'Sample'"},
            "id": {"type": "array", "items": {"type": "integer"}, "description": "Ids of the rows"},
        },
    }
    if edit:
        item["required"] += ["column", "new"]
        item["properties"]["column"] = {"type": "string", "description": "Column to change"}
        item["properties"]["new"] = {"description": "New value: an enum takes its exact value or member name"}
    body = {
        "type": "object",
        "required": ["modification"],
        "properties": {
            "modification": {"type": "array", "items": item},
            "dry_run": {"type": "boolean", "default": False, "description": "Report the changes without saving"},
        },
    }
    example = {"modification": [{"table": "sample", "id": [1]}]}
    if edit:
        example = {"modification": [{"table": "sample_group", "id": [1], "column": "name", "new": "SPECIMEN-1_G2"}]}
    else:
        for mode in ("cascade", "cascade_up", "cascade_down"):
            body["properties"][mode] = {"type": "boolean", "default": False, "description": "Only one cascade mode at a time"}
    return {"required": True, "content": {"application/json": {"schema": body, "example": example}}}

def get_json_data():
    """
    Attempts to parse JSON data from the incoming request.
    
    Returns:
        dict: Parsed JSON data if successful.
        None: If the JSON is invalid or cannot be parsed.
    """
    try:
        data = request.get_json(force=True)
        logger.debug(f"Received JSON data: {data}")
        return data
    except BadRequest as e:
        logger.warning(f"Invalid JSON data: {e}")
        return None

def handle_request(action_func):
    """
    Handles a modification request by parsing JSON input and invoking the given action function.

    Args:
        action_func (function): A function from db_actions to handle the parsed data.
        allow_dry_run (bool): Whether to pass the dry_run flag to the action function.

    Returns:
        Response: JSON response from the action function or an error message if parsing fails.
    """
    ingest_data = get_json_data()
    if ingest_data is None:
        return jsonify({"error": "Invalid JSON data"}), 400

    dry_run = ingest_data.get("dry_run", False)
    cascade_mode = None
    for mode in ["cascade", "cascade_up", "cascade_down"]:
        if ingest_data.get(mode, False):
            if cascade_mode:
                return jsonify({"error": "Only one of 'cascade', 'cascade_up', or 'cascade_down' can be true."}), 400
            cascade_mode = mode

    with session_scope(dry_run=dry_run) as session:
        return action_func(
            ingest_data,
            dry_run=dry_run,
            cascade_mode=cascade_mode,
            session=session
        )

@bp.route('/edit', methods=['POST'])
@openapi_doc(requestBody=modification_body(edit=True))
def edit():
    """
    POST: Change one column of the given rows.
    JSON describing the edit to be made.
    Returns: Result of the edit operation.
    """
    return handle_request(db_actions.edit)

@bp.route('/delete', methods=['POST'])
@openapi_doc(requestBody=modification_body())
def delete():
    """
    POST: Mark the given rows as deleted (soft delete, undone by undelete).
    JSON describing the delete to be made.
    Returns: Result of the delete operation.
    """
    return handle_request(db_actions.delete)

@bp.route('/undelete', methods=['POST'])
@openapi_doc(requestBody=modification_body())
def undelete():
    """
    POST: Undo a delete of the given rows.
    JSON describing the undelete to be made.
    Returns: Result of the undelete operation.
    """
    return handle_request(db_actions.undelete)

@bp.route('/deprecate', methods=['POST'])
@openapi_doc(requestBody=modification_body())
def deprecate():
    """
    POST: Mark the given rows as deprecated (undone by undeprecate).
    JSON describing the deprecate to be made.
    Returns: Result of the deprecate operation.
    """
    return handle_request(db_actions.deprecate)

@bp.route('/undeprecate', methods=['POST'])
@openapi_doc(requestBody=modification_body())
def undeprecate():
    """
    POST: Undo a deprecate of the given rows.
    JSON describing the undeprecate to be made.
    Returns: Result of the undeprecate operation.
    """
    return handle_request(db_actions.undeprecate)

@bp.route('/curate', methods=['POST'])
@openapi_doc(requestBody=modification_body())
def curate():
    """
    POST: Remove the given rows from the database for good (hard delete).
    JSON describing the curate to be made.
    Optional: "dry_run": true to simulate the operation.
    Returns: Result of the curate operation.
    """
    return handle_request(db_actions.curate)
