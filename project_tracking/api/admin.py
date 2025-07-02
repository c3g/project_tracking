"""
Admin API
"""
import logging

from flask import Blueprint, jsonify

from .. import db_actions

from ..database import session_scope

log = logging.getLogger(__name__)

bp = Blueprint('admin_api', __name__, url_prefix='/admin/')


@bp.route('/')
def admin_root():
    """
    Admin api root
    """
    return 'Welcome to the Admin API!'


@bp.route('/create_project/<string:project_name>', methods=['POST'])
def create_project(project_name: str):
    """
    Create new project
    Project name are capitalized by the platform
    """
    with session_scope() as session:
        result = db_actions.create_project(
            project_name=project_name.upper(),
            session=session
        )
        # Convert the output to flat_dict format
        result["DB_ACTION_OUTPUT"] = [i.flat_dict for i in result["DB_ACTION_OUTPUT"]]

    return jsonify(result)
