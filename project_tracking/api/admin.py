import logging

from flask import Blueprint, jsonify, request, flash, redirect, json, abort

from .. import db_action
from .. import vocabulary as vc

log = logging.getLogger(__name__)

bp = Blueprint('admin_api', __name__, url_prefix='/admin/')


@bp.route('/')
def admin_root():
    """
    Admin api root
    """
    return 'Welcome to the Admin API!'


@bp.route('/create_project/<string:project_name>')
def create_project(project_name: str):
    """
    Create new project
    Project name are capitalized by the platform
    """

    return db_action.create_project(project_name=project_name.upper()).flat_dict
