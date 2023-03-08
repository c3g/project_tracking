import logging

from flask import Blueprint, jsonify, request, flash, redirect, json, abort

from .. import db_action
from .. import vocabulary as vc

log = logging.getLogger(__name__)

bp = Blueprint('admin_api', __name__, url_prefix='/admin')


@bp.route('/')
def admin_root():
    return 'Welcome to the Admin API!'


@bp.route('/create_project/<string:project_name>')
def create_project(project_name: str):

    return db_action.create_project(project_name=project_name).flat_dict


@bp.route('/fix-db-from-file-system/<string:project_name>', methods=['POST'])
def fix_db_from_fs(project_name: str):

    if request.method == 'POST':
        try:
            ingest_data = request.get_json(force=True)
        except:
            flash('Data does not seems to be json')
            return redirect(request.url)

        if project_name != ingest_data[vc.PROJECT_NAME]:
            return abort(400, "project name in POST {} not Valid, {} requires".format(ingest_data[vc.PROJECT_NAME],
                                                                                      project_name))
        return db_action.fix_db_from_file_system(project_name, ingest_data)
