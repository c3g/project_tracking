import logging

from flask import Blueprint, jsonify, request, flash, redirect, json, abort

from . import db_action
from . import vocabulary as vc

log = logging.getLogger(__name__)

bp = Blueprint('base_api', __name__, url_prefix='/project')


@bp.route('/')
def projects():
    return [i.as_dict for i in db_action.projects()]



@bp.route('/create/<string:project_name>')
def create(project_name: str):

    return db_action.create_project(project_name=project_name).flat_dict


@bp.route('/<string:project_name>/list_all_sample')
def list_all_sample(project_name: str):
    if project_name not in db_action.projects():
        return abort(404, "Project {} not found".format(project_name))

    pass



@bp.route('/<string:project_name>/ingest_run_processing', methods=['GET', 'POST'])
def ingest_run_processing(project_name: str):

    if project_name not in [p.name for p in db_action.projects()]:
        return abort(404, f"Project {project_name} not found")

    if request.method == 'POST':
        try:
            ingest_data = request.get_json(force=True)
        except:
            flash('Data does not seems to be json')
            return redirect(request.url)

        if project_name != ingest_data[vc.PROJECT_NAME]:
            return abort(400, "project name in POST {} not Valid, {} requires".format(ingest_data[vc.PROJECT_NAME],
                                                                                      project_name))
        return db_action.ingest_run_processing(project_name, ingest_data).flat_dict

