import logging

from flask import Blueprint, jsonify, request, flash, redirect, json, abort

from .. import db_action
from .. import vocabulary as vc

log = logging.getLogger(__name__)

bp = Blueprint('project', __name__, url_prefix='/project')


# @bp.route('/')
# def all_projects():
#     return [i.flat_dict for i in db_action.projects()]

def project_decor(func):
    def wrapper_func(*args, **kwargs):

        func(*args, **kwargs)
        # Do something after the function.
    return wrapper_func


@bp.route('/')
@bp.route('/<string:project_name>')
def project(project_name: str = None):
    return [i.flat_dict for i in db_action.projects(project_name)]


@bp.route('/<string:project_name>/sample')
def list_all_sample(project_name: str):
    if project_name not in [p.name for p in db_action.projects(project_name)]:
        return abort(404, "Project {} not found".format(project_name))

    return [i.flat_dict for i in db_action.samples(project_name)]

@bp.route('/<string:project_name>/sample/<string:sample_list>')
def sample(sample_list: str):
    if project_name not in db_action.projects():
        return abort(404, "Project {} not found".format(project_name))

    pass

@bp.route('/<string:project_name>/readset')
def list_all_readset(project_name: str):
    if project_name not in [p.name for p in db_action.projects(project_name)]:
        return abort(404, "Project {} not found".format(project_name))

    return [i.flat_dict for i in db_action.readsets(project_name)]

@bp.route('/<string:project_name>/sample/<string:sample_id>/readset')
def readset_from_sample(project_name: str, sample_id: str):
    if project_name not in [p.name for p in db_action.projects(project_name)]:
        return abort(404, "Project {} not found".format(project_name))

    sample_id = [int(i) for i in sample_id.split(',')]

    return [i.flat_dict for i in db_action.readsets(project_name, sample_id)]



@bp.route('/<string:project_name>/ingest_run_processing', methods=['GET', 'POST'])
def ingest_run_processing(project_name: str):

    if request.method == 'GET':
        return abort(405, "Use post methode to ingest runs")

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

