import logging

from flask import Blueprint, jsonify, request, flash, redirect, json, abort

from .. import db_action
from .. import vocabulary as vc

log = logging.getLogger(__name__)

bp = Blueprint('project', __name__, url_prefix='/project')

def unroll(string):

    elem = string.split(',')
    unroll_list = []
    for e in elem:
        if '-' in e:
            first= e.split('-')[0]
            last = e.split('-')[-1]
            for i in range(int(first), int(last) + 1):
                unroll_list.append(int(i))
        else:
            unroll_list.append(int(e))

    return unroll_list

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
@bp.route('/<string:project_name>/sample/<string:sample_id>')
def sample(project_name: str, sample_id: str = None):
    if project_name not in [p.name for p in db_action.projects(project_name)]:
        return abort(404, "Project {} not found".format(project_name))

    if sample_id is not None:
        sample_id = unroll(sample_id)

    return [i.flat_dict for i in db_action.samples(project_name, sample_id=sample_id)]

@bp.route('/<string:project_name>/readset')
@bp.route('/<string:project_name>/readset/<string:readset_id>')
def readsets(project_name: str, readset_id: str=None):
    if project_name not in [p.name for p in db_action.projects(project_name)]:
        return abort(404, "Project {} not found".format(project_name))

    if readset_id is not None:
        readset_id = unroll(readset_id)

    return [i.flat_dict for i in db_action.readsets(project_name, readset_id=readset_id)]

@bp.route('/<string:project_name>/sample/<string:sample_id>/readset')
def readset_from_sample(project_name: str, sample_id: str):
    if project_name not in [p.name for p in db_action.projects(project_name)]:
        return abort(404, "Project {} not found".format(project_name))

    sample_id = unroll(sample_id)

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

@bp.route('/<string:project_name>/metric/<string:metric_id>')
@bp.route('/<string:project_name>/readset/<string:readset_id>/metric')
@bp.route('/<string:project_name>/sample/<string:sample_id>/metric')
def metrics(project_name: str, readset_id: str=None, metric_id: str=None, sample_id: str=None):
    if project_name not in [p.name for p in db_action.projects(project_name)]:
        return abort(404, "Project {} not found".format(project_name))

    if readset_id is not None:
        readset_id = unroll(readset_id)
    if metric_id is not None:
        metric_id = unroll(metric_id)
    if sample_id is not None:
        sample_id = unroll(sample_id)

    return [i.flat_dict for i in db_action.metrics(
                                                   readset_id=readset_id, metric_id=metric_id, sample_id=sample_id)]

