import logging

from flask import Blueprint, jsonify, request, flash, redirect, json, abort

from . import db_action

log = logging.getLogger(__name__)

bp = Blueprint('base_api', __name__, url_prefix='/project')


@bp.route('/')
def projects():
    return jsonify(db_action.projects())




@bp.route('/<string:project_name>/list_all_sample')
def list_all_sample(project_name: str):
    if project_name not in db_action.projects():
        return abort(404, "Project {} not found".format(project_name))

    pass



@bp.route('/<string:project_name>/ingest_run_processing', methods=['GET', 'POST'])
def ingest_run_processin(project_name: str):

    if project_name not in [p.name for p in db_action.projects()]:
        return abort(404, "Project {} not found".format(project_name))

    if request.method == 'POST':
        try:
            ingest_data = request.get_json(force=True)
        except:
            flash('Data does not seems to be json')
            return redirect(request.url)

        return db_action.ingset(projet_name,ingest_data)

    return "Load new run, ingest as json with POST"
