import logging

from flask import Blueprint, jsonify, request, flash, redirect, json

from . import db_action

log = logging.getLogger(__name__)

bp = Blueprint('base_api', __name__, url_prefix='/')


@bp.route('/projects')
def projects():
    return jsonify(db_action.projects())


@bp.route('/list_all_sample')
def list_all_sample():
    pass


@bp.route('/ingest_run_processing', methods=['GET', 'POST'])
def ingest_run_processin():
    if request.method == 'POST':
        try:
            ingest_data = request.get_json(force=True)
        except:
            flash('Data does not seems to be json')
            return redirect(request.url)
        return ingest_data

    return "Load new run, ingest as json with POST"
