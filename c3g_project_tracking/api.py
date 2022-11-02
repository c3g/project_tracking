import logging

from flask import Blueprint, jsonify

from . import database
from . import db_action

log = logging.getLogger(__name__)

bp = Blueprint('base_api', __name__, url_prefix='/')


@bp.route('/project')
def project():
    return jsonify(db_action.projects())


@bp.route('get/<truite>')
def test(truite):
    log.info(truite)
    return 'The C3G Assembly tracking API'




@bp.route('/list_all_sample')
def list_all_sample():
    pass


@bp.route('/ingest_run_processin')
def ingest_run_processin():
    log.info()
    return 'The C3G Assembly tracking API'
