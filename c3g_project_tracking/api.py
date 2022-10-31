from flask import Blueprint
import logging

log = logging.getLogger(__name__)

bp = Blueprint('base_api', __name__, url_prefix='/parent')


@bp.route('/<truite>')
def root(truite):

    log.info(truite)
    return 'The C3G Assembly tracking API'


@bp.route('/projects')
def root():

    log.info(truite)
    return 'The C3G Assembly tracking API'


@bp.route('/list_all_sample')
def root():
    database.list_all_sample
    log.info(truite)
    return 'The C3G Assembly tracking API'



@bp.route('/ingest_run_processin')
def root():
    log.info(truite)
    return 'The C3G Assembly tracking API'
