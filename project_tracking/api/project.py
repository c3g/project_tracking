import logging
import functools

from flask import Blueprint, jsonify, request, flash, redirect, json, abort

from .. import db_action
from .. import vocabulary as vc

log = logging.getLogger(__name__)

bp = Blueprint('project', __name__, url_prefix='/project')

def unroll(string):
    """
    string : includes number in the "1,3-7,9" form
    return:  a list if int of the form [1,3,4,5,6,7,9]
    """

    elem = [e for e in string.split(',') if e]
    unroll_list = []
    for e in elem:
        if '-' in e:
            first= int(e.split('-')[0])
            last = int(e.split('-')[-1])
            for i in range(min(first,last), max(first,last) + 1):
                unroll_list.append(int(i))
        else:
            unroll_list.append(int(e))

    return unroll_list

def capitalize(func):
    """
    Capitalize project_name
    """
    @functools.wraps(func)
    def wrap(*args,project_name = None, **kwargs):
        if isinstance(project_name, str):
            project_name = project_name.upper()
            if project_name not in [p.name for p in db_action.projects(project_name)]:
                return abort(404, "Project {} not found".format(project_name))
        return func(*args, project_name=project_name, **kwargs)
    return wrap


@bp.route('/')
@bp.route('/<string:project_name>')
@capitalize
def projects(project_name: str = None):
    """
        return list of all projects the details of the poject with name "project_name"
    """
    if project_name is None:
        return {"Projetc list": [i.name for i in db_action.projects(project_name)]}
    return [i.flat_dict for i in db_action.projects(project_name)]



@bp.route('/<string:project_name>/patients')
@bp.route('/<string:project_name>/patients/<string:patient_id>')
@capitalize
def patients(project_name: str, patient_id: str = None):
    """
    patient_id : uses the form "1,3-8,9"
    return: list all patient or selected patient that are also par of <project>

    Query:
    (pair, tumor):  Default (None, True)
    The tumor query only have an effect if pair is false
        (None, True/False):
            Return: all or selected patients (Default)
        (true, True/False):
            Return: a subset of patient who have Tumor=False & Tumor=True samples
        (false, True):
            return: a subset of patient who only have Tumor=True samples
        (false, True):
            return: a subset of patient who only have Tumor=false samples

    """

    query = request.args
    # valid query
    pair = None
    tumor = True
    if query.get('pair'):
        if query['pair'].lower() in ['true', '1']:
            pair = True
        elif query['pair'].lower() in ['false', '0']:
            pair = False
            if query.get('tumor','').lower() in ['false', '0']:
                tumor=False


    if patient_id is not None:
        patient_id = unroll(patient_id)
    if pair is not None:
        return [i.flat_dict for i in db_action.patient_pair(project_name, patient_id=patient_id,
                                                            pair=pair, tumor=tumor)]
    else:
        return [i.flat_dict for i in db_action.patients(project_name, patient_id=patient_id)]



@bp.route('/<string:project_name>/samples')
@bp.route('/<string:project_name>/samples/<string:sample_id>')
@capitalize
def samples(project_name: str, sample_id: str = None):
    """
    sample_id : uses the form "1,3-8,9", if not provides, all sample are returned
    return: all or selected sample that are in sample_id and part of project
    """
    if sample_id is not None:
        sample_id = unroll(sample_id)

    return [i.flat_dict for i in db_action.samples(project_name, sample_id=sample_id)]

@bp.route('/<string:project_name>/readsets')
@bp.route('/<string:project_name>/readsets/<string:readset_id>')
@capitalize
def readsets(project_name: str, readset_id: str=None):
    """
    readset_id : uses the form "1,3-8,9", if not provided, all readsets are returned
    return: selected readsets that are in sample_id and part of project
    """

    if readset_id is not None:
        readset_id = unroll(readset_id)

    return [i.flat_dict for i in db_action.readsets(project_name, readset_id=readset_id)]


@bp.route('/<string:project_name>/readsets/<string:readset_id>/ingestion/files')
@capitalize
def files(project_name: str, readset_id: str=None):
    """
    readset_id : uses the form "1,3-8,9", if not provided, all readsets are returned
    return: selected readsets that are in sample_id and part of project
    """

    if readset_id is not None:
        readset_id = unroll(readset_id)

    return [i.flat_dict for i in db_action.files(project_name, readset_id=readset_id)]



@bp.route('/<string:project_name>/samples/<string:sample_id>/readsets')
@capitalize
def readsets_from_samples(project_name: str, sample_id: str):
    """
    sample_id : uses the form "1,3-8,9"
    return: readsets for slected sample_id
    """

    sample_id = unroll(sample_id)

    return [i.flat_dict for i in db_action.readsets(project_name, sample_id)]


@bp.route('/<string:project_name>/digest_readset_file', methods=['POST'])
@capitalize
def digest_readset_file(project_name: str):
    """
    POST: list of Readset/Sample Name or id
    return: all information to create a "Genpipes readset file"
    """

    if request.method == 'POST':
        try:
            ingest_data = request.get_json(force=True)
        except:
            flash('Data does not seems to be json')
            return redirect(request.url)

        return db_action.digest_readset_file(project_name=project_name.upper(), digest_data=ingest_data)


@bp.route('/<string:project_name>/ingest_run_processing', methods=['GET', 'POST'])
@capitalize
def ingest_run_processing(project_name: str):
    """
    POST:  json describing run processing
    return: The Operation object
    """

    if request.method == 'GET':
        return abort(405, "Use post methode to ingest runs")

    if request.method == 'POST':
        try:
            ingest_data = request.get_json(force=True)
        except:
            flash('Data does not seems to be json')
            return redirect(request.url)

        if project_name != ingest_data[vc.PROJECT_NAME].upper():
            return abort(400, "project name in POST {} not Valid, {} requires"
                         .format(ingest_data[vc.PROJECT_NAME].upper(), project_name))

        return [i.flat_dict for i in db_action.ingest_run_processing(project_name=project_name.upper(), ingest_data=ingest_data)]


@bp.route('/<string:project_name>/ingest_transfer', methods=['POST'])
@capitalize
def ingest_transfer(project_name: str):
    """
    Add new location to file that has already been moved before
    the db was created
    """
    try:
        ingest_data = request.get_json(force=True)
    except:
        flash('Data does not seems to be json')
        return redirect(request.url)

    return  [i.flat_dict for i in db_action.ingest_transfer(project_name=project_name, ingest_data=ingest_data)]


@bp.route('/<string:project_name>/metrics/<string:metric_id>')
@bp.route('/<string:project_name>/readsets/<string:readset_id>/metrics')
@bp.route('/<string:project_name>/samples/<string:sample_id>/metrics')
@capitalize
def metrics(project_name: str, readset_id: str=None, metric_id: str=None, sample_id: str=None):
    """
    metric_id : uses the form "1,3-8,9". Select metric by ids
    redeaset_id : uses the form "1,3-8,9". Select metric by readset ids
    sample_id : uses the form "1,3-8,9". Select metric by sample ids

    return: selected metrics
    """


    if readset_id is not None:
        readset_id = unroll(readset_id)
    if metric_id is not None:
        metric_id = unroll(metric_id)
    if sample_id is not None:
        sample_id = unroll(sample_id)

    return [i.flat_dict for i in db_action.metrics(project_name=project_name,
                                                   readset_id=readset_id,
                                                   metric_id=metric_id, sample_id=sample_id)]
