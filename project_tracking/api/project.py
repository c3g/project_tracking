import logging
import functools

from flask import Blueprint, jsonify, request, flash, redirect, json, abort

from .. import db_action
from .. import vocabulary as vc

logger = logging.getLogger(__name__)

bp = Blueprint('project', __name__, url_prefix='/project')

def unroll(string):
    """
    string: includes number in the "1,3-7,9" form
    return: a list if int of the form [1,3,4,5,6,7,9]
    """

    elem = [e for e in string.split(',') if e]
    unroll_list = []
    for e in elem:
        if '-' in e:
            first = int(e.split('-')[0])
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
    def wrap(*args, project_name = None, **kwargs):
        if isinstance(project_name, str):
            project_name = project_name.upper()
            if project_name not in [p.name for p in db_action.projects(project_name)]:
                return abort(
                    404,
                    f"Project {project_name} not found"
                    )
        return func(*args, project_name=project_name, **kwargs)
    return wrap


@bp.route('/')
@bp.route('/<string:project_id>')
# @capitalize
def projects(project_id: str = None):
    """
    patient_id: uses the form "/project/1"
    patient_name: uses the form "/project/'?name=<project_name>'"
    return: list of all the details of the poject with name "project_name" or ID "project_id"
    """
    query = request.args
    # valid query
    name = None
    if query.get('name'):
        name = query['name']
    if name:
        project_id = db_action.name_to_id("Project", name)

    if project_id is None:
        return {"Project list": [f"id: {i.id}, name: {i.name}" for i in db_action.projects(project_id)]}
    return [i.flat_dict for i in db_action.projects(project_id)]



@bp.route('/<string:project_id>/patients')
@bp.route('/<string:project_id>/patients/<string:patient_id>')
# @capitalize
def patients(project_id: str, patient_id: str = None):
    """
    patient_id: uses the form "1,3-8,9"
    return: list all patient or selected patient that are also par of <project>

    Query:
    (pair, tumor):  Default (None, true)
    The tumor query only have an effect if pair is false
        (None, true/false):
            Return: all or selected patients (Default)
        (true, true/false):
            Return: a subset of patient who have Tumor=False & Tumor=True samples
        (false, true):
            return: a subset of patient who only have Tumor=True samples
        (false, true):
            return: a subset of patient who only have Tumor=false samples
    """

    query = request.args
    # valid query
    pair = None
    tumor = True
    name = None
    if query.get('pair'):
        if query['pair'].lower() in ['true', '1']:
            pair = True
        elif query['pair'].lower() in ['false', '0']:
            pair = False
            if query.get('tumor','').lower() in ['false', '0']:
                tumor=False

    if patient_id is not None:
        patient_id = unroll(patient_id)

    if query.get('name'):
        name = query['name']
    if name:
        patient_id = []
        for patient_name in name.split(","):
            patient_id.extend(db_action.name_to_id("Patient", patient_name))

    # pair being either True or False
    if pair is not None:
        return [
        i.flat_dict for i in db_action.patient_pair(
            project_id,
            patient_id=patient_id,
            pair=pair,
            tumor=tumor
            )
        ]
    else:
        return [
        i.flat_dict for i in db_action.patients(
            project_id,
            patient_id=patient_id
            )
        ]



@bp.route('/<string:project_id>/samples')
@bp.route('/<string:project_id>/samples/<string:sample_id>')
# @capitalize
def samples(project_id: str, sample_id: str = None):
    """
    sample_id: uses the form "1,3-8,9", if not provides, all sample are returned
    return: all or selected sample that are in sample_id and part of project
    """

    query = request.args
    # valid query
    name = None

    if sample_id is not None:
        sample_id = unroll(sample_id)

    if query.get('name'):
        name = query['name']
    if name:
        sample_id = []
        for sample_name in name.split(","):
            sample_id.extend(db_action.name_to_id("Sample", sample_name))

    return [i.flat_dict for i in db_action.samples(project_id, sample_id=sample_id)]

@bp.route('/<string:project_id>/readsets')
@bp.route('/<string:project_id>/readsets/<string:readset_id>')
# @capitalize
def readsets(project_id: str, readset_id: str=None):
    """
    readset_id: uses the form "1,3-8,9", if not provided, all readsets are returned
    return: selected readsets that are in sample_id and part of project
    """

    query = request.args
    # valid query
    name = None

    if readset_id is not None:
        readset_id = unroll(readset_id)

    if query.get('name'):
        name = query['name']
    if name:
        readset_id = []
        for readset_name in name.split(","):
            readset_id.extend(db_action.name_to_id("Readset", readset_name))

    return [i.flat_dict for i in db_action.readsets(project_id, readset_id=readset_id)]


@bp.route('/<string:project_id>/files/<string:file_id>')
@bp.route('/<string:project_id>/patients/<string:patient_id>/files')
@bp.route('/<string:project_id>/samples/<string:sample_id>/files')
@bp.route('/<string:project_id>/readsets/<string:readset_id>/files')
# @capitalize
def files(project_id: str, patient_id: str=None, sample_id: str=None, readset_id: str=None, file_id: str=None):
    """
    file_id: uses the form "1,3-8,9". Select file by ids
    patient_id: uses the form "1,3-8,9". Select file by patient ids
    sample_id: uses the form "1,3-8,9". Select file by sample ids
    redeaset_id: uses the form "1,3-8,9". Select file by readset ids

    return: selected files

    Query:
    (deliverable):  Default (None)
    The deliverable query allows to get all files labelled as deliverable
        (None):
            return: all or selected metrics (Default)
        (true):
            return: a subset of metrics who have Deliverable=True
        (false):
            return: a subset of metrics who have Deliverable=True
    """

    query = request.args
    # valid query
    deliverable = None
    if query.get('deliverable'):
        if query['deliverable'].lower() in ['true', '1']:
            deliverable = True
        elif query['deliverable'].lower() in ['false', '0']:
            deliverable = False

    if patient_id is not None:
        patient_id = unroll(patient_id)
    elif sample_id is not None:
        sample_id = unroll(sample_id)
    elif readset_id is not None:
        readset_id = unroll(readset_id)
    elif file_id is not None:
        file_id = unroll(file_id)

    if deliverable is not None:
        return [
        i.flat_dict for i in db_action.files_deliverable(
            project_id=project_id,
            patient_id=patient_id,
            sample_id=sample_id,
            readset_id=readset_id,
            file_id=file_id,
            deliverable=deliverable
            )
        ]
    else:
        return [
        i.flat_dict for i in db_action.files(
            project_id=project_id,
            patient_id=patient_id,
            sample_id=sample_id,
            readset_id=readset_id,
            file_id=file_id
            )
        ]


@bp.route('/<string:project_id>/metrics', methods=['POST'])
@bp.route('/<string:project_id>/metrics/<string:metric_id>')
@bp.route('/<string:project_id>/patients/<string:patient_id>/metrics')
@bp.route('/<string:project_id>/samples/<string:sample_id>/metrics')
@bp.route('/<string:project_id>/readsets/<string:readset_id>/metrics')
@capitalize
def metrics(project_id: str, patient_id: str=None, sample_id: str=None, readset_id: str=None, metric_id: str=None):
    """
    metric_id: uses the form "1,3-8,9". Select metric by ids
    patient_id: uses the form "1,3-8,9". Select metric by patient ids
    sample_id: uses the form "1,3-8,9". Select metric by sample ids
    redeaset_id: uses the form "1,3-8,9". Select metric by readset ids

    We also accespt POST data with comma separeted list
    metric_name = <NAME> [,NAME] [...]
    readset_name = <NAME> [,NAME] [...]
    sample_name = <NAME> [,NAME] [...]
    patient_name = <NAME> [,NAME] [...]

    return: selected metrics

    Query:
    (deliverable):  Default (None)
    The deliverable query allows to get all metrics labelled as deliverable
        (None):
            return: all or selected metrics (Default)
        (true):
            return: a subset of metrics who have Deliverable=True
        (false):
            return: a subset of metrics who have Deliverable=True
    """

    query = request.args
    # valid query
    deliverable = None
    if query.get('deliverable'):
        if query['deliverable'].lower() in ['true', '1']:
            deliverable = True
        elif query['deliverable'].lower() in ['false', '0']:
            deliverable = False

    if request.method == 'POST':
        post_data = request.data.decode()
        post_input = post_data.split('=')
        if post_input[0] in ["metric_name", "readset_name", "sample_name", "patient_name"]:
            model_class = post_input[0].split('_')[0]
            names = post_input[1].split(',')
            ids = db_action.name_to_id(model_class.capitalize(), names)
            if post_input[0] == "metric_name":
                metric_id = ids
            elif post_input[0] == "readset_name":
                readset_id = ids
            elif post_input[0] == "sample_name":
                sample_id = ids
            elif post_input[0] == "patient_name":
                patient_id = ids
    elif patient_id is not None:
        patient_id = unroll(patient_id)
    elif sample_id is not None:
        sample_id = unroll(sample_id)
    elif readset_id is not None:
        readset_id = unroll(readset_id)
    elif metric_id is not None:
        metric_id = unroll(metric_id)

    if deliverable is not None:
        return [
        i.flat_dict for i in db_action.metrics_deliverable(
            project_id=project_id,
            patient_id=patient_id,
            sample_id=sample_id,
            readset_id=readset_id,
            metric_id=metric_id,
            deliverable=deliverable
            )
        ]
    else:
        return [
        i.flat_dict for i in db_action.metrics(
            project_id=project_id,
            patient_id=patient_id,
            sample_id=sample_id,
            readset_id=readset_id,
            metric_id=metric_id
            )
        ]

@bp.route('/<string:project_id>/samples/<string:sample_id>/readsets')
# @capitalize
def readsets_from_samples(project_id: str, sample_id: str):
    """
    sample_id: uses the form "1,3-8,9"
    return: readsets for slected sample_id
    """

    query = request.args
    # valid query
    name = None

    sample_id = unroll(sample_id)

    if query.get('name'):
        name = query['name']
    if name:
        sample_id = []
        for sample_name in name.split(","):
            sample_id.extend(db_action.name_to_id("Sample", sample_name))

    return [i.flat_dict for i in db_action.readsets(project_id, sample_id)]


@bp.route('/<string:project_id>/digest_readset_file', methods=['POST'])
# @capitalize
def digest_readset_file(project_id: str):
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

        return db_action.digest_readset_file(project_id=project_id, digest_data=ingest_data)

@bp.route('/<string:project_id>/digest_pair_file', methods=['POST'])
# @capitalize
def digest_pair_file(project_id: str):
    """
    POST: list of Readset/Sample Name or id
    return: all information to create a "Genpipes pair file"
    """

    if request.method == 'POST':
        try:
            ingest_data = request.get_json(force=True)
        except:
            flash('Data does not seems to be json')
            return redirect(request.url)

        return db_action.digest_pair_file(project_id=project_id, digest_data=ingest_data)

@bp.route('/<string:project_id>/ingest_run_processing', methods=['GET', 'POST'])
# @capitalize
def ingest_run_processing(project_id: str):
    """
    POST:  json describing run processing
    return: The Operation object
    """

    if request.method == 'GET':
        return abort(
            405,
            "Use post method to ingest runs"
            )

    if request.method == 'POST':
        try:
            ingest_data = request.get_json(force=True)
        except:
            flash('Data does not seems to be json')
            return redirect(request.url)

        project_id_from_name = db_action.name_to_id("Project", ingest_data[vc.PROJECT_NAME].upper())
        if project_id != project_id_from_name:
            return abort(
                400,
                f"project name in POST {ingest_data[vc.PROJECT_NAME].upper()} not Valid"
                )

        return [i.flat_dict for i in db_action.ingest_run_processing(project_id=project_id, ingest_data=ingest_data)]


@bp.route('/<string:project_id>/ingest_transfer', methods=['POST'])
# @capitalize
def ingest_transfer(project_id: str):
    """
    Add new location to file that has already been moved before
    the db was created
    """
    try:
        ingest_data = request.get_json(force=True)
    except:
        flash('Data does not seems to be json')
        return redirect(request.url)

    return  [i.flat_dict for i in db_action.ingest_transfer(project_id=project_id, ingest_data=ingest_data)]

@bp.route('/<string:project_id>/ingest_genpipes', methods=['GET', 'POST'])
# @capitalize
def ingest_genpipes(project_id: str):
    """
    POST:  json describing genpipes
    return: The Operation object and Jobs associated
    """

    if request.method == 'GET':
        return abort(
            405,
            "Use post method to ingest genpipes analysis"
            )

    if request.method == 'POST':
        try:
            ingest_data = request.get_json(force=True)
        except:
            flash('Data does not seems to be json')
            return redirect(request.url)

        project_id_from_name = db_action.name_to_id("Project", ingest_data[vc.PROJECT_NAME].upper())
        if project_id != project_id_from_name:
            return abort(
                400,
                f"project name in POST {ingest_data[vc.PROJECT_NAME].upper()} not Valid, {project_id} requires"
                )

        output = db_action.ingest_genpipes(project_id=project_id, ingest_data=ingest_data)
        operation = output[0].flat_dict
        jobs = [job.flat_dict for job in output[1]]
        return [operation, jobs]
