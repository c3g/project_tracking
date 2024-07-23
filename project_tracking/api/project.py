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

def convcheck_project(func):
    """
    Converting project name to project id and checking if project found
    """
    @functools.wraps(func)
    def wrap(*args, project=None, **kwargs):
        if project is None:
            project_id = None
        elif project.isdigit():
            project_id = project
            if not db_action.projects(project_id):
                all_available = [f"id: {project.id}, name: {project.name}" for project in db_action.projects()]
                project_id = {"DB_ACTION_WARNING": f"Requested Project '{project}' doesn't exist. Please try again with one of the following: {all_available}"}
        else:
            project_id = db_action.name_to_id("Project", project.upper())
            if not project_id:
                all_available = [f"id: {project.id}, name: {project.name}" for project in db_action.projects()]
                project_id = {"DB_ACTION_WARNING": f"Requested Project '{project}' doesn't exist. Please try again with one of the following: {all_available}"}
            else:
                # Converting list of 1 project to string
                project_id = "".join(map(str, project_id))

        return func(*args, project_id=project_id, **kwargs)
    return wrap

def sanity_check(item, action_output):
    if not action_output:
        ret = {"DB_ACTION_WARNING": f"Requested {item} doesn't exist."}
    else:
        ret = [i.flat_dict for i in action_output]
    return ret


@bp.route('/')
@bp.route('/<string:project>')
@convcheck_project
def projects(project_id: str = None):
    """
    GET:
        project: uses the form "/project/1" for project ID and "/project/name" for project name
    return: list of all the details of the poject with name "project_name" or ID "project_id"
    """

    if project_id is None:
        return {"Project list": [f"id: {project.id}, name: {project.name}" for project in db_action.projects(project_id)]}
    if isinstance(project_id, dict) and project_id.get("DB_ACTION_WARNING"):
        return project_id

    return [i.flat_dict for i in db_action.projects(project_id)]


@bp.route('/<string:project>/specimens')
@bp.route('/<string:project>/specimens/<string:specimen_id>')
@convcheck_project
def specimens(project_id: str, specimen_id: str = None):
    """
    GET:
        specimen_id: uses the form "1,3-8,9", if not provided all specimens are returned
    return: list all specimens or selected specimens, belonging to <project>

    Query:
    (pair, tumor):  Default (None, true)
    The tumor query only have an effect if pair is false
        (None, true/false):
            Return: all or selected specimens (Default)
        (true, true/false):
            Return: a subset of specimen who have Tumor=False & Tumor=True samples
        (false, true):
            return: a subset of specimen who only have Tumor=True samples
        (false, false):
            return: a subset of specimen who only have Tumor=false samples
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

    if specimen_id is not None:
        specimen_id = unroll(specimen_id)

    if query.get('name'):
        name = query['name']
    if name:
        specimen_id = []
        for specimen_name in name.split(","):
            specimen_id.extend(db_action.name_to_id("Specimen", specimen_name))

    if isinstance(project_id, dict) and project_id.get("DB_ACTION_WARNING"):
        return project_id

    # pair being either True or False
    if pair is not None:
        action_output = db_action.specimen_pair(
            project_id,
            specimen_id=specimen_id,
            pair=pair,
            tumor=tumor
            )
    else:
        action_output = db_action.specimens(
            project_id,
            specimen_id=specimen_id
            )
    return sanity_check("Specimen", action_output)


@bp.route('/<string:project>/samples')
@bp.route('/<string:project>/samples/<string:sample_id>')
@convcheck_project
def samples(project_id: str, sample_id: str = None):
    """
    GET:
        sample_id: uses the form "1,3-8,9", if not provided all samples are returned
    return: list all specimens or selected samples, belonging to <project>
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

    if isinstance(project_id, dict) and project_id.get("DB_ACTION_WARNING"):
        return project_id

    action_output = db_action.samples(project_id, sample_id=sample_id)

    return sanity_check("Sample", action_output)

@bp.route('/<string:project>/readsets')
@bp.route('/<string:project>/readsets/<string:readset_id>')
@convcheck_project
def readsets(project_id: str, readset_id: str=None):
    """
    GET:
        readset_id: uses the form "1,3-8,9", if not provided all readsets are returned
    return: list all specimens or selected readsets, belonging to <project>
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

    if isinstance(project_id, dict) and project_id.get("DB_ACTION_WARNING"):
        return project_id

    action_output = db_action.readsets(project_id, readset_id=readset_id)

    return sanity_check("Readset", action_output)


@bp.route('/<string:project>/files/<string:file_id>')
@bp.route('/<string:project>/specimens/<string:specimen_id>/files')
@bp.route('/<string:project>/samples/<string:sample_id>/files')
@bp.route('/<string:project>/readsets/<string:readset_id>/files')
@convcheck_project
def files(project_id: str, specimen_id: str=None, sample_id: str=None, readset_id: str=None, file_id: str=None):
    """
    GET:
        file_id: uses the form "1,3-8,9". Select file by ids
        specimen_id: uses the form "1,3-8,9". Select file by specimen ids
        sample_id: uses the form "1,3-8,9". Select file by sample ids
        redeaset_id: uses the form "1,3-8,9". Select file by readset ids
    return: selected files, belonging to <project>

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

    if specimen_id is not None:
        specimen_id = unroll(specimen_id)
    elif sample_id is not None:
        sample_id = unroll(sample_id)
    elif readset_id is not None:
        readset_id = unroll(readset_id)
    elif file_id is not None:
        file_id = unroll(file_id)

    if deliverable is not None:
        action_output = db_action.files_deliverable(
            project_id=project_id,
            specimen_id=specimen_id,
            sample_id=sample_id,
            readset_id=readset_id,
            file_id=file_id,
            deliverable=deliverable
            )
    else:
        action_output = db_action.files(
            project_id=project_id,
            specimen_id=specimen_id,
            sample_id=sample_id,
            readset_id=readset_id,
            file_id=file_id
            )

    if isinstance(project_id, dict) and project_id.get("DB_ACTION_WARNING"):
        return project_id

    return sanity_check("File", action_output)



@bp.route('/<string:project>/metrics', methods=['GET', 'POST'])
@bp.route('/<string:project>/metrics/<string:metric_id>')
@bp.route('/<string:project>/specimens/<string:specimen_id>/metrics')
@bp.route('/<string:project>/samples/<string:sample_id>/metrics')
@bp.route('/<string:project>/readsets/<string:readset_id>/metrics')
@convcheck_project
def metrics(project_id: str, specimen_id: str=None, sample_id: str=None, readset_id: str=None, metric_id: str=None):
    """
    GET:
        metric_id: uses the form "1,3-8,9". Select metric by ids
        specimen_id: uses the form "1,3-8,9". Select metric by specimen ids
        sample_id: uses the form "1,3-8,9". Select metric by sample ids
        redeaset_id: uses the form "1,3-8,9". Select metric by readset ids
    return: selected metrics, belonging to <project>

    We also accept POST data with comma separeted list
    metric_name = <NAME> [,NAME] [...]
    readset_name = <NAME> [,NAME] [...]
    sample_name = <NAME> [,NAME] [...]
    specimen_name = <NAME> [,NAME] [...]

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
        if post_input[0] in ["metric_name", "readset_name", "sample_name", "specimen_name"]:
            model_class = post_input[0].split('_')[0]
            names = post_input[1].split(',')
            ids = db_action.name_to_id(model_class.capitalize(), names)
            if post_input[0] == "metric_name":
                metric_id = ids
            elif post_input[0] == "readset_name":
                readset_id = ids
            elif post_input[0] == "sample_name":
                sample_id = ids
            elif post_input[0] == "specimen_name":
                specimen_id = ids
    elif specimen_id is not None:
        specimen_id = unroll(specimen_id)
    elif sample_id is not None:
        sample_id = unroll(sample_id)
    elif readset_id is not None:
        readset_id = unroll(readset_id)
    elif metric_id is not None:
        metric_id = unroll(metric_id)

    if deliverable is not None:
        action_output = db_action.metrics_deliverable(
            project_id=project_id,
            specimen_id=specimen_id,
            sample_id=sample_id,
            readset_id=readset_id,
            metric_id=metric_id,
            deliverable=deliverable
            )
    else:
        action_output = db_action.metrics(
            project_id=project_id,
            specimen_id=specimen_id,
            sample_id=sample_id,
            readset_id=readset_id,
            metric_id=metric_id
            )

    if isinstance(project_id, dict) and project_id.get("DB_ACTION_WARNING"):
        return project_id

    return sanity_check("Metric", action_output)

@bp.route('/<string:project>/samples/<string:sample_id>/readsets')
@convcheck_project
def readsets_from_samples(project_id: str, sample_id: str):
    """
    GET:
        sample_id: uses the form "1,3-8,9"
    return: selected readsets belonging to <sample_id>
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

    if isinstance(project_id, dict) and project_id.get("DB_ACTION_WARNING"):
        return project_id

    action_output = db_action.readsets(project_id, sample_id)

    return sanity_check("Readset", action_output)


@bp.route('/<string:project>/digest_readset_file', methods=['POST'])
@convcheck_project
def digest_readset_file(project_id: str):
    """
    POST: json holding the list of Specimen/Sample/Readset Name or id AND location endpoint + experiment nucleic_acid_type
    return: all information to create a "Genpipes readset file"
    """

    if request.method == 'POST':
        try:
            ingest_data = request.get_json(force=True)
        except:
            flash('Data does not seems to be json')
            return redirect(request.url)

        if isinstance(project_id, dict) and project_id.get("DB_ACTION_WARNING"):
            return project_id

        return db_action.digest_readset_file(project_id=project_id, digest_data=ingest_data)


@bp.route('/<string:project>/digest_pair_file', methods=['POST'])
@convcheck_project
def digest_pair_file(project_id: str):
    """
    POST: json holding the list of Specimen/Sample/Readset Name or id AND location endpoint + experiment nucleic_acid_type
    return: all information to create a "Genpipes pair file"
    """

    if request.method == 'POST':
        try:
            ingest_data = request.get_json(force=True)
        except:
            flash('Data does not seems to be json')
            return redirect(request.url)

        if isinstance(project_id, dict) and project_id.get("DB_ACTION_WARNING"):
            return project_id

        return db_action.digest_pair_file(project_id=project_id, digest_data=ingest_data)


@bp.route('/<string:project>/ingest_run_processing', methods=['POST'])
@convcheck_project
def ingest_run_processing(project_id: str):
    """
    POST: json describing run processing
    return: The Operation object
    """

    if request.method == 'POST':
        try:
            ingest_data = request.get_json(force=True)
        except:
            flash('Data does not seems to be json')
            return redirect(request.url)

        if isinstance(project_id, dict) and project_id.get("DB_ACTION_WARNING"):
            return project_id

        out = db_action.ingest_run_processing(project_id=project_id, ingest_data=ingest_data)
        logger.debug(f"ingest_run_processing: {out}")
        out["DB_ACTION_OUTPUT"] = [i.flat_dict for i in out["DB_ACTION_OUTPUT"]]

        return out


@bp.route('/<string:project>/ingest_transfer', methods=['POST'])
@convcheck_project
def ingest_transfer(project_id: str):
    """
    POST: json describing a transfer
    return: The Operation object
    """
    if request.method == 'POST':
        try:
            ingest_data = request.get_json(force=True)
        except:
            flash('Data does not seems to be json')
            return redirect(request.url)

        if isinstance(project_id, dict) and project_id.get("DB_ACTION_WARNING"):
            return project_id

        out = db_action.ingest_transfer(project_id=project_id, ingest_data=ingest_data)
        out["DB_ACTION_OUTPUT"] = [i.flat_dict for i in out["DB_ACTION_OUTPUT"]]

        return out


@bp.route('/<string:project>/ingest_genpipes', methods=['POST'])
@convcheck_project
def ingest_genpipes(project_id: str):
    """
    POST: json describing genpipes analysis
    return: The Operation object and Jobs associated
    """

    if request.method == 'POST':
        try:
            ingest_data = request.get_json(force=True)
        except:
            flash('Data does not seems to be json')
            return redirect(request.url)

        if isinstance(project_id, dict) and project_id.get("DB_ACTION_WARNING"):
            return project_id

        out = db_action.ingest_genpipes(project_id=project_id, ingest_data=ingest_data)
        out["DB_ACTION_OUTPUT"] = [i.flat_dict for i in out["DB_ACTION_OUTPUT"]]

        return out

@bp.route('/<string:project>/digest_unanalyzed', methods=['POST'])
@convcheck_project
def digest_unanalyzed(project_id: str):
    """
    POST: json holding the list of Sample/Readset Name or id AND location endpoint + experiment nucleic_acid_type
    return: Samples/Readsets unanalyzed with location endpoint + experiment nucleic_acid_type
    """
    if request.method == 'POST':
        try:
            ingest_data = request.get_json(force=True)
        except:
            flash('Data does not seems to be json')
            return redirect(request.url)

        if isinstance(project_id, dict) and project_id.get("DB_ACTION_WARNING"):
            return project_id

        return db_action.digest_unanalyzed(project_id=project_id, digest_data=ingest_data)


@bp.route('/<string:project>/digest_delivery', methods=['POST'])
@convcheck_project
def digest_delivery(project_id: str):
    """
    POST: json holding the list of Specimen/Sample/Readset Name or id AND location endpoint + experiment nucleic_acid_type (optional)
    return: Samples/Readsets unanalyzed with location endpoint + experiment nucleic_acid_type
    """
    if request.method == 'POST':
        try:
            ingest_data = request.get_json(force=True)
        except:
            flash('Data does not seems to be json')
            return redirect(request.url)

        if isinstance(project_id, dict) and project_id.get("DB_ACTION_WARNING"):
            return project_id

        return db_action.digest_delivery(project_id=project_id, digest_data=ingest_data)
