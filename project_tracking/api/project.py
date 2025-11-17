"""Project API"""
import time
import functools
import logging
import json

from flask import Blueprint, request, flash, redirect, jsonify
from werkzeug.exceptions import BadRequest

from .. import db_actions
from ..database import session_scope
from ..schema import serialize

logger = logging.getLogger(__name__)

bp = Blueprint('project', __name__, url_prefix='/project')

def unroll(value):
    """
    Accepts either a string in the form "1,3-7,9" or a list of integers.
    Returns a list of integers.
    Raises ValueError if input contains non-integer values.
    """
    if isinstance(value, list):
        return value

    if not isinstance(value, str):
        raise ValueError("Input must be a string or list of integers in the form '1,3-7,9'.")

    elem = [e for e in value.split(',') if e]
    unroll_list = []
    for e in elem:
        if '-' in e:
            try:
                first, last = map(int, e.split('-'))
                unroll_list.extend(range(min(first, last), max(first, last) + 1))
            except ValueError as exc:
                raise ValueError(f"Invalid range format: '{e}' is not a valid integer range. Accepted format must be similar to '1,3-7,9'.") from exc
        else:
            try:
                unroll_list.append(int(e))
            except ValueError as exc:
                raise ValueError(f"Invalid value: '{e}' is not an integer.") from exc

    return unroll_list


def convcheck_project(func):
    """
    Converts project name to project ID if needed and validates existence.
    Returns standardized DB_ACTION_ERROR if project is not found.
    If project is None, it passes None to the decorated function.
    If project is a string, it converts it to an ID using db_actions.
    If project is a list, it assumes it's already an ID or a list of IDs.
    If project is a digit, it passes it as is.
    If project is not found, it returns a JSON response with available projects.
    Args:
        func (function): The function to decorate.
    Returns:
        function: The decorated function that checks project validity.
    """
    @functools.wraps(func)
    def wrapper(*args, project=None, **kwargs):
        project_id = None

        if project is None:
            project_id = None
        elif project.isdigit():
            project_id = project
        else:
            project_id = db_actions.name_to_id("Project", project.upper())
            # Handle empty list case as non-existent project
            if isinstance(project_id, list) and not project_id:
                with session_scope() as session:
                    _, available = db_actions.project_exists(session, None)
                    return jsonify({
                        "DB_ACTION_ERROR": [
                            f"Project '{project}' not found.",
                            "Available projects:",
                            *[
                                f"id: {p['id']}, name: {p['name']}"
                                for p in available
                            ]
                        ]
                    }), 404
            if isinstance(project_id, list) and len(project_id) == 1:
                project_id = str(project_id[0])

        if project_id is not None:
            with session_scope() as session:
                exists, available = db_actions.project_exists(session, project_id)
                # Handle case where project does not exist
                if not exists:
                    return jsonify({
                        "DB_ACTION_ERROR": [
                            f"Project with ID '{project_id}' not found.",
                            "Available projects:",
                            *[
                                f"id: {p['id']}, name: {p['name']}"
                                for p in available
                            ]
                        ]
                    }), 404
        # Check for warning dicts
        if isinstance(project_id, dict) and project_id.get("DB_ACTION_WARNING"):
            return jsonify(project_id)

        return func(*args, project_id=project_id, **kwargs)

    return wrapper


def sanity_check(item, action_output):
    """
    Sanity check for the action output.
    If action_output is None or empty, return a warning.
    If action_output is a list, return the flat_dict of each item.
    If action_output is a dict with DB_ACTION_WARNING, return it.
    If action_output is a dict with DB_ACTION_OUTPUT, return the flat_dict of each item.
    If action_output is a dict with DB_ACTION_ERROR, return it.
    Args:
        item (str): The name of the item being checked (e.g., "Project", "Specimen").
        action_output (dict or list): The output from the database action.
    Returns:
        A JSON response with the appropriate message or data.
    """
    if not action_output:
        ret = {"DB_ACTION_WARNING": f"Requested {item} doesn't exist."}
    else:
        ret = action_output
    return jsonify(ret)

def get_all_subclasses(cls):
    """
    Recursively get all subclasses of a given class.
    Args:
        cls: The class to get subclasses for.
    Returns:
        A set of all subclasses.
    """
    subclasses = set()
    for subclass in cls.__subclasses__():
        subclasses.add(subclass)
        subclasses.update(get_all_subclasses(subclass))
    return subclasses

def fetch_and_format(query_fn, *args, include_relationships=False, **kwargs):
    """
    Fetch results from the database using the provided query function,
    and format them using Marshmallow serialization.

    Args:
        query_fn: The function to execute the query.
        *args: Positional arguments to pass to the query function.
        include_relationships: Whether to include relationship fields in the schema.
        **kwargs: Keyword arguments to pass to the query function, including session.

    Returns:
        A dictionary with keys 'DB_ACTION_OUTPUT' and 'DB_ACTION_WARNING'.
    """
    result = {}

    with session_scope() as session:
        query_result = query_fn(*args, session=session, **kwargs)
        start_flatten = time.time()

        output = query_result.get("DB_ACTION_OUTPUT", [])

        if output:
            try:
                result["DB_ACTION_OUTPUT"] = serialize(
                    output,
                    include_relationships=include_relationships,
                    context={"session": session}
                )
            except ValueError as e:
                logger.warning(str(e))
                result["DB_ACTION_OUTPUT"] = []
        else:
            result["DB_ACTION_OUTPUT"] = []

        end_flatten = time.time()
        logger.debug(f"Flattening took {end_flatten - start_flatten:.4f} seconds")

        warnings = query_result.get("DB_ACTION_WARNING")
        if warnings:
            result["DB_ACTION_WARNING"] = warnings

    return result


def parse_names(raw):
    """
    Splits a comma-separated string into a list of stripped names.
    Ignores empty entries.
    """
    return [n.strip() for n in raw.split(',') if n.strip()]

def parse_json_input(func):
    """
    Decorator to parse JSON input from the request.
    If the JSON is invalid, flash an error message and redirect to the same URL.
    Args:
        func (function): The function to decorate.
    Returns:
        function: The decorated function that processes JSON input.
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            ingest_data = request.get_json(force=True)
        except BadRequest:
            flash('Data does not seem to be valid JSON')
            return redirect(request.url)

        kwargs["ingest_data"] = ingest_data
        return func(*args, **kwargs)
    return wrapper

def parse_json_get(expected_keys=None):
    """
    Decorator to parse JSON input from the request's query parameters.
    If the JSON is invalid or contains unexpected keys, return a 400 Bad Request response.
    Args:
        expected_keys (set, optional): A set of expected keys in the JSON input.
            If provided, the decorator will check for unexpected keys.
    Returns:
        function: The decorated function that processes JSON input from query parameters.
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # Check for unexpected query parameters
            allowed_params = {'json'}
            unexpected_params = set(request.args.keys()) - allowed_params
            if unexpected_params:
                return jsonify({
                    "DB_ACTION_ERROR": [
                        f"Unexpected query parameter(s): {', '.join(unexpected_params)}. Allowed: {', '.join(allowed_params)}"
                    ]
                }), 400
            raw_json = request.args.get("json", "{}")
            try:
                digest_data = json.loads(raw_json)
            except json.JSONDecodeError as e:
                error_position = e.pos
                pointer_line = " " * error_position + "^"

                return jsonify({
                    "DB_ACTION_ERROR": [
                        "Invalid JSON",
                        f"Error: {str(e)}",
                        raw_json,
                        pointer_line
                    ]
                }), 400

            if expected_keys is not None:
                unexpected_keys = set(digest_data.keys()) - expected_keys
                if unexpected_keys:
                    return jsonify({
                        "DB_ACTION_ERROR": [
                            f"Unexpected keys in JSON: {', '.join(unexpected_keys)}"
                        ]
                    }), 400

            kwargs["digest_data"] = digest_data
            return func(*args, **kwargs)
        return wrapper
    return decorator


@bp.route('/', methods=['GET'])
@bp.route('/<string:project>', methods=['GET'])
@convcheck_project
def projects(project_id=None):
    """
    GET:
        project_id: uses the form "1,3-8,9", if not provided all projects are returned
    return: list all projects or selected projects
    Query:
    (project_name):
    The project_name query allows to get all projects labelled with a specific name
        (name):
            return: a subset of projects who have the name
            Ex: /project?json={"project_name": "<NAME1>,<NAME2>,..."}
    If project_id is provided, it will return the project with that ID.
    If project_name is provided in digest_data, it will convert the name to an ID.
    If no project_id or project_name is provided, it will return all projects.
    If the project does not exist, it will return a warning with available projects.
    """
    # Unroll project_id if it's a string
    try:
        if project_id is not None:
            project_id = unroll(project_id)
    except ValueError as exc:
        return jsonify({
            "DB_ACTION_ERROR": [
                f"Invalid ID format: {exc}"
            ]
        }), 400

    # Fetch project details
    result_dicts = fetch_and_format(
        db_actions.projects,
        project_id=project_id
    )

    return sanity_check("Project", result_dicts)


@bp.route('/<string:project>/specimens', methods=['GET'])
@bp.route('/<string:project>/specimens/<string:specimen_id>', methods=['GET'])
@bp.route('/<string:project>/samples/<string:sample_id>/specimens', methods=['GET'])
@bp.route('/<string:project>/readsets/<string:readset_id>/specimens', methods=['GET'])
@convcheck_project
@parse_json_get(expected_keys={"specimen_name", "sample_name", "readset_name", "include_relationships"})
def specimens(project_id: str, specimen_id: str=None, sample_id: str=None, readset_id: str=None, digest_data=None):
    """
    GET:
        specimen_id: uses the form "1,3-8,9", if not provided all specimens are returned
    return: list all specimens or selected specimens, belonging to <project>

    Query:
    (specimen_name):
    The specimen_name query allows to get all specimens labelled with a specific name
        (name):
            return: a subset of specimens who have the name
            Ex: /project/<project>/specimens?json={"specimen_name": "<NAME1>,<NAME2>,..."}
    (sample_name):
    The sample_name query allows to get all specimens belonging to a specific sample
        (name):
            return: a subset of specimens who have the sample name
            Ex: /project/<project>/specimens?json={"sample_name": "<NAME1>,<NAME2>,..."}
    (readset_name):
    The readset_name query allows to get all specimens belonging to a specific readset
        (name):
            return: a subset of specimens who have the readset name
            Ex: /project/<project>/specimens?json={"readset_name": "<NAME1>,<NAME2>,..."}
    (include_relationships):
    The include_relationships query allows to include related entities in the output
        (true):
            return: specimens with related entities included
            Ex: /project/<project>/specimens?json={"include_relationships": true}
        (false):
            return: specimens without related entities (default behavior)
            Ex: /project/<project>/specimens?json={"include_relationships": false}
    """
    # Don't serialize relationships by default aka when requesting all specimens
    include_relationships = False
    # Read digest_data for filtering
    if digest_data:
        specimen_name = digest_data.get("specimen_name")
        sample_name = digest_data.get("sample_name")
        readset_name = digest_data.get("readset_name")
        include_relationships = bool(digest_data.get("include_relationships"))
        if specimen_name:
            names = parse_names(specimen_name)
            ids = db_actions.name_to_id("Specimen", names)
            specimen_id = ids
        elif sample_name:
            names = parse_names(sample_name)
            ids = db_actions.name_to_id("Sample", names)
            sample_id = ids
        elif readset_name:
            names = parse_names(readset_name)
            ids = db_actions.name_to_id("Readset", names)
            readset_id = ids

    # Unroll the IDs if they are provided as strings or lists
    try:
        if specimen_id is not None:
            specimen_id = unroll(specimen_id)
            include_relationships = True
        elif sample_id is not None:
            sample_id = unroll(sample_id)
            include_relationships = True
        elif readset_id is not None:
            readset_id = unroll(readset_id)
            include_relationships = True
    except ValueError as exc:
        return jsonify({
            "DB_ACTION_ERROR": [
                f"Invalid ID format: {exc}"
            ]
        }), 400

    result_dicts = fetch_and_format(
        db_actions.specimens,
        project_id=project_id,
        specimen_id=specimen_id,
        sample_id=sample_id,
        readset_id=readset_id,
        include_relationships=include_relationships
        )

    return sanity_check("Specimen", result_dicts)


@bp.route('/<string:project>/samples', methods=['GET'])
@bp.route('/<string:project>/samples/<string:sample_id>', methods=['GET'])
@bp.route('/<string:project>/specimens/<string:specimen_id>/samples', methods=['GET'])
@bp.route('/<string:project>/readsets/<string:readset_id>/samples', methods=['GET'])
@convcheck_project
@parse_json_get(expected_keys={"pair", "tumour", "tumor", "specimen_name", "sample_name", "readset_name", "include_relationships"})
def samples(project_id: str, specimen_id: str=None, sample_id: str=None, readset_id: str=None, digest_data=None):
    """
    GET:
        sample_id: uses the form "1,3-8,9", if not provided all samples are returned
            Ex: /project/<project>/samples/<sample_id>
        specimen_id: uses the form "1,3-8,9", if not provided all samples are returned
            Ex: /project/<project>/specimens/<specimen_id>/samples
        readset_id: uses the form "1,3-8,9", if not provided all samples are returned
            Ex: /project/<project>/readsets/<readset_id>/samples
    return: list all samples or selected samples, belonging to <project>
    Query:
    (sample_name):
    The sample_name query allows to get all samples labelled with a specific name
        (name):
            return: a subset of samples who have the name
            Ex: /project/<project>/samples?json={"sample_name": "<NAME1>,<NAME2>,..."}
    (specimen_name):
    The specimen_name query allows to get all samples belonging to a specific specimen
        (name):
            return: a subset of samples who have the specimen name
            Ex: /project/<project>/samples?json={"specimen_name": "<NAME1>,<NAME2>,..."}
    (readset_name):
    The readset_name query allows to get all samples belonging to a specific readset
        (name):
            return: a subset of samples who have the readset name
            Ex: /project/<project>/samples?json={"readset_name": "<NAME1>,<NAME2>,..."}
    (pair):
    The pair query allows to get all samples that are paired
        (true):
            return: a subset of samples that are paired
            Ex: /project/<project>/samples?json={"pair": "true"}
        (false):
            return: a subset of samples that are not paired
            Ex: /project/<project>/samples?json={"pair": "false"}
    (tumour/tumor):
    The tumour query allows to get all samples that are tumour samples
        (true):
            return: a subset of samples that are tumour samples
            Ex: /project/<project>/samples?json={"tumour": "true"}
        (false):
            return: a subset of samples that are not tumour samples
            Ex: /project/<project>/samples?json={"tumour": "false"}
    (include_relationships):
    The include_relationships query allows to include related entities in the output
        (true):
            return: samples with related entities included
            Ex: /project/<project>/samples?json={"include_relationships": true}
        (false):
            return: samples without related entities (default behavior)
            Ex: /project/<project>/samples?json={"include_relationships": false}
    """
    # Don't serialize relationships by default aka when requesting all samples
    include_relationships = False
    # Read digest_data for filtering
    pair = None
    tumour = None
    if digest_data:
        pair = digest_data.get("pair")
        if pair is not None:
            pair = bool(pair)
        tumour = digest_data.get("tumour", digest_data.get("tumor"))
        if tumour is not None:
            tumour = bool(tumour)
        specimen_name = digest_data.get("specimen_name")
        sample_name = digest_data.get("sample_name")
        readset_name = digest_data.get("readset_name")
        include_relationships = bool(digest_data.get("include_relationships"))
        if readset_name:
            names = parse_names(readset_name)
            ids = db_actions.name_to_id("Readset", names)
            readset_id = ids
        elif sample_name:
            names = parse_names(sample_name)
            ids = db_actions.name_to_id("Sample", names)
            sample_id = ids
        elif specimen_name:
            names = parse_names(specimen_name)
            ids = db_actions.name_to_id("Specimen", names)
            specimen_id = ids

    # Unroll the IDs if they are provided as strings or lists
    try:
        if sample_id is not None:
            include_relationships = True
            sample_id = unroll(sample_id)
        elif specimen_id is not None:
            include_relationships = True
            specimen_id = unroll(specimen_id)
        elif readset_id is not None:
            include_relationships = True
            readset_id = unroll(readset_id)
    except ValueError as exc:
        return jsonify({
            "DB_ACTION_ERROR": [
                f"Invalid ID format: {exc}"
            ]
        }), 400

    if pair:
        result_dicts = fetch_and_format(
            db_actions.samples_pair,
            project_id=project_id,
            specimen_id=specimen_id,
            pair=pair,
            include_relationships=include_relationships
            )
    else:
        result_dicts = fetch_and_format(
            db_actions.samples,
            project_id=project_id,
            readset_id=readset_id,
            sample_id=sample_id,
            specimen_id=specimen_id,
            tumour=tumour,
            include_relationships=include_relationships
            )

    return sanity_check("Sample", result_dicts)

@bp.route('/<string:project>/readsets', methods=['GET'])
@bp.route('/<string:project>/readsets/<string:readset_id>', methods=['GET'])
@bp.route('/<string:project>/samples/<string:sample_id>/readsets', methods=['GET'])
@bp.route('/<string:project>/specimens/<string:specimen_id>/readsets', methods=['GET'])
@convcheck_project
@parse_json_get(expected_keys={"readset_name", "sample_name", "specimen_name", "include_relationships"})
def readsets(project_id: str, specimen_id: str=None, sample_id: str=None, readset_id: str=None, digest_data=None):
    """
    GET:
        readset_id: uses the form "1,3-8,9", if not provided all readsets are returned
            Ex: /project/<project>/readsets/<readset_id>
        specimen_id: uses the form "1,3-8,9", if not provided all readsets are returned
            Ex: /project/<project>/specimens/<specimen_id>/readsets
        sample_id: uses the form "1,3-8,9", if not provided all readsets are returned
            Ex: /project/<project>/samples/<sample_id>/readsets
    return: list all readsets or selected readsets, belonging to <project>
    Query:
    (readset_name):
    The readset_name query allows to get all readsets labelled with a specific name
        (name):
            return: a subset of readsets who have the name
            Ex: /project/<project>/readsets?json={"readset_name": "<NAME1>,<NAME2>,..."}
    (sample_name):
    The sample_name query allows to get all readsets belonging to a specific sample
        (name):
            return: a subset of readsets who have the sample name
            Ex: /project/<project>/readsets?json={"sample_name": "<NAME1>,<NAME2>,..."}
    (specimen_name):
    The specimen_name query allows to get all readsets belonging to a specific specimen
        (name):
            return: a subset of readsets who have the specimen name
            Ex: /project/<project>/readsets?json={"specimen_name": "<NAME1>,<NAME2>,..."}
    (include_relationships):
    The include_relationships query allows to include related entities in the output
        (true):
            return: readsets with related entities included
            Ex: /project/<project>/readsets?json={"include_relationships": true}
        (false):
            return: readsets without related entities (default behavior)
            Ex: /project/<project>/readsets?json={"include_relationships": false}
    """
    # Don't serialize relationships by default aka when requesting all readsets
    include_relationships = False
    # Read digest_data for filtering
    if digest_data:
        specimen_name = digest_data.get("specimen_name")
        sample_name = digest_data.get("sample_name")
        readset_name = digest_data.get("readset_name")
        include_relationships = bool(digest_data.get("include_relationships", False))
        if readset_name:
            names = parse_names(readset_name)
            ids = db_actions.name_to_id("Readset", names)
            readset_id = ids
        elif sample_name:
            names = parse_names(sample_name)
            ids = db_actions.name_to_id("Sample", names)
            sample_id = ids
        elif specimen_name:
            names = parse_names(specimen_name)
            ids = db_actions.name_to_id("Specimen", names)
            specimen_id = ids

    # Unroll the IDs if they are provided as strings or lists
    try:
        if sample_id is not None:
            include_relationships = True
            sample_id = unroll(sample_id)
        elif specimen_id is not None:
            include_relationships = True
            specimen_id = unroll(specimen_id)
        elif readset_id is not None:
            include_relationships = True
            readset_id = unroll(readset_id)
    except ValueError as exc:
        return jsonify({
            "DB_ACTION_ERROR": [
                f"Invalid ID format: {exc}"
            ]
        }), 400

    result_dicts = fetch_and_format(
        db_actions.readsets,
        project_id=project_id,
        readset_id=readset_id,
        sample_id=sample_id,
        specimen_id=specimen_id,
        include_relationships=include_relationships
        )

    return sanity_check("Readset", result_dicts)


@bp.route('/<string:project>/operations', methods=['GET'])
@bp.route('/<string:project>/operations/<string:operation_id>', methods=['GET'])
@bp.route('/<string:project>/readsets/<string:readset_id>/operations', methods=['GET'])
@convcheck_project
@parse_json_get(expected_keys={"operation_name", "readset_name", "include_relationships"})
def operations(project_id: str, readset_id: str=None, operation_id: str=None, digest_data=None):
    """
    GET:
        operation_id: uses the form "1,3-8,9". Select operation by ids
            Ex: /project/<project>/operations/<operation_id>
        readset_id: uses the form "1,3-8,9". Select operation by readset ids
            Ex: /project/<project>/readsets/<readset_id>/operations
    return: selected operations, belonging to <project>

    Query:
    (operation_name):
    The operation_name query allows to get all operations labelled with a specific name
        (name):
            return: a subset of operations who have the name
            Ex: /project/<project>/operations?json={"operation_name": "<NAME1>,<NAME2>,..."}
    (readset_name):
    The readset_name query allows to get all operations belonging to a specific readset
        (name):
            return: a subset of operations who have the readset name
            Ex: /project/<project>/operations?json={"readset_name": "<NAME1>,<NAME2>,..."}\
    (include_relationships):
    The include_relationships query allows to include related entities in the output
        (true):
            return: operations with related entities included
            Ex: /project/<project>/operations?json={"include_relationships": true}
        (false):
            return: operations without related entities (default behavior)
            Ex: /project/<project>/operations?json={"include_relationships": false}
    """
    # Don't serialize relationships by default aka when requesting all operations
    include_relationships = False
    # Read digest_data for filtering
    if digest_data:
        operation_name = digest_data.get("operation_name")
        readset_name = digest_data.get("readset_name")
        include_relationships = bool(digest_data.get("include_relationships", False))
        if operation_name:
            names = parse_names(operation_name)
            ids = db_actions.name_to_id("Operation", names)
            operation_id = ids
        elif readset_name:
            names = parse_names(readset_name)
            ids = db_actions.name_to_id("Readset", names)
            readset_id = ids

    # Unroll the IDs if they are provided as strings or lists
    try:
        if readset_id is not None:
            include_relationships = True
            readset_id = unroll(readset_id)
        elif operation_id is not None:
            include_relationships = True
            operation_id = unroll(operation_id)
    except ValueError as exc:
        return jsonify({
            "DB_ACTION_ERROR": [
                f"Invalid ID format: {exc}"
            ]
        }), 400

    result_dicts = fetch_and_format(
        db_actions.operations,
        project_id=project_id,
        readset_id=readset_id,
        operation_id=operation_id,
        include_relationships=include_relationships
        )

    return sanity_check("Operation", result_dicts)


@bp.route('/<string:project>/jobs', methods=['GET'])
@bp.route('/<string:project>/jobs/<string:job_id>', methods=['GET'])
@bp.route('/<string:project>/readsets/<string:readset_id>/jobs', methods=['GET'])
@convcheck_project
@parse_json_get(expected_keys={"job_name", "readset_name", "include_relationships"})
def jobs(project_id: str, readset_id: str=None, job_id: str=None, digest_data=None):
    """
    GET:
        job_id: uses the form "1,3-8,9". Select job by ids
            Ex: /project/<project>/jobs/<job_id>
        readset_id: uses the form "1,3-8,9". Select job by readset ids
            Ex: /project/<project>/readsets/<readset_id>/jobs
    return: selected jobs, belonging to <project>

    Query:
    (job_name):
    The job_name query allows to get all jobs labelled with a specific name
        (name):
            return: a subset of jobs who have the name
            Ex: /project/<project>/jobs?json={"job_name": "<NAME1>,<NAME2>,..."}
    (readset_name):
    The readset_name query allows to get all jobs belonging to a specific readset
        (name):
            return: a subset of jobs who have the readset name
            Ex: /project/<project>/jobs?json={"readset_name": "<NAME1>,<NAME2>,..."}
    (include_relationships):
    The include_relationships query allows to include related entities in the output
        (true):
            return: jobs with related entities included
            Ex: /project/<project>/jobs?json={"include_relationships": true}
        (false):
            return: jobs without related entities (default behavior)
            Ex: /project/<project>/jobs?json={"include_relationships": false}
    """
    # Don't serialize relationships by default aka when requesting all jobs
    include_relationships = False
    # Read digest_data for filtering
    if digest_data:
        job_name = digest_data.get("job_name")
        readset_name = digest_data.get("readset_name")
        include_relationships = bool(digest_data.get("include_relationships", False))
        if job_name:
            names = parse_names(job_name)
            ids = db_actions.name_to_id("Job", names)
            job_id = ids
        elif readset_name:
            names = parse_names(readset_name)
            ids = db_actions.name_to_id("Readset", names)
            readset_id = ids

    # Unroll the IDs if they are provided as strings or lists
    try:
        if readset_id is not None:
            include_relationships = True
            readset_id = unroll(readset_id)
        elif job_id is not None:
            include_relationships = True
            job_id = unroll(job_id)
    except ValueError as exc:
        return jsonify({
            "DB_ACTION_ERROR": [
                f"Invalid ID format: {exc}"
            ]
        }), 400

    result_dicts = fetch_and_format(
        db_actions.jobs,
        project_id=project_id,
        readset_id=readset_id,
        job_id=job_id,
        include_relationships=include_relationships
        )

    return sanity_check("Job", result_dicts)


@bp.route('/<string:project>/files', methods=['GET'])
@bp.route('/<string:project>/files/<string:file_id>', methods=['GET'])
@bp.route('/<string:project>/specimens/<string:specimen_id>/files', methods=['GET'])
@bp.route('/<string:project>/samples/<string:sample_id>/files', methods=['GET'])
@bp.route('/<string:project>/readsets/<string:readset_id>/files', methods=['GET'])
@convcheck_project
@parse_json_get(expected_keys={"file_name", "specimen_name", "sample_name", "readset_name", "deliverable", "state", "include_relationships"})
def files(project_id: str, specimen_id: str=None, sample_id: str=None, readset_id: str=None, file_id: str=None, digest_data=None):
    """
    GET:
        file_id: uses the form "1,3-8,9". Select file by ids
            Ex: /project/<project>/files/<file_id>
        specimen_id: uses the form "1,3-8,9". Select file by specimen ids
            Ex: /project/<project>/specimens/<specimen_id>/files
        sample_id: uses the form "1,3-8,9". Select file by sample ids
            Ex: /project/<project>/samples/<sample_id>/files
        redeaset_id: uses the form "1,3-8,9". Select file by readset ids
            Ex: /project/<project>/readsets/<readset_id>/files
    return: selected files, belonging to <project>

    Query:
    (deliverable):
    The deliverable query allows to get all files labelled as deliverable
        (true):
            return: a subset of metrics who have Deliverable=True
            Ex: /project/<project>/files?json={"deliverable": "true"}
        (false):
            return: a subset of metrics who have Deliverable=True
            Ex: /project/<project>/files?json={"deliverable": "false"}
    (state):
    The state query allows to get all files with a specific state
        (state):
            return: a subset of files who have the state VALID, ON_HOLD, INVALID or DELIVERED
            Ex: /project/<project>/files?json={"state": "<STATE>"}
    (file_name):
    The file_name query allows to get all files labelled with a specific name
        (name):
            return: a subset of files who have the name
            Ex: /project/<project>/files?json={"file_name": "<NAME1>,<NAME2>,..."}
    (specimen_name):
    The specimen_name query allows to get all files belonging to a specific specimen
        (name):
            return: a subset of files who have the specimen name
            Ex: /project/<project>/files?json={"specimen_name": "<NAME1>,<NAME2>,..."}
    (sample_name):
    The sample_name query allows to get all files belonging to a specific sample
        (name):
            return: a subset of files who have the sample name
            Ex: /project/<project>/files?json={"sample_name": "<NAME1>,<NAME2>,..."}
    (readset_name):
    The readset_name query allows to get all files belonging to a specific readset
        (name):
            return: a subset of files who have the readset name
            Ex: /project/<project>/files?json={"readset_name": "<NAME1>,<NAME2>,..."}
    (include_relationships):
    The include_relationships query allows to include related entities in the output
        (true):
            return: files with related entities included
            Ex: /project/<project>/files?json={"include_relationships": true}
        (false):
            return: files without related entities (default behavior)
            Ex: /project/<project>/files?json={"include_relationships": false}
    """
    # Don't serialize relationships by default aka when requesting all files
    include_relationships = False
    # Read digest_data for filtering
    deliverable = None
    state = None
    if digest_data:
        deliverable = digest_data.get("deliverable")
        if deliverable is not None:
            deliverable = bool(deliverable)
        state = digest_data.get("state")
        file_name = digest_data.get("file_name")
        specimen_name = digest_data.get("specimen_name")
        sample_name = digest_data.get("sample_name")
        readset_name = digest_data.get("readset_name")
        include_relationships = bool(digest_data.get("include_relationships", False))
        if file_name:
            names = parse_names(file_name)
            ids = db_actions.name_to_id("File", names)
            file_id = ids
        elif specimen_name:
            names = parse_names(specimen_name)
            ids = db_actions.name_to_id("Specimen", names)
            specimen_id = ids
        elif sample_name:
            names = parse_names(sample_name)
            ids = db_actions.name_to_id("Sample", names)
            sample_id = ids
        elif readset_name:
            names = parse_names(readset_name)
            ids = db_actions.name_to_id("Readset", names)
            readset_id = ids

    # Unroll the IDs if they are provided as strings or lists
    try:
        if specimen_id is not None:
            include_relationships = True
            specimen_id = unroll(specimen_id)
        elif sample_id is not None:
            include_relationships = True
            sample_id = unroll(sample_id)
        elif readset_id is not None:
            include_relationships = True
            readset_id = unroll(readset_id)
        elif file_id is not None:
            include_relationships = True
            file_id = unroll(file_id)
    except ValueError as exc:
        return jsonify({
            "DB_ACTION_ERROR": [
                f"Invalid ID format: {exc}"
            ]
        }), 400

    # Fetch results from the database
    result_dicts = fetch_and_format(
        db_actions.files,
        project_id=project_id,
        file_id=file_id,
        specimen_id=specimen_id,
        sample_id=sample_id,
        readset_id=readset_id,
        deliverable=deliverable,
        state=state,
        include_relationships=include_relationships
        )

    return sanity_check("File", result_dicts)


@bp.route('/<string:project>/metrics', methods=['GET'])
@bp.route('/<string:project>/metrics/<string:metric_id>', methods=['GET'])
@bp.route('/<string:project>/specimens/<string:specimen_id>/metrics', methods=['GET'])
@bp.route('/<string:project>/samples/<string:sample_id>/metrics', methods=['GET'])
@bp.route('/<string:project>/readsets/<string:readset_id>/metrics', methods=['GET'])
@convcheck_project
@parse_json_get(expected_keys={"metric_name", "specimen_name", "sample_name", "readset_name", "deliverable", "include_relationships"})
def metrics(project_id: str, specimen_id: str=None, sample_id: str=None, readset_id: str=None, metric_id: str=None, digest_data=None):
    """
    GET:
        metric_id: uses the form "1,3-8,9". Select metric by ids
            Ex: /project/<project>/metrics/<metric_id>
        specimen_id: uses the form "1,3-8,9". Select metric by specimen ids
            Ex: /project/<project>/specimens/<specimen_id>/metrics
        sample_id: uses the form "1,3-8,9". Select metric by sample ids
            Ex: /project/<project>/samples/<sample_id>/metrics
        redeaset_id: uses the form "1,3-8,9". Select metric by readset ids
            Ex: /project/<project>/readsets/<readset_id>/metrics
    return: selected metrics, belonging to <project>

    Query:
    (deliverable):
    The deliverable query allows to get all metrics labelled as deliverable
        (true):
            return: a subset of metrics who have Deliverable=True
            Ex: /project/<project>/metrics?json={"deliverable": "true"}
        (false):
            return: a subset of metrics who have Deliverable=False
            Ex: /project/<project>/metrics?json={"deliverable": "false"}
    (metric_name):
    The metric_name query allows to get all metrics labelled with a specific name
        (name):
            return: a subset of metrics who have the name
            Ex: /project/<project>/metrics?json={"metric_name": "<NAME1>,<NAME2>,..."}
    (specimen_name):
    The specimen_name query allows to get all metrics belonging to a specific specimen
        (name):
            return: a subset of metrics who have the specimen name
            Ex: /project/<project>/metrics?json={"specimen_name": "<NAME1>,<NAME2>,..."}
    (sample_name):
    The sample_name query allows to get all metrics belonging to a specific sample
        (name):
            return: a subset of metrics who have the sample name
            Ex: /project/<project>/metrics?json={"sample_name": "<NAME1>,<NAME2>,..."}
    (readset_name):
    The readset_name query allows to get all metrics belonging to a specific readset
        (name):
            return: a subset of metrics who have the readset name
            Ex: /project/<project>/metrics?json={"readset_name": "<NAME1>,<NAME2>,..."}
    (include_relationships):
    The include_relationships query allows to include related entities in the output
        (true):
            return: metrics with related entities included
            Ex: /project/<project>/metrics?json={"include_relationships": true}
        (false):
            return: metrics without related entities (default behavior)
    """
    # Don't serialize relationships by default aka when requesting all metrics
    include_relationships = False
    # Read digest_data for filtering
    deliverable = None
    if digest_data:
        deliverable = digest_data.get("deliverable")
        if deliverable is not None:
            deliverable = bool(deliverable)
        metric_name = digest_data.get("metric_name")
        specimen_name = digest_data.get("specimen_name")
        sample_name = digest_data.get("sample_name")
        readset_name = digest_data.get("readset_name")
        include_relationships = bool(digest_data.get("include_relationships"))
        if metric_name:
            names = parse_names(metric_name)
            ids = db_actions.name_to_id("Metric", names)
            metric_id = ids
        elif specimen_name:
            names = parse_names(specimen_name)
            ids = db_actions.name_to_id("Specimen", names)
            specimen_id = ids
        elif sample_name:
            names = parse_names(sample_name)
            ids = db_actions.name_to_id("Sample", names)
            sample_id = ids
        elif readset_name:
            names = parse_names(readset_name)
            ids = db_actions.name_to_id("Readset", names)
            readset_id = ids

    # Unroll the IDs if they are provided as strings or lists
    try:
        if specimen_id is not None:
            include_relationships = True
            specimen_id = unroll(specimen_id)
        elif sample_id is not None:
            include_relationships = True
            sample_id = unroll(sample_id)
        elif readset_id is not None:
            include_relationships = True
            readset_id = unroll(readset_id)
        elif metric_id is not None:
            include_relationships = True
            metric_id = unroll(metric_id)
    except ValueError as exc:
        return jsonify({
            "DB_ACTION_ERROR": [
                f"Invalid ID format: {exc}"
            ]
        }), 400

    # Fetch results from the database
    result_dicts = fetch_and_format(
        db_actions.metrics,
        project_id=project_id,
        metric_id=metric_id,
        specimen_id=specimen_id,
        sample_id=sample_id,
        readset_id=readset_id,
        deliverable=deliverable,
        include_relationships=include_relationships
        )

    return sanity_check("Metric", result_dicts)


# Ingest routes
@bp.route('/<string:project>/ingest_run_processing', methods=['POST'])
@convcheck_project
@parse_json_input
def ingest_run_processing(project_id: str, ingest_data):
    """
    POST: json describing run processing
    return: The Operation object
    """
    # Call the ingest_run_processing function from db_actions
    result = fetch_and_format(
        db_actions.ingest_run_processing,
        project_id=project_id,
        ingest_data=ingest_data
        )

    return jsonify(result)


@bp.route('/<string:project>/ingest_transfer', methods=['POST'])
@convcheck_project
@parse_json_input
def ingest_transfer(project_id: str, ingest_data):
    """
    POST: json describing a transfer
    return: The Operation object
    """
    result = fetch_and_format(
        db_actions.ingest_transfer,
        project_id=project_id,
        ingest_data=ingest_data,
        include_relationships=False
        )

    return jsonify(result)


@bp.route('/<string:project>/ingest_genpipes', methods=['POST'])
@convcheck_project
@parse_json_input
def ingest_genpipes(project_id: str, ingest_data):
    """
    POST: json describing genpipes analysis
    return: The Operation object and Jobs associated
    """
    result = fetch_and_format(
        db_actions.ingest_genpipes,
        project_id=project_id,
        ingest_data=ingest_data
        )

    return jsonify(result)


@bp.route('/<string:project>/ingest_delivery', methods=['POST'])
@convcheck_project
@parse_json_input
def ingest_delivery(project_id: str, ingest_data):
    """
    POST: json describing a delivery
    return: The Operation object and Files associated
    """
    result = fetch_and_format(
        db_actions.ingest_delivery,
        project_id=project_id,
        ingest_data=ingest_data
        )

    return jsonify(result)



# Digest routes
@bp.route('/<string:project>/digest_readset_file', methods=['GET'])
@convcheck_project
@parse_json_get()
def digest_readset_file(project_id: str, digest_data):
    """
    GET: JSON holding the list of Specimen/Sample/Readset Name or ID AND location endpoint + experiment_nucleic_acid_type
    Return: all information to create a "GenPipes readset file"
    """
    # Call the digest_readset_file function from db_actions
    with session_scope() as session:
        result = db_actions.digest_readset_file(
            project_id=project_id,
            digest_data=digest_data,
            session=session
        )
    return jsonify(result)


@bp.route('/<string:project>/digest_pair_file', methods=['GET'])
@convcheck_project
@parse_json_get()
def digest_pair_file(project_id: str, digest_data):
    """
    GET: json holding the list of Specimen/Sample/Readset Name or id AND location endpoint + experiment nucleic_acid_type
    return: all information to create a "Genpipes pair file"
    """
    # Call the digest_pair_file function from db_actions
    with session_scope() as session:
        result = db_actions.digest_pair_file(
            project_id=project_id,
            digest_data=digest_data,
            session=session
        )
    return jsonify(result)


@bp.route('/<string:project>/digest_unanalyzed', methods=['GET'])
@convcheck_project
@parse_json_get()
def digest_unanalyzed(project_id: str, digest_data):
    """
    GET: json holding the list of Sample/Readset Name or id AND location endpoint + experiment nucleic_acid_type
    return: Samples/Readsets unanalyzed with location endpoint + experiment nucleic_acid_type
    """
    # Call the digest_unanalyzed function from db_actions
    with session_scope() as session:
        result = db_actions.digest_unanalyzed(
            project_id=project_id,
            digest_data=digest_data,
            session=session
        )
    return jsonify(result)


@bp.route('/<string:project>/digest_delivery', methods=['GET'])
@convcheck_project
@parse_json_get()
def digest_delivery(project_id: str, digest_data):
    """
    GET: json holding the list of Specimen/Sample/Readset Name or id AND location endpoint + experiment nucleic_acid_type (optional)
    return: Samples/Readsets unanalyzed with location endpoint + experiment nucleic_acid_type
    """
    # Call the digest_delivery function from db_actions
    with session_scope() as session:
        result = db_actions.digest_delivery(
            project_id=project_id,
            digest_data=digest_data,
            session=session
        )
    return jsonify(result)
