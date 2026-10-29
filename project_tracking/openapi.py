"""
OpenAPI 3 specification generated from the app, so it follows every route change: paths and methods from the URL
map, summaries and descriptions from the view docstrings, the json query and body keys from the parse_json_*
decorators, and the entities returned from the marshmallow schemas. Served at /openapi.json and shown at /docs.
"""
import inspect
import json
import re

import click
from apispec import APISpec
from apispec.ext.marshmallow import MarshmallowPlugin
from flask import current_app
from flask.cli import with_appcontext
from sqlalchemy import Enum as SQLAlchemyEnum, JSON, inspect as sqlalchemy_inspect

from . import schema
from .auth import PUBLIC_ENDPOINTS
from .__version__ import __version__

OPENAPI_VERSION = "3.0.3"

DESCRIPTION = """\
Tracks the C3G genomics data processing: projects, specimens, samples, readsets, runs, operations, jobs, files and
metrics. [pt-cli](https://github.com/c3g/project_tracking_cli) is its command line client.

Every response has the same envelope: `DB_ACTION_OUTPUT` holds the result, `DB_ACTION_WARNING` the non blocking
warnings and `DB_ACTION_ERROR` the errors. GET filters go in one `json` query parameter holding a URL-encoded JSON
object, e.g. `/project/MY-PROJECT/samples?json={"sample_name": ["SAMPLE-1-1DT"]}`.

With authentication on, every route but the documentation, the login, `/version` and `/health` needs a COmanage
token: `Authorization: Bearer <token>`. `pt-cli token` prints one to paste in **Authorize**. 401 means no valid
token, 403 that your groups don't give the role the route needs on the project's database.
"""

# Blueprint name -> tag, in display order
TAGS = (
    ("general", "API root and route listing"),
    ("project", "Data of one project: queries, ingestion and digests. The project is given by name, any case."),
    ("modification", "Edit, delete, deprecate and curate rows of any table"),
    ("vocabulary", "Controlled vocabularies: enum values and analysis operations"),
    ("admin", "Project creation and cross database lookups"),
    ("version", "API version"),
)
BLUEPRINT_TAGS = {"admin_api": "admin"}
# Endpoints left out: the documentation itself and static files
EXCLUDED_ENDPOINTS = re.compile(r"(^|\.)static$|^docs\.")

# Entity returned by a route, from the last static segment of its path
ENTITY_SCHEMAS = {
    "project": ("Project", schema.create_project_schema),
    "specimens": ("Specimen", schema.create_specimen_schema),
    "samples": ("Sample", schema.create_sample_schema),
    "readsets": ("Readset", schema.create_readset_schema),
    "operations": ("Operation", schema.create_operation_schema),
    "jobs": ("Job", schema.create_job_schema),
    "files": ("File", schema.create_file_schema),
    "locations": ("Location", schema.create_location_schema),
    "metrics": ("Metric", schema.create_metric_schema),
}

PATH_PARAMETER_DESCRIPTIONS = {
    "project": "Project name, matched ignoring case. Numeric ids are refused.",
}
CONVERTER_TYPES = {"IntegerConverter": "integer", "FloatConverter": "number"}


def docstring_parts(func, method="get"):
    """
    Summary and description of a view from its docstring. The summary is the 'return:' line of a GET (what it lists)
    and otherwise the first line (what is sent), without a 'GET:'/'POST:' prefix; the description is the whole
    docstring kept as preformatted text.
    """
    doc = inspect.getdoc(func) or ""
    lines = [line.strip() for line in doc.splitlines() if line.strip()]
    summary = ""
    returns = [line for line in lines if method == "get" and re.match(r"^returns?\s*:", line, re.IGNORECASE)]
    for line in returns[:1] + lines:
        summary = re.sub(r"^(GET|POST|PUT|PATCH|DELETE|returns?)\s*:\s*", "", line, flags=re.IGNORECASE).strip()
        if summary:
            summary = summary[0].upper() + summary[1:]
            break
    description = f"```text\n{doc}\n```" if doc else ""
    return summary, description


def entity_component(spec, converter, name, factory):
    """
    JSON schema of an entity as the routes return it: the marshmallow fields, the enum columns as their values, the
    JSON columns as lists or objects (by their default), the 'tablename' added when dumping and the *_id(s) Method
    fields as integers.
    """
    if name in spec.components.schemas:
        return
    instance = factory()
    component = converter.schema2jsonschema(instance)
    properties = component.setdefault("properties", {})
    model = instance.Meta.model
    # Dumps may add fields, e.g. '_database' on the admin routes
    component.pop("additionalProperties", None)
    for attr in sqlalchemy_inspect(model).column_attrs:
        column = attr.columns[0]
        if attr.key not in properties:
            continue
        if isinstance(column.type, SQLAlchemyEnum):
            properties[attr.key] = {"type": "string", "enum": [member.value for member in column.type.enum_class]}
        elif isinstance(column.type, JSON):
            default = getattr(column.default, "arg", None)
            is_list = getattr(default, "__name__", None) == "list"
            properties[attr.key] = {"type": "array", "items": {}} if is_list else {"type": "object"}
            properties[attr.key]["nullable"] = True
    for field_name in properties:
        if field_name.endswith("_ids"):
            properties[field_name] = {"type": "array", "items": {"type": "integer"}}
        elif field_name.endswith("_id") and not properties[field_name]:
            properties[field_name] = {"type": "integer", "nullable": True}
    properties["tablename"] = {"type": "string", "example": model.__tablename__}
    spec.components.schema(name, component)


def envelope(output_items=None):
    """
    The DB_ACTION_* response envelope, DB_ACTION_OUTPUT holding output_items.
    """
    items = output_items or {}
    return {
        "type": "object",
        "properties": {
            "DB_ACTION_OUTPUT": {"type": "array", "items": items},
            "DB_ACTION_WARNING": {"$ref": "#/components/schemas/Messages"},
            "DB_ACTION_ERROR": {"$ref": "#/components/schemas/Messages"},
        },
    }


def json_object(keys, description):
    """
    Schema of a JSON object limited to keys (any key when keys is None).
    """
    if keys is None:
        return {"type": "object", "description": description}
    return {
        "type": "object",
        "description": description,
        "properties": {key: {} for key in sorted(keys)},
        "additionalProperties": False,
    }


def operation_for(rule, method, view, entity_ref):
    summary, description = docstring_parts(view, method)
    blueprint = rule.endpoint.split(".")[0] if "." in rule.endpoint else "general"
    operation = {
        "tags": [BLUEPRINT_TAGS.get(blueprint, blueprint)],
        "summary": summary,
        "operationId": f"{method}_{re.sub(r'[^A-Za-z0-9]+', '_', rule.rule).strip('_') or 'root'}",
        "parameters": [],
        "responses": {
            "200": {
                "description": "Success: the result in DB_ACTION_OUTPUT, with any warnings",
                "content": {"application/json": {"schema": envelope(entity_ref)}},
            },
            "400": {
                "description": "Invalid request: DB_ACTION_ERROR says why",
                "content": {"application/json": {"schema": {"$ref": "#/components/schemas/Error"}}},
            },
        },
    }
    error = {"application/json": {"schema": {"$ref": "#/components/schemas/Error"}}}
    if "project" in rule.arguments:
        operation["responses"]["404"] = {"description": "The project in the URL doesn't exist", "content": error}
    if rule.endpoint.split(".")[-1].startswith("ingest_"):
        operation["responses"]["409"] = {"description": "Duplicate, e.g. readsets already ingested", "content": error}
    if rule.endpoint in PUBLIC_ENDPOINTS:
        operation["security"] = []
    else:
        operation["responses"]["401"] = {"description": "No valid token", "content": error}
        operation["responses"]["403"] = {"description": "Your groups don't give the role this route needs",
                                         "content": error}
    if description:
        operation["description"] = description
    for argument in sorted(rule.arguments):
        converter = type(rule._converters[argument]).__name__
        parameter = {
            "name": argument,
            "in": "path",
            "required": True,
            "schema": {"type": CONVERTER_TYPES.get(converter, "string")},
        }
        if argument in PATH_PARAMETER_DESCRIPTIONS:
            parameter["description"] = PATH_PARAMETER_DESCRIPTIONS[argument]
        operation["parameters"].append(parameter)

    json_input = getattr(view, "openapi_json", None)
    if json_input and json_input["in"] == "query" and method == "get":
        keys = json_input["keys"]
        listed = ", ".join(f"`{key}`" for key in sorted(keys)) if keys is not None else "any key"
        operation["parameters"].append({
            "name": "json",
            "in": "query",
            "required": False,
            "description": f"URL-encoded JSON object of filters, all optional: {listed}.",
            "content": {"application/json": {"schema": json_object(keys, "Filters")}},
        })
    elif json_input and json_input["in"] == "body" and method in ("post", "put", "patch"):
        operation["requestBody"] = {
            "required": False,
            "content": {"application/json": {"schema": json_object(json_input["keys"], "JSON payload")}},
        }
    for key, value in getattr(view, "openapi", {}).items():
        if key == "parameters":
            operation["parameters"].extend(value)
        else:
            operation[key] = value
    if not operation["parameters"]:
        del operation["parameters"]
    return operation


def build_spec(app):
    """
    The OpenAPI specification of app, as a dict.
    """
    marshmallow_plugin = MarshmallowPlugin()
    spec = APISpec(
        title="Project tracking API",
        version=__version__,
        openapi_version=OPENAPI_VERSION,
        plugins=[marshmallow_plugin],
        info={"description": DESCRIPTION},
        security=[{"bearerAuth": []}],
        tags=[{"name": name, "description": text} for name, text in TAGS],
    )
    converter = marshmallow_plugin.converter
    spec.components.security_scheme("bearerAuth", {"type": "http", "scheme": "bearer", "bearerFormat": "JWT"})
    spec.components.schema("Messages", {
        "description": "One message or a list of messages",
        "oneOf": [{"type": "string"}, {"type": "array", "items": {"type": "string"}}],
    })
    spec.components.schema("Error", {
        "type": "object",
        "properties": {"DB_ACTION_ERROR": {"$ref": "#/components/schemas/Messages"}},
    })

    paths = {}
    for rule in app.url_map.iter_rules():
        if EXCLUDED_ENDPOINTS.search(rule.endpoint):
            continue
        view = app.view_functions[rule.endpoint]
        path = re.sub(r"<(?:[^:<>]+:)?([^<>]+)>", r"{\1}", rule.rule)
        if path != "/":
            path = path.rstrip("/")
        static_segments = [segment for segment in path.split("/") if segment and not segment.startswith("{")]
        entity_ref = None
        if rule.endpoint.startswith("project.") and static_segments and static_segments[-1] in ENTITY_SCHEMAS:
            name, factory = ENTITY_SCHEMAS[static_segments[-1]]
            entity_component(spec, converter, name, factory)
            entity_ref = {"$ref": f"#/components/schemas/{name}"}
        for method in sorted(rule.methods - {"HEAD", "OPTIONS"}):
            paths.setdefault(path, {})[method.lower()] = operation_for(rule, method.lower(), view, entity_ref)

    for path in sorted(paths):
        spec.path(path=path, operations=paths[path])
    return spec.to_dict()


def spec_json(spec):
    """
    Stable JSON text of a spec, for the committed docs/openapi.json.
    """
    return json.dumps(spec, indent=2, sort_keys=True) + "\n"


@click.command("openapi")
@click.option("--output", "-o", type=click.Path(dir_okay=False), help="File to write, standard output otherwise.")
@with_appcontext
def openapi_command(output=None):
    """Print the OpenAPI specification of the API, generated from the routes."""
    text = spec_json(build_spec(current_app))
    if output:
        with open(output, "w", encoding="utf-8") as f:
            f.write(text)
        click.echo(f"OpenAPI specification written to {output}")
    else:
        click.echo(text, nl=False)
