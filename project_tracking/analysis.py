"""Controlled vocabulary of the analysis operations accepted by ingest_analysis."""
import json
import re
from importlib import resources

import flask

# operation_name -> operation_pipeline -> allowed operation_protocol values, see analysis_operations.json.
# A tool without pipelines maps to {} (no operation_pipeline accepted), a pipeline without protocols to [].
# Overridden with the ANALYSIS_OPERATIONS config (C3G_ANALYSIS_OPERATIONS or C3G_ANALYSIS_OPERATIONS_FILE, as JSON).
DEFAULT_ANALYSIS_OPERATIONS = json.loads(
    resources.files(__package__).joinpath("analysis_operations.json").read_text(encoding="utf-8")
)

# Operation names set by the other ingest routes: digests rely on them, so analyses can't use them
RESERVED_OPERATION_NAMES = ("run_processing", "transfer", "delivery")

NAME_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$")


def validate_analysis_operations(vocabulary):
    """
    Check the shape and the names of an analysis operations vocabulary; raises ValueError listing every problem.
    """
    if not isinstance(vocabulary, dict):
        raise ValueError(f"ANALYSIS_OPERATIONS must be a JSON object, got {type(vocabulary).__name__}.")
    problems = []
    for name, pipelines in vocabulary.items():
        if not NAME_PATTERN.match(str(name)):
            problems.append(f"invalid operation name {name!r}")
        if name in RESERVED_OPERATION_NAMES:
            problems.append(f"operation name {name!r} is reserved")
        if not isinstance(pipelines, dict):
            problems.append(f"{name!r} must map pipeline names to protocol lists")
            continue
        for pipeline, protocols in pipelines.items():
            if not NAME_PATTERN.match(str(pipeline)):
                problems.append(f"invalid pipeline name {pipeline!r} for {name!r}")
            if not isinstance(protocols, list):
                problems.append(f"protocols of {name!r} {pipeline!r} must be a list")
                continue
            for protocol in protocols:
                if not NAME_PATTERN.match(str(protocol)):
                    problems.append(f"invalid protocol name {protocol!r} for {name!r} {pipeline!r}")
    if problems:
        raise ValueError(
            "Invalid ANALYSIS_OPERATIONS: " + "; ".join(problems) +
            ". Names use letters, digits, '_', '.' and '-' (64 characters max)."
        )


def analysis_operations():
    """
    The vocabulary in use: the app config when called within the app, the default otherwise (e.g. scripts, tests).
    """
    if flask.has_app_context():
        return flask.current_app.config.get("ANALYSIS_OPERATIONS", DEFAULT_ANALYSIS_OPERATIONS)
    return DEFAULT_ANALYSIS_OPERATIONS
