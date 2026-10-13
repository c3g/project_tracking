"""
Utility functions for db_actions package.
"""

# Third-party
from sqlalchemy import select, or_

# Local modules
from .errors import DidNotFindError
from .. import database
from .. import model
from ..model import Project

def is_explicit_regex(s):
    """
    Checks if a string is marked as an explicit regex.
    An explicit regex starts with 'r:'.
    """
    return s.startswith("r:")

_REGEX_META_CHARS = set(".^$*+?{}[]|()\\")


def _escape_like(value):
    """Escape SQL LIKE special chars for literal matching."""
    return value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


def _classify_explicit_regex(pattern):
    """Return a fast-match class for simple explicit regex patterns.

    Supported optimizations (regex `~` is unanchored, so only anchors restrict the match):
    - ^literal$                  -> exact
    - ^literal / ^literal.*      -> prefix
    - literal$ / .*literal$      -> suffix
    - literal / .*literal.*      -> contains

    Any other regex remains a true regex predicate.
    """
    body = pattern
    anchored_start = body.startswith("^")
    anchored_end = body.endswith("$") and not body.endswith("\\$")

    if anchored_start:
        body = body[1:]
    if anchored_end:
        body = body[:-1]

    # A leading/trailing ".*" is equivalent to no anchor on that side
    if body.startswith(".*"):
        body = body[2:]
        anchored_start = False
    if body.endswith(".*"):
        body = body[:-2]
        anchored_end = False

    literal = body
    if anchored_start and anchored_end:
        kind = "exact"
    elif anchored_start:
        kind = "prefix"
    elif anchored_end:
        kind = "suffix"
    else:
        kind = "contains"

    if not literal:
        return None

    if any(ch in _REGEX_META_CHARS for ch in literal):
        return None

    return kind, literal


def _resolve_ids(the_class, column, values, session, model_class, attribute_label):
    """Resolve IDs from mixed exact values and optional explicit regex values."""
    if isinstance(values, str):
        values = [values]

    exact_values = []
    regex_patterns = []
    prefix_values = []
    suffix_values = []
    contains_values = []

    for value in values:
        if not is_explicit_regex(value):
            exact_values.append(value)
            continue

        pattern = value[2:]
        classified = _classify_explicit_regex(pattern)
        if classified is None:
            regex_patterns.append(pattern)
            continue

        kind, literal = classified
        if kind == "exact":
            exact_values.append(literal)
        elif kind == "prefix":
            prefix_values.append(literal)
        elif kind == "suffix":
            suffix_values.append(literal)
        elif kind == "contains":
            contains_values.append(literal)

    predicates = []
    if exact_values:
        predicates.append(column.in_(exact_values))

    predicates.extend([column.like(f"{_escape_like(v)}%", escape="\\") for v in prefix_values])
    predicates.extend([column.like(f"%{_escape_like(v)}", escape="\\") for v in suffix_values])
    predicates.extend([column.like(f"%{_escape_like(v)}%", escape="\\") for v in contains_values])
    predicates.extend([column.op("~")(pattern) for pattern in regex_patterns])

    if not predicates:
        raise DidNotFindError(table=model_class, attribute=attribute_label, query=", ".join(values))

    stmt = select(the_class.id).where(or_(*predicates))
    ids = session.execute(stmt).scalars().all()

    if not ids:
        raise DidNotFindError(table=model_class, attribute=attribute_label, query=", ".join(values))

    return ids

def name_to_id(model_class, name, session=None):
    """
    Converts a given name or list of names into their id(s) for a given model_class.
    Supports exact match and optional regex-based search.
    
    Parameters:
    - model_class: str, name of the model class
    - name: str or list of str, name(s) to search
    - session: SQLAlchemy session (optional)
    - use_regex: bool, whether to use regex search
    """
    if session is None:
        session = database.get_session()

    the_class = getattr(model, model_class)
    return _resolve_ids(the_class, the_class.name, name, session, model_class, "name")

def uri_to_id(model_class, uri, session=None):
    """
    Converts a given URI into its id for a given model_class.
    Supports exact match and optional regex-based search.
    
    Parameters:
    - model_class: str, name of the model class
    - uri: str, URI to search
    - session: SQLAlchemy session (optional)
    """
    if session is None:
        session = database.get_session()

    the_class = getattr(model, model_class)
    return _resolve_ids(the_class, the_class.uri, uri, session, model_class, "uri")

def select_all_projects():
    """
    Returns a SQLAlchemy Select statement for all projects.
    """
    return select(Project)


def project_exists(session, project_id):
    """
    Checks if a project with the given ID exists.
    Returns a tuple: (exists: bool, available_projects: list)
    """
    stmt = select(Project).where(Project.id == project_id)
    result = session.execute(stmt).scalar_one_or_none()

    if result:
        return True, []

    # If not found, return all available projects for context
    all_projects = session.scalars(select_all_projects()).all()
    return False, [{"id": p.id, "name": p.name} for p in all_projects]
