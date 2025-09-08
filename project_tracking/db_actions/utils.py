"""
Utility functions for db_actions package.
"""

import re

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

def sanitize_name(name):
    """
    Sanitizes the input name for regex usage.
    If the name is marked as an explicit regex (starts with 'r:'), it is returned as-is (without the 'r:' prefix).
    Otherwise, it is escaped to treat it as a literal string in regex.
    """
    if is_explicit_regex(name):
        return name[2:], True  # Strip prefix, treat as regex
    return re.escape(name), False  # Escape if not marked

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

    if isinstance(name, str):
        name = [name]

    patterns = []
    use_regex = False

    for n in name:
        pattern, is_regex = sanitize_name(n)
        patterns.append(pattern)
        use_regex = use_regex or is_regex

    if use_regex:
        stmt = select(the_class.id).where(
            or_(*[the_class.name.op("~")(pattern) for pattern in patterns])
        )
    else:
        stmt = select(the_class.id).where(the_class.name.in_(name))

    ids = session.execute(stmt).scalars().all()

    if not ids:
        raise DidNotFindError(table=model_class, attribute="name", query=", ".join(name))

    return ids


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
