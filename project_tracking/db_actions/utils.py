"""
Utility functions for db_actions package.
"""

# Third-party
from sqlalchemy import select

# Local modules
from .errors import DidNotFindError
from .. import database
from .. import model
from ..model import Project

def name_to_id(model_class, name, session=None):
    """
    Converting a given name into its id(s) for a given model_class.
    """
    if session is None:
        session = database.get_session()

    the_class = getattr(model, model_class)

    if isinstance(name, str):
        name = [name]

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
