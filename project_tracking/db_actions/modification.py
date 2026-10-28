"""
Modification actions for the database.
"""
# Standard library
import logging
import json

# Third-party
from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import class_mapper
from sqlalchemy.orm.exc import UnmappedClassError

# Local modules
from .errors import DidNotFindError, RequestError
from .utils import to_enum
from .sample_group import group_name_problem
from .. import vocabulary as vb
from .. import database
from .. import model

logger = logging.getLogger(__name__)

# Condition Functions
def is_deleted(obj):
    """
    Returns True if the object is marked as deleted.
    """
    return getattr(obj, 'deleted', False)

def is_deprecated(obj):
    """
    Returns True if the object is marked as deprecated.
    """
    return getattr(obj, 'deprecated', False)

def should_delete(obj, direction):
    """
    Returns True if the object is marked as deleted or if all related objects
    in the specified direction ('children' or 'parents') are marked as deleted.
    """
    if hasattr(obj, 'deleted') and obj.deleted:
        return True

    try:
        mapper = class_mapper(obj.__class__)
    except UnmappedClassError:
        raise ValueError(f"Object of type {type(obj)} is not a mapped SQLAlchemy model.")

    table_name = obj.__class__.__name__.lower()
    cascade_info = CASCADE_MAP.get(table_name, {})
    related_entities = cascade_info.get(direction, [])

    for rel in mapper.relationships:
        rel_name = rel.key
        rel_entity = rel.mapper.class_.__name__.lower()

        if not any(entry['entity'] == rel_entity for entry in related_entities):
            continue

        related = getattr(obj, rel_name)
        if related is None:
            continue

        if rel.uselist:
            if any(not getattr(item, 'deleted', False) for item in related):
                return False
        else:
            if not getattr(related, 'deleted', False):
                return False

    return True

def should_deprecate(obj, direction):
    """
    Returns True if the object is marked as deprecated or if all related objects
    in the specified direction ('children' or 'parents') are marked as deprecated.
    """
    if hasattr(obj, 'deprecated') and obj.deprecated:
        return True

    try:
        mapper = class_mapper(obj.__class__)
    except UnmappedClassError:
        raise ValueError(f"Object of type {type(obj)} is not a mapped SQLAlchemy model.")

    table_name = obj.__class__.__name__.lower()
    cascade_info = CASCADE_MAP.get(table_name, {})
    related_entities = cascade_info.get(direction, [])

    for rel in mapper.relationships:
        rel_name = rel.key
        rel_entity = rel.mapper.class_.__name__.lower()

        if not any(entry['entity'] == rel_entity for entry in related_entities):
            continue

        related = getattr(obj, rel_name)
        if related is None:
            continue

        if rel.uselist:
            if any(not getattr(item, 'deprecated', False) for item in related):
                return False
        else:
            if not getattr(related, 'deprecated', False):
                return False

    return True

def should_curate(obj, direction):
    """
    Returns True if the object has no related objects in the specified direction
    ('children' or 'parents'). This ensures it's safe to hard-delete the object
    without breaking relationships.
    """
    try:
        mapper = class_mapper(obj.__class__)
    except UnmappedClassError:
        raise ValueError(f"Object of type {type(obj)} is not a mapped SQLAlchemy model.")

    table_name = obj.__class__.__name__.lower()
    cascade_info = CASCADE_MAP.get(table_name, {})
    related_entities = cascade_info.get(direction, [])

    for rel in mapper.relationships:
        rel_name = rel.key
        rel_entity = rel.mapper.class_.__name__.lower()

        if not any(entry['entity'] == rel_entity for entry in related_entities):
            continue

        related = getattr(obj, rel_name)
        if related is None:
            continue

        if rel.uselist:
            if len(related) > 0:
                return False
        else:
            if related is not None:
                return False

    return True

# Define cascade relationships
CASCADE_MAP = {
    'readset_file': {
        'parents': [
            {'entity': 'readset', 'conditions': {
                'delete': should_delete,
                'deprecate': should_deprecate,
                'curate': should_curate
            }},
            {'entity': 'file', 'conditions': {
                'delete': should_delete,
                'deprecate': should_deprecate,
                'curate': should_curate
            }}
        ],
        'children': []

    },
    'readset_job': {
        'parents': [
            {'entity': 'readset', 'conditions': {
                'delete': should_delete,
                'deprecate': should_deprecate,
                'curate': should_curate
            }},
            {'entity': 'job', 'conditions': {
                'delete': should_delete,
                'deprecate': should_deprecate,
                'curate': should_curate
            }}
        ],
        'children': []
    },
    'readset_operation': {
        'parents': [
            {'entity': 'readset', 'conditions': {
                'delete': should_delete,
                'deprecate': should_deprecate,
                'curate': should_curate
            }},
            {'entity': 'operation', 'conditions': {
                'delete': should_delete,
                'deprecate': should_deprecate,
                'curate': should_curate
            }}
        ],
        'children': []
    },
    'readset_metric': {
        'parents': [
            {'entity': 'readset', 'conditions': {
                'delete': should_delete,
                'deprecate': should_deprecate,
                'curate': should_curate
            }},
            {'entity': 'metric', 'conditions': {
                'delete': should_delete,
                'deprecate': should_deprecate,
                'curate': should_curate
            }}
        ],
        'children': []
    },
    'job_file': {
        'parents': [
            {'entity': 'job', 'conditions': {
                'delete': should_delete,
                'deprecate': should_deprecate,
                'curate': should_curate
            }},
            {'entity': 'file', 'conditions': {
                'delete': should_delete,
                'deprecate': should_deprecate,
                'curate': should_curate
            }}
        ],
        'children': []
    },
    'readset': {
        'parents': [
            {'entity': 'sample', 'conditions': {
                'delete': should_delete,
                'deprecate': should_deprecate,
                'curate': should_curate
            }}
        ],
        'children': [
            {'entity': 'readset_file', 'conditions': {
                'delete': should_delete,
                'deprecate': should_deprecate,
                'curate': should_curate
            }},
            {'entity': 'readset_job', 'conditions': {
                'delete': should_delete,
                'deprecate': should_deprecate,
                'curate': should_curate
            }},
            {'entity': 'readset_operation', 'conditions': {
                'delete': should_delete,
                'deprecate': should_deprecate,
                'curate': should_curate
            }},
            {'entity': 'readset_metric', 'conditions': {
                'delete': should_delete,
                'deprecate': should_deprecate,
                'curate': should_curate
            }}
        ]
    },
    'sample': {
        'parents': [
            {'entity': 'specimen', 'conditions': {
                'delete': should_delete,
                'deprecate': should_deprecate,
                'curate': should_curate
            }}
        ],
        'children': [
            {'entity': 'readset', 'conditions': {
                'delete': should_delete,
                'deprecate': should_deprecate,
                'curate': should_curate
            }}
        ]
    },
    'specimen': {
        'parents': [],
        'children': [
            {'entity': 'sample', 'conditions': {
                'delete': should_delete,
                'deprecate': should_deprecate,
                'curate': should_curate
            }}
        ]
    },
    'operation': {
        'parents': [],
        'children': [
            {'entity': 'job', 'conditions': {
                'delete': should_delete,
                'deprecate': should_deprecate,
                'curate': should_curate
            }},
            {'entity': 'readset_operation', 'conditions': {
                'delete': should_delete,
                'deprecate': should_deprecate,
                'curate': should_curate
            }}
        ]
    },
    'job': {
        'parents': [
            {'entity': 'operation', 'conditions': {
                'delete': should_delete,
                'deprecate': should_deprecate,
                'curate': should_curate
            }}
        ],
        'children': [
            {'entity': 'job_file', 'conditions': {
                'delete': should_delete,
                'deprecate': should_deprecate,
                'curate': should_curate
            }},
            {'entity': 'metric', 'conditions': {
                'delete': should_delete,
                'deprecate': should_deprecate,
                'curate': should_curate
            }},
            {'entity': 'readset_job', 'conditions': {
                'delete': should_delete,
                'deprecate': should_deprecate,
                'curate': should_curate
            }}
        ]
    },
    'metric': {
        'parents': [
            {'entity': 'job', 'conditions': {
                'delete': should_delete,
                'deprecate': should_deprecate,
                'curate': should_curate
            }}
        ],
        'children': [
            {'entity': 'readset_metric', 'conditions': {
                'delete': should_delete,
                'deprecate': should_deprecate,
                'curate': should_curate
            }}
        ]
    },
    'location': {
        'parents': [
            {'entity': 'file', 'conditions': {
                'delete': should_delete,
                'deprecate': should_deprecate,
                'curate': should_curate
            }}
        ],
        'children': []
    },
    'project': {
        'parents': [],
        'children': [
            {'entity': 'specimen', 'conditions': {
                'delete': should_delete,
                'deprecate': should_deprecate,
                'curate': should_curate
            }},
            {'entity': 'operation', 'conditions': {
                'delete': should_delete,
                'deprecate': should_deprecate,
                'curate': should_curate
            }}
        ]
    },
    'file': {
        'parents': [],
        'children': [
            {'entity': 'readset_file', 'conditions': {
                'delete': should_delete,
                'deprecate': should_deprecate,
                'curate': should_curate
            }},
            {'entity': 'job_file', 'conditions': {
                'delete': should_delete,
                'deprecate': should_deprecate,
                'curate': should_curate
            }},
            {'entity': 'location', 'conditions': {
                'delete': should_delete,
                'deprecate': should_deprecate,
                'curate': should_curate
            }}
        ]
    },
    'experiment': {
        'parents': [],
        'children': [
            {'entity': 'run', 'conditions': {
                'delete': should_delete,
                'deprecate': should_deprecate,
                'curate': should_curate
            }}
        ]
    },
    'run': {
        'parents': [],
        'children': [
            {'entity': 'readset', 'conditions': {
                'delete': should_delete,
                'deprecate': should_deprecate,
                'curate': should_curate
            }}
        ]
    },
    'operation_config': {
        'parents': [],
        'children': [
            {'entity': 'operation', 'conditions': {
                'delete': should_delete,
                'deprecate': should_deprecate,
                'curate': should_curate
            }}
        ]
    },
    'reference': {
        'parents': [],
        'children': [
            {'entity': 'operation', 'conditions': {
                'delete': should_delete,
                'deprecate': should_deprecate,
                'curate': should_curate
            }}
        ]
    }
}


# Cascade Evaluation Function
def should_cascade(source, target, action):
    """
    Evaluate if an action should cascade from source to target based on the relationship.
    Supports both parent and child directions.
    """
    source_type = source.__class__.__name__.lower()
    target_type = target.__class__.__name__.lower()
    cascade_info = CASCADE_MAP.get(source_type, {})

    # Check if target is a parent
    for parent_entry in cascade_info.get('parents', []):
        if parent_entry['entity'] == target_type:
            condition_fn = parent_entry['conditions'].get(action)
            if condition_fn and not condition_fn(target, direction='parents'):
                return False

    # Check if target is a child
    for child_entry in cascade_info.get('children', []):
        if child_entry['entity'] == target_type:
            condition_fn = child_entry['conditions'].get(action)
            if condition_fn and not condition_fn(target, direction='children'):
                return False

    return True



def cascade_action(instance, table_name, session, ret, action, direction):
    """
    Cascade an action (delete, deprecate, etc.) based on the cascade map.
    Now supports cascading to both parents and children based on direction.
    """
    if table_name not in CASCADE_MAP:
        return

    cascade_info = CASCADE_MAP[table_name]

    # Cascade to parents
    if direction == 'parents':
        for parent_entry in cascade_info.get('parents', []):
            parent_name = parent_entry['entity']
            parent_instance = getattr(instance, parent_name, None)
            if parent_instance and not getattr(parent_instance, action, False):
                if should_cascade(instance, parent_instance, action):
                    setattr(parent_instance, f"{action}d", True)
                    logger.info(f"Cascading {action} from {table_name} id={instance.id} to parent {parent_name} id={parent_instance.id}")
                    ret["DB_ACTION_OUTPUT"].append(
                        f"'{parent_name.title()}' with id '{parent_instance.id}' {action}d."
                    )
                    cascade_action(parent_instance, parent_name, session, ret, action, direction)

    # Cascade to children
    elif direction == 'children':
        for child_entry in cascade_info.get('children', []):
            child_name = child_entry['entity']
            children = getattr(instance, child_name, None)
            if not children:
                continue
            if not isinstance(children, list):
                children = [children]
            for child_instance in children:
                if not getattr(child_instance, action, False):
                    if should_cascade(instance, child_instance, action):
                        setattr(child_instance, f"{action}d", True)
                        logger.info(f"Cascading {action} from {table_name} id={instance.id} to child {child_name} id={child_instance.id}")
                        ret["DB_ACTION_OUTPUT"].append(
                            f"'{child_name.title()}' with id '{child_instance.id}' {action}d."
                        )
                        cascade_action(child_instance, child_name, session, ret, action, direction)


def model_table(name):
    """
    Model class of a table, by table name (e.g. 'readset', 'sample_group') or class name (e.g. 'SampleGroup').
    Raises RequestError listing the tables otherwise.
    """
    tables = {mapper.class_.__tablename__: mapper.class_ for mapper in model.Base.registry.mappers}
    key = str(name).lower()
    for tablename, cls in tables.items():
        if key in (tablename, cls.__name__.lower()):
            return cls
    raise RequestError(f"'table' '{name}' doesn't exist: expected one of {', '.join(repr(t) for t in sorted(tables))}.")


def modifiable_table(name):
    """
    Model class of a table the modification routes may change: any table but 'project', whose rows are the projects
    the routes are under (create them with the admin routes).
    """
    the_table = model_table(name)
    if the_table is model.Project:
        raise RequestError("'project' rows can't be changed by the modification routes.")
    return the_table


def commit(session):
    """
    Save the changes, or raise RequestError: a failed save is reported as an error, never as done.
    """
    try:
        session.commit()
    except SQLAlchemyError as error:
        session.rollback()
        logger.error("Error: %s", error)
        raise RequestError(f"Nothing was changed, the database refused the changes: {getattr(error, 'orig', error)}") from error


def check_edit(the_table, column, new_value, ids, session):
    """
    Refuse edits breaking a rule the generic edit can't see: ids and links between rows (primary and foreign keys)
    don't change, sample group names are oncoanalyser group ids, unique, and a group holds samples of its specimen
    only.
    """
    if the_table is model.SampleGroup and column == "specimen_id":
        raise RequestError(
            "'sample_group' 'specimen_id' can't be edited: a group holds samples of its specimen only. "
            "Create a group for the other specimen instead."
        )
    table_column = the_table.__table__.columns.get(column)
    if table_column is None:
        raise RequestError(f"'{the_table.__tablename__}' has no column '{column}'.")
    if table_column.primary_key or table_column.foreign_keys:
        raise RequestError(
            f"'{the_table.__tablename__}' '{column}' can't be edited: it is an id or a link to another row."
        )
    if the_table is model.SampleGroup and column == "name":
        problem = group_name_problem(new_value)
        if problem:
            raise RequestError(problem)
        if len(set(ids)) > 1:
            raise RequestError(f"'sample_group' names are unique: can't give '{new_value}' to several groups.")
        existing = session.execute(
            select(model.SampleGroup).where(model.SampleGroup.name == new_value, model.SampleGroup.id.not_in(ids))
        ).scalar_one_or_none()
        if existing is not None:
            raise RequestError(f"'SampleGroup' '{new_value}' already exists.")


def edit_value(the_table, column, value):
    """
    Value to store in the_table.column: for an enum column, the member matching value (exact value,
    e.g. 'ON_HOLD' or lane '1', or member name, e.g. lane 'ONE'), raising EnumValueError otherwise.
    """
    column_type = getattr(the_table.__table__.columns.get(column), "type", None)
    enum_class = getattr(column_type, "enum_class", None)
    if enum_class is None or value is None or isinstance(value, enum_class):
        return value
    if isinstance(value, str) and value in enum_class.__members__:
        return enum_class[value]
    return to_enum(enum_class, value)


def edit(ingest_data, session, cascade_mode=None, dry_run=False):
    """
    Edition of the database based on ingested_data
    cascade_mode is not used in here and used for consisterncy with other actions.
    """
    if not isinstance(ingest_data, dict):
        ingest_data = json.loads(ingest_data)

    ret = {
        "DB_ACTION_WARNING": [],
        "DB_ACTION_OUTPUT": []
        }

    if not ingest_data[vb.MODIFICATION]:
        raise RequestError("No 'table' provided under 'modification' list, this json is malformed.")

    for table in ingest_data[vb.MODIFICATION]:
        the_table = modifiable_table(table[vb.TABLE])
        new_value = edit_value(the_table, table[vb.COLUMN], table[vb.NEW])
        if table[vb.ID]:
            check_edit(the_table, table[vb.COLUMN], new_value, table[vb.ID], session)
            for current_id in set(table[vb.ID]):
                stmt = select(the_table).where(the_table.id == current_id)
                selected_entry = session.execute(stmt).scalar_one_or_none()
                if not selected_entry:
                    raise DidNotFindError(table=table[vb.TABLE], attribute="id", query=current_id)
                old = getattr(selected_entry, table[vb.COLUMN])
                # Skip the edit if the new value is the same as the old one
                if old == new_value:
                    ret["DB_ACTION_WARNING"].append(f"Table '{table[vb.TABLE]}' with id '{current_id}' already has '{table[vb.COLUMN]}' with value '{old}'. Skipping...")
                else:
                    setattr(selected_entry, table[vb.COLUMN], new_value)
                    new = getattr(selected_entry, table[vb.COLUMN])
                    ret["DB_ACTION_OUTPUT"].append(f"Table '{table[vb.TABLE]}' edited: column '{table[vb.COLUMN]}' with id '{current_id}' changes from '{old}' to '{new}'.")
        else:
            ret["DB_ACTION_WARNING"].append(f"No IDs provided for table '{table[vb.TABLE]}'. Skipping...")

    if not dry_run:
        commit(session)

    # If no warning
    if not ret["DB_ACTION_WARNING"]:
        ret.pop("DB_ACTION_WARNING")

    return ret

def delete(ingest_data, session, cascade_mode=None, dry_run=False):
    """Soft delete data with cascading logic based on relationships."""
    if not isinstance(ingest_data, dict):
        ingest_data = json.loads(ingest_data)

    ret = {
        "DB_ACTION_WARNING": [],
        "DB_ACTION_OUTPUT": []
    }

    if not ingest_data[vb.MODIFICATION]:
        raise RequestError("No 'table' provided under 'modification' list, this json is malformed.")

    for table in ingest_data[vb.MODIFICATION]:
        the_table = modifiable_table(table[vb.TABLE])
        table_name = the_table.__name__
        if table[vb.ID]:
            for current_id in set(table[vb.ID]):
                stmt = select(the_table).where(the_table.id == current_id)
                selected_entry = session.execute(stmt).scalar_one_or_none()
                if not selected_entry:
                    raise DidNotFindError(table=table_name, attribute="id", query=current_id)
                if selected_entry.deleted:
                    ret["DB_ACTION_WARNING"].append(f"'{table_name}' with id '{current_id}' already deleted. Skipping...")
                else:
                    selected_entry.deleted = True
                    ret["DB_ACTION_OUTPUT"].append(f"'{table_name}' with id '{current_id}' deleted.")

                    # Handle cascading based on cascade_mode
                    if cascade_mode in ('cascade', 'cascade_down'):
                        cascade_action(selected_entry, table_name.lower(), session, ret, 'delete', direction='children')
                    if cascade_mode in ('cascade', 'cascade_up'):
                        cascade_action(selected_entry, table_name.lower(), session, ret, 'delete', direction='parents')
        else:
            ret["DB_ACTION_WARNING"].append(f"No IDs provided for table '{table[vb.TABLE]}'. Skipping...")

    if not dry_run:
        commit(session)

    if not ret["DB_ACTION_WARNING"]:
        ret.pop("DB_ACTION_WARNING")

    return ret


def undelete(ingest_data, session, cascade_mode=None, dry_run=False):
    """Revert deletion of data in the database with cascading logic."""
    if not isinstance(ingest_data, dict):
        ingest_data = json.loads(ingest_data)

    ret = {
        "DB_ACTION_WARNING": [],
        "DB_ACTION_OUTPUT": []
    }

    if not ingest_data[vb.MODIFICATION]:
        raise RequestError("No 'table' provided under 'modification' list, this json is malformed.")

    for table in ingest_data[vb.MODIFICATION]:
        the_table = modifiable_table(table[vb.TABLE])
        table_name = the_table.__name__
        if table[vb.ID]:
            for current_id in set(table[vb.ID]):
                stmt = select(the_table).where(the_table.id == current_id)
                selected_entry = session.execute(stmt).scalar_one_or_none()
                if not selected_entry:
                    raise DidNotFindError(table=table_name, attribute="id", query=current_id)
                if not selected_entry.deleted:
                    ret["DB_ACTION_WARNING"].append(f"'{table_name}' with id '{current_id}' already undeleted. Skipping...")
                else:
                    selected_entry.deleted = False
                    ret["DB_ACTION_OUTPUT"].append(f"'{table_name}' with id '{current_id}' undeleted.")

                    # Handle cascading based on cascade_mode
                    if cascade_mode in ('cascade', 'cascade_down'):
                        cascade_undelete(selected_entry, table_name.lower(), session, ret, direction='children')
                    if cascade_mode in ('cascade', 'cascade_up'):
                        cascade_undelete(selected_entry, table_name.lower(), session, ret, direction='parents')
        else:
            ret["DB_ACTION_WARNING"].append(f"No IDs provided for table '{table[vb.TABLE]}'. Skipping...")

    if not dry_run:
        commit(session)

    if not ret["DB_ACTION_WARNING"]:
        ret.pop("DB_ACTION_WARNING")

    return ret

def cascade_undelete(instance, table_name, session, ret, direction):
    """
    Cascade undelete based on the cascade map.
    Supports both upward (to parents) and downward (to children) propagation.
    """
    if table_name not in CASCADE_MAP:
        return

    cascade_info = CASCADE_MAP[table_name]

    # Undelete parents
    if direction == 'parents':
        for parent_entry in cascade_info.get('parents', []):
            parent_name = parent_entry['entity']
            parent_instance = getattr(instance, parent_name, None)
            if parent_instance and getattr(parent_instance, 'deleted', False):
                parent_instance.deleted = False
                logger.info(f"Cascading undelete from {table_name} id={instance.id} to parent {parent_name} id={parent_instance.id}")
                ret["DB_ACTION_OUTPUT"].append(
                    f"'{parent_name.title()}' with id '{parent_instance.id}' undeleted."
                )
                cascade_undelete(parent_instance, parent_name, session, ret, direction='parents')

    # Undelete children
    elif direction == 'children':
        for child_entry in cascade_info.get('children', []):
            child_name = child_entry['entity']
            children = getattr(instance, child_name, None)
            if not children:
                continue
            if not isinstance(children, list):
                children = [children]
            for child_instance in children:
                if getattr(child_instance, 'deleted', False):
                    child_instance.deleted = False
                    logger.info(f"Cascading undelete from {table_name} id={instance.id} to child {child_name} id={child_instance.id}")
                    ret["DB_ACTION_OUTPUT"].append(
                        f"'{child_name.title()}' with id '{child_instance.id}' undeleted."
                    )
                    cascade_undelete(child_instance, child_name, session, ret, direction='children')



def deprecate(ingest_data, session, cascade_mode=None, dry_run=False):
    """deprecation of the database based on ingested_data"""
    if not isinstance(ingest_data, dict):
        ingest_data = json.loads(ingest_data)

    ret = {
        "DB_ACTION_WARNING": [],
        "DB_ACTION_OUTPUT": []
        }

    if not ingest_data[vb.MODIFICATION]:
        raise RequestError("No 'table' provided under 'modification' list, this json is malformed.")

    for table in ingest_data[vb.MODIFICATION]:
        the_table = modifiable_table(table[vb.TABLE])
        table_name = the_table.__name__
        if table[vb.ID]:
            for current_id in set(table[vb.ID]):
                stmt = select(the_table).where(the_table.id == current_id)
                selected_entry = session.execute(stmt).scalar_one_or_none()
                if not selected_entry:
                    raise DidNotFindError(table=table_name, attribute="id", query=current_id)
                if selected_entry.deprecated:
                    ret["DB_ACTION_WARNING"].append(f"'{table_name}' with id '{current_id}' already deprecated. Skipping...")
                else:
                    selected_entry.deprecated = True
                    ret["DB_ACTION_OUTPUT"].append(f"'{table_name}' with id '{current_id}' deprecated.")

                    # Handle cascading based on cascade_mode
                    if cascade_mode in ('cascade', 'cascade_down'):
                        cascade_action(selected_entry, table_name.lower(), session, ret, 'deprecate', direction='children')
                    if cascade_mode in ('cascade', 'cascade_up'):
                        cascade_action(selected_entry, table_name.lower(), session, ret, 'deprecate', direction='parents')
        else:
            ret["DB_ACTION_WARNING"].append(f"No IDs provided for table '{table[vb.TABLE]}'. Skipping...")

    if not dry_run:
        commit(session)

    # If no warning
    if not ret["DB_ACTION_WARNING"]:
        ret.pop("DB_ACTION_WARNING")

    return ret


def undeprecate(ingest_data, session, cascade_mode=None, dry_run=False):
    """revert deprecation of the database based on ingested_data"""
    if not isinstance(ingest_data, dict):
        ingest_data = json.loads(ingest_data)

    ret = {
        "DB_ACTION_WARNING": [],
        "DB_ACTION_OUTPUT": []
        }

    if not ingest_data[vb.MODIFICATION]:
        raise RequestError("No 'table' provided under 'modification' list, this json is malformed.")

    for table in ingest_data[vb.MODIFICATION]:
        the_table = modifiable_table(table[vb.TABLE])
        table_name = the_table.__name__
        if table[vb.ID]:
            for current_id in set(table[vb.ID]):
                stmt = select(the_table).where(the_table.id == current_id)
                selected_entry = session.execute(stmt).scalar_one_or_none()
                if not selected_entry:
                    raise DidNotFindError(table=table_name, attribute="id", query=current_id)
                if selected_entry.deprecated is False:
                    ret["DB_ACTION_WARNING"].append(f"'{table_name}' with id '{current_id}' already undeprecated. Skipping...")
                else:
                    selected_entry.deprecated = False
                    ret["DB_ACTION_OUTPUT"].append(f"'{table_name}' with id '{current_id}' undeprecated.")

                    # Handle cascading based on cascade_mode
                    if cascade_mode in ('cascade', 'cascade_down'):
                        cascade_undeprecate(selected_entry, table_name.lower(), session, ret, direction='children')
                    if cascade_mode in ('cascade', 'cascade_up'):
                        cascade_undeprecate(selected_entry, table_name.lower(), session, ret, direction='parents')
        else:
            ret["DB_ACTION_WARNING"].append(f"No IDs provided for table '{table[vb.TABLE]}'. Skipping...")

    if not dry_run:
        commit(session)

    # If no warning
    if not ret["DB_ACTION_WARNING"]:
        ret.pop("DB_ACTION_WARNING")

    return ret

def cascade_undeprecate(instance, table_name, session, ret, direction):
    """
    Cascade undeprecate based on the cascade map.
    Supports both upward (to parents) and downward (to children) propagation.
    """
    if table_name not in CASCADE_MAP:
        return

    cascade_info = CASCADE_MAP[table_name]

    # Undeprecate parents
    if direction == 'parents':
        for parent_entry in cascade_info.get('parents', []):
            parent_name = parent_entry['entity']
            parent_instance = getattr(instance, parent_name, None)
            if parent_instance and getattr(parent_instance, 'deprecated', False):
                parent_instance.deprecated = False
                logger.info(f"Cascading undeprecate from {table_name} id={instance.id} to parent {parent_name} id={parent_instance.id}")
                ret["DB_ACTION_OUTPUT"].append(
                    f"'{parent_name.title()}' with id '{parent_instance.id}' undeprecated."
                )
                cascade_undeprecate(parent_instance, parent_name, session, ret, direction='parents')

    # Undeprecate children
    elif direction == 'children':
        for child_entry in cascade_info.get('children', []):
            child_name = child_entry['entity']
            children = getattr(instance, child_name, None)
            if not children:
                continue
            if not isinstance(children, list):
                children = [children]
            for child_instance in children:
                if getattr(child_instance, 'deprecated', False):
                    child_instance.deprecated = False
                    logger.info(f"Cascading undeprecate from {table_name} id={instance.id} to child {child_name} id={child_instance.id}")
                    ret["DB_ACTION_OUTPUT"].append(
                        f"'{child_name.title()}' with id '{child_instance.id}' undeprecated."
                    )
                    cascade_undeprecate(child_instance, child_name, session, ret, direction='children')



def curate(ingest_data, session, cascade_mode=None, dry_run=False):
    """Curate the database based on ingested_data (hard delete)."""
    if not isinstance(ingest_data, dict):
        ingest_data = json.loads(ingest_data)

    ret = {
        "DB_ACTION_WARNING": [],
        "DB_ACTION_OUTPUT": []
    }

    if not ingest_data.get(vb.MODIFICATION):
        raise RequestError("No 'table' provided under 'modification' list, this JSON is malformed.")

    for table in ingest_data[vb.MODIFICATION]:
        the_table = modifiable_table(table[vb.TABLE])
        table_name = the_table.__name__
        if table[vb.ID]:
            for current_id in set(table[vb.ID]):
                stmt = select(the_table).where(the_table.id == current_id)
                selected_entry = session.execute(stmt).scalar_one_or_none()
                if not selected_entry:
                    raise DidNotFindError(table=table_name, attribute="id", query=current_id)

                session.delete(selected_entry)
                ret["DB_ACTION_OUTPUT"].append(f"'{table_name}' with id '{current_id}' permanently deleted.")

                # Handle cascading based on cascade_mode
                if cascade_mode in ('cascade', 'cascade_down'):
                    cascade_curate(selected_entry, table_name.lower(), session, ret, direction='children')
                if cascade_mode in ('cascade', 'cascade_up'):
                    cascade_curate(selected_entry, table_name.lower(), session, ret, direction='parents')
        else:
            ret["DB_ACTION_WARNING"].append(f"No IDs provided for table '{table_name}'. Skipping...")

    if not dry_run:
        commit(session)

    if not ret["DB_ACTION_WARNING"]:
        ret.pop("DB_ACTION_WARNING")

    return ret


def cascade_curate(instance, table_name, session, ret, direction):
    """
    Cascade hard deletion (curation) based on the cascade map.
    Supports both upward (to parents) and downward (to children) propagation.
    """
    if table_name not in CASCADE_MAP:
        return

    cascade_info = CASCADE_MAP[table_name]

    # Curate parents
    if direction == 'parents':
        for parent_entry in cascade_info.get('parents', []):
            parent_name = parent_entry['entity']
            parent_instance = getattr(instance, parent_name, None)

            if parent_instance:
                if should_cascade(instance, parent_instance, action='curate'):
                    session.delete(parent_instance)
                    logger.info(f"Cascading curate from {table_name} id={instance.id} to parent {parent_name} id={parent_instance.id}")
                    ret["DB_ACTION_OUTPUT"].append(
                        f"'{parent_name.title()}' with id '{parent_instance.id}' permanently deleted."
                    )
                    cascade_curate(parent_instance, parent_name, session, ret, direction='parents')

    # Curate children
    elif direction == 'children':
        for child_entry in cascade_info.get('children', []):
            child_name = child_entry['entity']
            children = getattr(instance, child_name, None)
            if not children:
                continue
            if not isinstance(children, list):
                children = [children]
            for child_instance in children:
                if should_cascade(instance, child_instance, action='curate'):
                    session.delete(child_instance)
                    logger.info(f"Cascading curate from {table_name} id={instance.id} to child {child_name} id={child_instance.id}")
                    ret["DB_ACTION_OUTPUT"].append(
                        f"'{child_name.title()}' with id '{child_instance.id}' permanently deleted."
                    )
                    cascade_curate(child_instance, child_name, session, ret, direction='children')
