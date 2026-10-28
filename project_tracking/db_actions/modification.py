"""
Modification actions for the database.
"""
# Standard library
import logging
import json
from collections import deque
from dataclasses import dataclass

# Third-party
from sqlalchemy import inspect as sa_inspect, select
from sqlalchemy.exc import SQLAlchemyError

# Local modules
from .errors import DidNotFindError, RequestError
from .utils import to_enum
from .sample_group import group_name_problem
from .. import vocabulary as vb
from .. import model

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Edge:
    """
    A link a cascade follows: parent.children_attr lists the children, child.parents_attr the parent(s). Down, a child
    is reached when all its parents through this link are gone; up (only when up is True), a parent is reached when
    all its children through this link are gone.
    """
    parent: type
    children_attr: str
    child: type
    parents_attr: str
    up: bool = False


# Run, Experiment, OperationConfig and Reference are shared by many rows: no cascade reaches them, and the project
# rows are never modified (see modifiable_table).
EDGES = (
    Edge(model.Specimen, "samples", model.Sample, "specimen", up=True),
    Edge(model.Sample, "readsets", model.Readset, "sample", up=True),
    Edge(model.File, "locations", model.Location, "file", up=True),
    Edge(model.Specimen, "sample_groups", model.SampleGroup, "specimen"),
    Edge(model.Sample, "sample_groups", model.SampleGroup, "samples"),
    Edge(model.Readset, "files", model.File, "readsets"),
    Edge(model.Readset, "metrics", model.Metric, "readsets"),
    Edge(model.Readset, "jobs", model.Job, "readsets"),
    Edge(model.Readset, "operations", model.Operation, "readsets"),
    Edge(model.Operation, "jobs", model.Job, "operation"),
    Edge(model.Job, "metrics", model.Metric, "job"),
    Edge(model.Job, "files", model.File, "jobs"),
)


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


def _key(row):
    return type(row), row.id


def _label(row):
    return f"'{type(row).__name__}' with id '{row.id}'"


def _related(row, attr):
    """The rows row.attr links to, as a list."""
    value = getattr(row, attr)
    if value is None:
        return []
    return list(value) if isinstance(value, list) else [value]


def _directions(cascade_mode):
    return cascade_mode in ("cascade", "cascade_down"), cascade_mode in ("cascade", "cascade_up")


def modification_targets(ingest_data, session, ret):
    """
    Rows named by the 'modification' entries of ingest_data, in order and without duplicates.
    """
    if not isinstance(ingest_data, dict):
        ingest_data = json.loads(ingest_data)
    if not ingest_data.get(vb.MODIFICATION):
        raise RequestError("No 'table' provided under 'modification' list, this json is malformed.")
    targets = {}
    for table in ingest_data[vb.MODIFICATION]:
        the_table = modifiable_table(table[vb.TABLE])
        if not table.get(vb.ID):
            ret["DB_ACTION_WARNING"].append(f"No IDs provided for table '{table[vb.TABLE]}'. Skipping...")
            continue
        for current_id in table[vb.ID]:
            row = session.execute(select(the_table).where(the_table.id == current_id)).scalar_one_or_none()
            if row is None:
                raise DidNotFindError(table=the_table.__name__, attribute="id", query=current_id)
            targets.setdefault(_key(row), row)
    return list(targets.values())


def plan_removal(targets, cascade_mode, flag=None):
    """
    Rows a delete (flag 'deleted'), deprecate (flag 'deprecated') or curate (flag None) of targets reaches, following
    EDGES down and/or up from every reached row. A row is gone when this request reaches it or, for delete and
    deprecate, when it is already flagged.
    Returns the plan, {key: (row, row it was reached from or None for a target)} in order, and the parents an up
    cascade stopped at, {key: (parent, child that isn't gone)}.
    """
    down, up = _directions(cascade_mode)
    plan = {_key(row): (row, None) for row in targets}
    stopped = {}

    def gone(row):
        return _key(row) in plan or (flag is not None and getattr(row, flag))

    queue = deque(targets)
    while queue:
        row = queue.popleft()
        candidates = []
        for edge in EDGES:
            if down and isinstance(row, edge.parent):
                candidates += [(child, edge.parents_attr, False) for child in _related(row, edge.children_attr)]
            if up and edge.up and isinstance(row, edge.child):
                candidates += [(parent, edge.children_attr, True) for parent in _related(row, edge.parents_attr)]
        for candidate, attr, is_up in candidates:
            if _key(candidate) in plan:
                continue
            blocker = next((other for other in _related(candidate, attr) if not gone(other)), None)
            if blocker is None:
                plan[_key(candidate)] = (candidate, row)
                stopped.pop(_key(candidate), None)
                queue.append(candidate)
            elif is_up:
                stopped[_key(candidate)] = (candidate, blocker)
    return plan, stopped


def plan_restore(targets, cascade_mode, flag):
    """
    Rows an undelete (flag 'deleted') or undeprecate (flag 'deprecated') of targets reaches: down, their flagged
    children, one live parent being enough; up the EDGES marked up, their flagged parents, a live row needing live
    parents. Each direction is followed from the targets on its own, so restoring a row up doesn't restore its
    siblings. Returns {key: (row, row it was reached from or None for a target)} in order.
    """
    plan = {_key(row): (row, None) for row in targets}
    for is_up, followed in zip((False, True), _directions(cascade_mode)):
        if not followed:
            continue
        queue = deque(targets)
        while queue:
            row = queue.popleft()
            for edge in EDGES:
                if not is_up and isinstance(row, edge.parent):
                    related = _related(row, edge.children_attr)
                elif is_up and edge.up and isinstance(row, edge.child):
                    related = _related(row, edge.parents_attr)
                else:
                    continue
                for other in related:
                    if _key(other) not in plan and getattr(other, flag):
                        plan[_key(other)] = (other, row)
                        queue.append(other)
    return plan


def finish(ret, session, dry_run):
    """
    Save the changes (or not, for a dry run) and drop an empty warning list.
    """
    if dry_run:
        ret["DB_ACTION_WARNING"].append("Dry run: nothing was saved.")
    else:
        commit(session)
    if not ret["DB_ACTION_WARNING"]:
        ret.pop("DB_ACTION_WARNING")
    return ret


def change_flag(ingest_data, session, cascade_mode, dry_run, flag, value):
    """
    Set flag ('deleted' or 'deprecated') to value on the given rows and the rows their cascade reaches.
    """
    ret = {"DB_ACTION_WARNING": [], "DB_ACTION_OUTPUT": []}
    verb = ("" if value else "un") + flag
    targets = modification_targets(ingest_data, session, ret)
    if value:
        plan, stopped = plan_removal(targets, cascade_mode, flag)
    else:
        plan, stopped = plan_restore(targets, cascade_mode, flag), {}
    for row, source in plan.values():
        if getattr(row, flag) == value:
            if source is None:
                ret["DB_ACTION_WARNING"].append(f"{_label(row)} already {verb}.")
            continue
        setattr(row, flag, value)
        cascaded = f" (cascade from {_label(source)})" if source is not None else ""
        ret["DB_ACTION_OUTPUT"].append(f"{_label(row)} {verb}{cascaded}.")
    for key, (parent, blocker) in stopped.items():
        if key not in plan:
            ret["DB_ACTION_WARNING"].append(f"{_label(parent)} not {verb}: {_label(blocker)} isn't.")
    return finish(ret, session, dry_run)


def delete(ingest_data, session, cascade_mode=None, dry_run=False):
    """
    Soft delete the given rows and, with a cascade mode, the rows they reach (see plan_removal).
    """
    return change_flag(ingest_data, session, cascade_mode, dry_run, "deleted", True)


def undelete(ingest_data, session, cascade_mode=None, dry_run=False):
    """
    Undo the delete of the given rows and, with a cascade mode, of the rows they reach (see plan_restore).
    """
    return change_flag(ingest_data, session, cascade_mode, dry_run, "deleted", False)


def deprecate(ingest_data, session, cascade_mode=None, dry_run=False):
    """
    Deprecate the given rows and, with a cascade mode, the rows they reach (see plan_removal).
    """
    return change_flag(ingest_data, session, cascade_mode, dry_run, "deprecated", True)


def undeprecate(ingest_data, session, cascade_mode=None, dry_run=False):
    """
    Undo the deprecation of the given rows and, with a cascade mode, of the rows they reach (see plan_restore).
    """
    return change_flag(ingest_data, session, cascade_mode, dry_run, "deprecated", False)


def curate(ingest_data, session, cascade_mode=None, dry_run=False):
    """
    Remove for good the given rows and, with a cascade mode, the rows they reach (see plan_removal). Refused when the
    database would remove more (rows the model deletes with their parent, e.g. the readsets of a sample or a run):
    curate only removes the rows it reports.
    """
    ret = {"DB_ACTION_WARNING": [], "DB_ACTION_OUTPUT": []}
    plan, _ = plan_removal(modification_targets(ingest_data, session, ret), cascade_mode)
    implied = {}
    for row, _ in plan.values():
        for other, *_ in sa_inspect(type(row)).cascade_iterator("delete", sa_inspect(row)):
            if _key(other) not in plan:
                implied.setdefault(_key(other), other)
    if implied:
        listed = ", ".join(_label(row) for row in list(implied.values())[:10])
        more = f" and {len(implied) - 10} more" if len(implied) > 10 else ""
        raise RequestError(
            f"Nothing was curated: it would also remove {listed}{more}, which the request doesn't reach. "
            "Name them in the request, or use a cascade mode that reaches them (cascade_down or cascade)."
        )
    for row, source in plan.values():
        session.delete(row)
        cascaded = f" (cascade from {_label(source)})" if source is not None else ""
        ret["DB_ACTION_OUTPUT"].append(f"{_label(row)} permanently deleted{cascaded}.")
    return finish(ret, session, dry_run)
