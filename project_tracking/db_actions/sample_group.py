"""
Groups of samples analysed together, e.g. a tumour/normal duo or a trio with the tumour RNA
"""
# Standard library
import logging
import re

# Third-party modules
from sqlalchemy import select

# Local modules
from .errors import RequestError
from .. import vocabulary as vb
from ..model import Specimen, Sample, SampleGroup

logger = logging.getLogger(__name__)

# Group names become oncoanalyser group_id and output folder names
GROUP_NAME_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$")
GROUP_NAME_RULE = "letters, digits, '_', '.' and '-', starting with a letter or a digit (128 characters max)"


def group_name_problem(name):
    """
    Why name can't be a group name, or None.
    """
    if not isinstance(name, str) or not GROUP_NAME_PATTERN.match(name):
        return f"'sample_group_name' {name!r} is invalid: expected {GROUP_NAME_RULE}."
    return None


def _group_by_name(session, name):
    return session.execute(select(SampleGroup).where(SampleGroup.name == name)).scalar_one_or_none()


def _group_state_problem(group):
    for state in ("deleted", "deprecated"):
        if getattr(group, state):
            return f"'SampleGroup' '{group.name}' is {state}: undo it or use another name."
    return None


def check_sample_groups(ingest_data, session):
    """
    Normalize every sample_group_name of a run_processing payload to a list in place and check, before anything is
    written, that names are valid and that every group only holds samples of one specimen. Raises RequestError
    listing all problems.
    """
    invalid = []
    group_specimens = {}
    for specimen_json in ingest_data[vb.SPECIMEN]:
        for sample_json in specimen_json[vb.SAMPLE]:
            names = sample_json.get(vb.SAMPLE_GROUP_NAME)
            if names is None:
                continue
            if isinstance(names, str):
                names = [names]
            if not isinstance(names, list):
                invalid.append(f"'Sample' '{sample_json[vb.SAMPLE_NAME]}' has an invalid 'sample_group_name' {names!r}: expected a name or a list of names.")
                continue
            for name in names:
                problem = group_name_problem(name)
                if problem:
                    invalid.append(f"'Sample' '{sample_json[vb.SAMPLE_NAME]}': {problem}")
                else:
                    group_specimens.setdefault(name, {}).setdefault(specimen_json[vb.SPECIMEN_NAME], []).append(sample_json[vb.SAMPLE_NAME])
            sample_json[vb.SAMPLE_GROUP_NAME] = list(dict.fromkeys(names))

    for name, specimens in group_specimens.items():
        group = _group_by_name(session, name)
        if group is not None:
            problem = _group_state_problem(group)
            if problem:
                invalid.append(problem)
                continue
            if group.specimen.name not in specimens or len(specimens) > 1:
                others = {specimen: samples for specimen, samples in specimens.items() if specimen != group.specimen.name}
                invalid.append(
                    f"'SampleGroup' '{name}' belongs to 'Specimen' '{group.specimen.name}': "
                    f"{_samples_by_specimen(others)} can't join it."
                )
        elif len(specimens) > 1:
            invalid.append(f"'SampleGroup' '{name}' would hold samples of several specimens: {_samples_by_specimen(specimens)}.")
    if invalid:
        raise RequestError(message=invalid)


def _samples_by_specimen(specimens):
    return ", ".join(
        f"{', '.join(repr(sample) for sample in samples)} of 'Specimen' '{specimen}'"
        for specimen, samples in specimens.items()
    )


def add_samples_to_groups(samples_groups, session):
    """
    Add each sample to its named groups, creating the missing ones; samples_groups is a list of (sample, names).
    Returns a warning per group created, so a misspelled name doesn't go unnoticed.
    """
    created = {}
    for sample, names in samples_groups:
        for name in names:
            group = created.get(name) or _group_by_name(session, name)
            if group is None:
                group = SampleGroup(name=name, specimen=sample.specimen)
                session.add(group)
                created[name] = group
            if sample not in group.samples:
                group.samples.append(sample)
    session.flush()
    return [
        f"Created 'SampleGroup' '{name}' for 'Specimen' '{group.specimen.name}' with 'Sample' "
        f"{', '.join(repr(s.name) for s in group.samples)}."
        for name, group in created.items()
    ]


def group_to_json(group):
    """
    A group with its specimen and sample names.
    """
    return {
        "id": group.id,
        "name": group.name,
        "specimen_name": group.specimen.name,
        "sample_name": sorted(s.name for s in group.live_samples),
        "deprecated": group.deprecated,
        "deleted": group.deleted,
    }


def sample_groups(project_id, digest_data, session):
    """
    Groups of the project, optionally only those of the specimens in specimen_name, named in sample_group_name or
    holding a sample named in sample_name.
    """
    stmt = select(SampleGroup).join(SampleGroup.specimen).where(Specimen.project_id == project_id).order_by(SampleGroup.id)
    digest_data = digest_data or {}
    if digest_data.get(vb.SPECIMEN_NAME):
        stmt = stmt.where(Specimen.name.in_(digest_data[vb.SPECIMEN_NAME]))
    if digest_data.get(vb.SAMPLE_GROUP_NAME):
        stmt = stmt.where(SampleGroup.name.in_(digest_data[vb.SAMPLE_GROUP_NAME]))
    if digest_data.get(vb.SAMPLE_NAME):
        stmt = stmt.where(SampleGroup.samples.any(Sample.name.in_(digest_data[vb.SAMPLE_NAME])))
    groups = session.execute(stmt).scalars().all()
    return {"DB_ACTION_OUTPUT": [group_to_json(group) for group in groups]}


def edit_sample_group(project_id, ingest_data, session):
    """
    Edit the samples of one group: add samples (creating the group when it doesn't exist), remove samples.
    Expects sample_group_name and add and/or remove (sample names). A group left without samples is deleted.
    Raises RequestError listing all problems, nothing is changed then. Rename a group with the edit route.
    """
    ret = {"DB_ACTION_WARNING": [], "DB_ACTION_OUTPUT": []}
    name = ingest_data.get(vb.SAMPLE_GROUP_NAME)
    to_add = ingest_data.get(vb.SAMPLE_GROUP_ADD) or []
    to_remove = ingest_data.get(vb.SAMPLE_GROUP_REMOVE) or []
    if not (to_add or to_remove):
        raise RequestError(message=f"Nothing to do: give '{vb.SAMPLE_GROUP_ADD}' and/or '{vb.SAMPLE_GROUP_REMOVE}'.")

    invalid = []
    problem = group_name_problem(name)
    if problem:
        raise RequestError(message=problem)
    group = _group_by_name(session, name)
    if group is not None:
        if str(group.specimen.project_id) != str(project_id):
            raise RequestError(message=f"'SampleGroup' '{name}' belongs to another project.")
        problem = _group_state_problem(group)
        if problem:
            invalid.append(problem)
    elif not to_add:
        raise RequestError(message=f"'SampleGroup' '{name}' doesn't exist: give '{vb.SAMPLE_GROUP_ADD}' to create it.")

    samples = {}
    for sample_name in dict.fromkeys(to_add + to_remove):
        sample = session.execute(
            select(Sample).join(Sample.specimen).where(Sample.name == sample_name, Specimen.project_id == project_id)
        ).scalar_one_or_none()
        if sample is None:
            invalid.append(f"'Sample' '{sample_name}' doesn't exist in this project.")
        else:
            samples[sample_name] = sample

    specimen = group.specimen if group is not None else None
    for sample_name in to_add:
        sample = samples.get(sample_name)
        if sample is None:
            continue
        if specimen is None:
            specimen = sample.specimen
        if sample.specimen != specimen:
            invalid.append(f"'Sample' '{sample_name}' of 'Specimen' '{sample.specimen.name}' can't join 'SampleGroup' '{name}' of 'Specimen' '{specimen.name}'.")
    for sample_name in to_remove:
        sample = samples.get(sample_name)
        if sample is not None and (group is None or sample not in group.samples):
            invalid.append(f"'Sample' '{sample_name}' isn't in 'SampleGroup' '{name}'.")
    if invalid:
        raise RequestError(message=invalid)

    if group is None:
        group = SampleGroup(name=name, specimen=specimen)
        session.add(group)
        ret["DB_ACTION_WARNING"].append(f"Created 'SampleGroup' '{name}' for 'Specimen' '{specimen.name}'.")
    for sample_name in to_add:
        if samples[sample_name] not in group.samples:
            group.samples.append(samples[sample_name])
    for sample_name in to_remove:
        group.samples.remove(samples[sample_name])

    if not group.samples:
        session.delete(group)
        ret["DB_ACTION_WARNING"].append(f"'SampleGroup' '{group.name}' has no sample left: deleted.")
    else:
        session.flush()
        ret["DB_ACTION_OUTPUT"].append(group_to_json(group))
    session.commit()

    if not ret["DB_ACTION_WARNING"]:
        ret.pop("DB_ACTION_WARNING")
    return ret
