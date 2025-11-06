"""
This module contains functions to interact with the database for project tracking via route.
"""
# Standard library
import logging

# Third-party
from sqlalchemy import select, and_
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import selectinload

# Local modules
from ..model import (
    Project,
    Specimen,
    Sample,
    Readset,
    Experiment,
    Operation,
    Job,
    Metric,
    File,
    StateEnum
    )

logger = logging.getLogger(__name__)


def projects(project_id, session, deprecated=False, deleted=False):
    """
    Fetching all projects in the database.
    If project_id is provided, fetch only that project.
    Returns a list of Project objects.
    """
    ret = {
        "DB_ACTION_WARNING": [],
        "DB_ACTION_OUTPUT": []
    }

    if isinstance(project_id, str):
        project_id = [project_id]

    stmt = (
        select(Project)
        .where(Project.deprecated.is_(deprecated), Project.deleted.is_(deleted))
        .group_by(Project.id)
    ).distinct()

    if project_id:
        if isinstance(project_id, int):
            project_id = [project_id]
        stmt = stmt.where(Project.id.in_(project_id))

    result = session.execute(stmt).scalars().all()

    if not result:
        ret["DB_ACTION_WARNING"].append(
            f"No projects found with the following criteria: "
            f"project_id={project_id}, "
            f"deprecated={deprecated}, "
            f"deleted={deleted}"
        )
    else:
        ret["DB_ACTION_OUTPUT"].extend(result)
    if not ret["DB_ACTION_WARNING"]:
        ret.pop("DB_ACTION_WARNING")

    return ret

def metrics(project_id, session, deliverable=None, specimen_id=None, sample_id=None, readset_id=None, metric_id=None, deprecated=False, deleted=False):
    """
    Fetching all metrics that are part of the project or specimen or sample or readset.
    """
    ret = {
        "DB_ACTION_WARNING": [],
        "DB_ACTION_OUTPUT": []
    }

    if isinstance(project_id, str):
        project_id = [project_id]

    stmt = (
        select(Metric)
        .options(
            selectinload(Metric.readsets)
            .selectinload(Readset.sample)
            .selectinload(Sample.specimen)
            .selectinload(Specimen.project)
        )
        .join(Metric.readsets)
        .join(Readset.sample)
        .join(Sample.specimen)
        .join(Specimen.project)
        .where(
            Metric.deprecated.is_(deprecated),
            Metric.deleted.is_(deleted),
            Project.id.in_(project_id)
        )
    ).distinct()

    if metric_id:
        if isinstance(metric_id, int):
            metric_id = [metric_id]
        stmt = stmt.where(Metric.id.in_(metric_id))
    elif specimen_id:
        if isinstance(specimen_id, int):
            specimen_id = [specimen_id]
        stmt = stmt.where(Specimen.id.in_(specimen_id))
    elif sample_id:
        if isinstance(sample_id, int):
            sample_id = [sample_id]
        stmt = stmt.where(Sample.id.in_(sample_id))
    elif readset_id:
        if isinstance(readset_id, int):
            readset_id = [readset_id]
        stmt = stmt.where(Readset.id.in_(readset_id))
    if deliverable is not None:
        # Filter metrics based on deliverable status
        stmt = stmt.where(Metric.deliverable.is_(deliverable))

    result = session.execute(stmt).scalars().all()

    if not result:
        ret["DB_ACTION_WARNING"].append(
            f"No metrics found with the following criteria: "
            f"project_id={project_id}, "
            f"deliverable={deliverable}, "
            f"specimen_id={specimen_id}, "
            f"sample_id={sample_id}, "
            f"readset_id={readset_id}, "
            f"metric_id={metric_id}, "
            f"deprecated={deprecated}, "
            f"deleted={deleted}"
        )
    else:
        ret["DB_ACTION_OUTPUT"].extend(result)
    if not ret["DB_ACTION_WARNING"]:
        ret.pop("DB_ACTION_WARNING")

    return ret


def files(project_id, session, deliverable=None, specimen_id=None, sample_id=None, readset_id=None, file_id=None, state=None, deprecated=False, deleted=False):
    """
    Fetching all files that are linked to readset.
    """
    ret = {
        "DB_ACTION_WARNING": [],
        "DB_ACTION_OUTPUT": []
    }

    if isinstance(project_id, str):
        project_id = [project_id]

    stmt = (
        select(File)
        .options(
            selectinload(File.readsets)
            .selectinload(Readset.sample)
            .selectinload(Sample.specimen)
            .selectinload(Specimen.project)
        )
        .join(File.readsets)
        .join(Readset.sample)
        .join(Sample.specimen)
        .join(Specimen.project)
        .where(
            File.deprecated.is_(deprecated),
            File.deleted.is_(deleted),
            Project.id.in_(project_id)
        )
    ).distinct()

    if file_id:
        if isinstance(file_id, int):
            file_id = [file_id]
        stmt = stmt.where(File.id.in_(file_id))
    elif specimen_id:
        if isinstance(specimen_id, int):
            specimen_id = [specimen_id]
        stmt = stmt.where(Specimen.id.in_(specimen_id))
    elif sample_id:
        if isinstance(sample_id, int):
            sample_id = [sample_id]
        stmt = stmt.where(Sample.id.in_(sample_id))
    elif readset_id:
        if isinstance(readset_id, int):
            readset_id = [readset_id]
        stmt = stmt.where(Readset.id.in_(readset_id))
    if deliverable is not None:
        # Filter files based on deliverable status
        stmt = stmt.where(File.deliverable.is_(deliverable))
    if state is not None:
        # Filter files based on state
        stmt = stmt.where(File.state == StateEnum(state))

    # logger.debug(str(stmt.compile(compile_kwargs={"literal_binds": True})))

    result = session.execute(stmt).scalars().all()

    if not result:
        ret["DB_ACTION_WARNING"].append(
            f"No files found with the following criteria: "
            f"project_id={project_id}, "
            f"state={state}, "
            f"deliverable={deliverable}, "
            f"specimen_id={specimen_id}, "
            f"sample_id={sample_id}, "
            f"readset_id={readset_id}, "
            f"file_id={file_id}, "
            f"deprecated={deprecated}, "
            f"deleted={deleted}"
        )
    else:
        ret["DB_ACTION_OUTPUT"].extend(result)
    if not ret["DB_ACTION_WARNING"]:
        ret.pop("DB_ACTION_WARNING")

    return ret


def operations(project_id, session, operation_id=None, readset_id=None, deprecated=False, deleted=False):
    """
    Fetching all operations that are linked to readset.
    """
    ret = {
        "DB_ACTION_WARNING": [],
        "DB_ACTION_OUTPUT": []
    }

    if isinstance(project_id, str):
        project_id = [project_id]

    stmt = (
        select(Operation)
        .options(
            selectinload(Operation.project),
            selectinload(Operation.readsets)
        )
        .join(Operation.project)
        .where(
            Operation.deprecated.is_(deprecated),
            Operation.deleted.is_(deleted),
            Project.id.in_(project_id)
        )
    ).distinct()

    if operation_id:
        if isinstance(operation_id, int):
            operation_id = [operation_id]
        stmt = stmt.where(Operation.id.in_(operation_id))
    elif readset_id:
        if isinstance(readset_id, int):
            readset_id = [readset_id]
        stmt = stmt.join(Operation.readsets).where(Readset.id.in_(readset_id))

    result = session.execute(stmt).scalars().all()

    if not result:
        ret["DB_ACTION_WARNING"].append(
            f"No operations found with the following criteria: "
            f"project_id={project_id}, "
            f"operation_id={operation_id}, "
            f"readset_id={readset_id}, "
            f"deprecated={deprecated}, "
            f"deleted={deleted}"
        )
    else:
        ret["DB_ACTION_OUTPUT"].extend(result)
    if not ret["DB_ACTION_WARNING"]:
        ret.pop("DB_ACTION_WARNING")

    return ret


def jobs(project_id, session, job_id=None, readset_id=None, deprecated=False, deleted=False):
    """
    Fetching all jobs that are linked to readset.
    """
    ret = {
        "DB_ACTION_WARNING": [],
        "DB_ACTION_OUTPUT": []
    }

    if isinstance(project_id, str):
        project_id = [project_id]

    stmt = (
        select(Job)
        .options(
            selectinload(Job.operation).selectinload(Operation.project)
        )
        .join(Job.operation)
        .join(Operation.project)
        .where(
            Job.deprecated.is_(deprecated),
            Job.deleted.is_(deleted),
            Project.id.in_(project_id)
        )
    ).distinct()

    if job_id:
        if isinstance(job_id, int):
            job_id = [job_id]
        stmt = stmt.where(Job.id.in_(job_id))
    elif readset_id:
        if isinstance(readset_id, int):
            readset_id = [readset_id]
        stmt = stmt.join(Job.readsets).where(Readset.id.in_(readset_id))

    result = session.execute(stmt).scalars().all()

    if not result:
        ret["DB_ACTION_WARNING"].append(
            f"No jobs found with the following criteria: "
            f"project_id={project_id}, "
            f"job_id={job_id}, "
            f"readset_id={readset_id}, "
            f"deprecated={deprecated}, "
            f"deleted={deleted}"
        )
    else:
        ret["DB_ACTION_OUTPUT"].extend(result)
    if not ret["DB_ACTION_WARNING"]:
        ret.pop("DB_ACTION_WARNING")

    return ret


def readsets(project_id, session, specimen_id=None, sample_id=None, readset_id=None, deprecated=False, deleted=False):
    """
    Fetching all readsets that are part of a given project and linked to a specimen, sample, or readset.
    """
    ret = {
        "DB_ACTION_WARNING": [],
        "DB_ACTION_OUTPUT": []
    }

    if isinstance(project_id, str):
        project_id = [project_id]

    stmt = (
        select(Readset)
        .join(Readset.sample)
        .join(Sample.specimen)
        .join(Specimen.project)
        .options(
            selectinload(Readset.sample)
            .selectinload(Sample.specimen)
            .selectinload(Specimen.project)
        )
        .where(
            Readset.deprecated.is_(deprecated),
            Readset.deleted.is_(deleted),
            Project.id.in_(project_id)
        )
    ).distinct()

    if specimen_id:
        if isinstance(specimen_id, int):
            specimen_id = [specimen_id]
        # Fetch readsets by specimen_id
        stmt = stmt.where(Specimen.id.in_(specimen_id))
    elif sample_id:
        if isinstance(sample_id, int):
            sample_id = [sample_id]
        # Fetch readsets by sample_id
        stmt = stmt.where(Sample.id.in_(sample_id))
    elif readset_id:
        if isinstance(readset_id, int):
            readset_id = [readset_id]
        # Fetch readsets by readset_id
        stmt = stmt.where(Readset.id.in_(readset_id))

    result = session.execute(stmt).scalars().all()

    if not result:
        ret["DB_ACTION_WARNING"].append(
            f"No readsets found with the following criteria: "
            f"project_id={project_id}, "
            f"specimen_id={specimen_id}, "
            f"sample_id={sample_id}, "
            f"readset_id={readset_id}, "
            f"deprecated={deprecated}, "
            f"deleted={deleted}"
        )
    else:
        ret["DB_ACTION_OUTPUT"].extend(result)
    if not ret["DB_ACTION_WARNING"]:
        ret.pop("DB_ACTION_WARNING")

    return ret


def specimens(project_id, session, specimen_id=None, sample_id=None, readset_id=None, deprecated=False, deleted=False):
    """
    Fetching all specimens in the database that are part of a given project and linked to a specimen, sample, or readset.
    """
    ret = {
        "DB_ACTION_WARNING": [],
        "DB_ACTION_OUTPUT": []
    }

    if isinstance(project_id, str):
        project_id = [project_id]

    stmt = (
        select(Specimen)
        .where(Specimen.deprecated.is_(deprecated), Specimen.deleted.is_(deleted))
        .join(Specimen.project)
        .where(Project.id.in_(project_id))
        .group_by(Specimen.id)
    ).distinct()

    if specimen_id:
        if isinstance(specimen_id, int):
            specimen_id = [specimen_id]
        # Fetch specimens by specimen_id
        stmt = stmt.where(Specimen.id.in_(specimen_id))
    elif sample_id:
        if isinstance(sample_id, int):
            sample_id = [sample_id]
        # Fetch specimens by sample_id
        stmt = stmt.join(Specimen.samples).where(Sample.id.in_(sample_id))
    elif readset_id:
        if isinstance(readset_id, int):
            readset_id = [readset_id]
        # Fetch specimens by readset_id
        stmt = stmt.join(Specimen.samples).join(Sample.readsets).where(Readset.id.in_(readset_id))

    result = session.execute(stmt).scalars().all()

    if not result:
        ret["DB_ACTION_WARNING"].append(
            f"No specimens found with the following criteria: "
            f"project_id={project_id}, "
            f"specimen_id={specimen_id}, "
            f"sample_id={sample_id}, "
            f"readset_id={readset_id}, "
            f"deprecated={deprecated}, "
            f"deleted={deleted}"
        )
    else:
        ret["DB_ACTION_OUTPUT"].extend(result)
    if not ret["DB_ACTION_WARNING"]:
        ret.pop("DB_ACTION_WARNING")

    return ret


def samples(project_id, session, specimen_id=None, sample_id=None, readset_id=None, tumour=None, deprecated=False, deleted=False):
    """
    Fetching all projects in database still need to check if sample are part of project when both are provided
    """
    ret = {
        "DB_ACTION_WARNING": [],
        "DB_ACTION_OUTPUT": []
    }

    if isinstance(project_id, str):
        project_id = [project_id]

    stmt = (
        select(Sample)
        .where(Sample.deprecated.is_(deprecated), Sample.deleted.is_(deleted))
        .join(Sample.specimen)
        .join(Specimen.project)
        .where(Project.id.in_(project_id))
        .group_by(Sample.id)
    ).distinct()

    if tumour is not None:
        # Filter samples based on tumour status
        stmt = stmt.where(Sample.tumour.is_(tumour))

    if specimen_id:
        if isinstance(specimen_id, int):
            specimen_id = [specimen_id]
        # Fetch samples by specimen_id
        stmt = stmt.where(Specimen.id.in_(specimen_id))
    elif sample_id:
        if isinstance(sample_id, int):
            sample_id = [sample_id]
        # Fetch samples by sample_id
        stmt = stmt.where(Sample.id.in_(sample_id))
    elif readset_id:
        if isinstance(readset_id, int):
            readset_id = [readset_id]
        # Fetch samples by readset_id
        stmt = stmt.join(Sample.readsets).where(Readset.id.in_(readset_id))

    result = session.execute(stmt).scalars().all()

    if not result:
        ret["DB_ACTION_WARNING"].append(
            f"No samples found with the following criteria: "
            f"project_id={project_id}, "
            f"specimen_id={specimen_id}, "
            f"sample_id={sample_id}, "
            f"readset_id={readset_id}, "
            f"tumour={tumour}, "
            f"deprecated={deprecated}, "
            f"deleted={deleted}"
        )
    else:
        ret["DB_ACTION_OUTPUT"].extend(result)
    if not ret["DB_ACTION_WARNING"]:
        ret.pop("DB_ACTION_WARNING")

    return ret


def samples_pair(project_id, session, pair: bool, specimen_id=None, deprecated=False, deleted=False):
    """
    Fetching samples that are part of a pair based on specimen ID.
    If pair is True, returns samples where experiment.nucleic_acid_type is 'DNA' and specimen_id is provided.
    If pair is False, returns samples that do not have a pair.
    """
    ret = {
        "DB_ACTION_WARNING": [],
        "DB_ACTION_OUTPUT": []
    }

    if isinstance(project_id, str):
        project_id = [project_id]

    stmt = (
        select(Sample)
        .where(Sample.deprecated.is_(deprecated), Sample.deleted.is_(deleted))
        .join(Sample.readsets)
        .join(Readset.experiment)
        .where(Experiment.nucleic_acid_type == 'DNA')
        .join(Sample.specimen)
        .join(Specimen.project)
        .where(Project.id.in_(project_id))
        .group_by(Sample.id)
    ).distinct()

    if specimen_id:
        if isinstance(specimen_id, int):
            specimen_id = [specimen_id]
        stmt = stmt.where(Specimen.id.in_(specimen_id))

    result = session.execute(stmt).scalars().all()

    if not result:
        ret["DB_ACTION_WARNING"].append(
            f"No samples found with the following criteria: "
            f"project_id={project_id}, "
            f"pair={pair}, "
            f"specimen_id={specimen_id}, "
            f"deprecated={deprecated}, "
            f"deleted={deleted}"
        )
    else:
        ret["DB_ACTION_OUTPUT"].extend(result)
        if not ret["DB_ACTION_WARNING"]:
            ret.pop("DB_ACTION_WARNING")
        if pair and len(result) == 2:
            return ret
        if not pair and len(result) != 2:
            return ret

    return ret


def create_project(project_name, session, ext_id=None, ext_src=None):
    """
    Creating new project
    Returns project even if it already exist
    """

    ret = {
        "DB_ACTION_WARNING": [],
        "DB_ACTION_OUTPUT": []
    }

    # Check if the project already exists
    stmt = select(Project).filter_by(name=project_name)
    result = session.scalars(stmt).first()

    if not result:
        # Create a new project if it doesn't exist
        project = Project(name=project_name, ext_id=ext_id, ext_src=ext_src)
        session.add(project)

        try:
            session.commit()
        except SQLAlchemyError as error:
            logger.warning(f"Could not commit {project_name}: {error}")
            ret["DB_ACTION_WARNING"].append(f"Could not commit {project_name}: {error}")
            session.rollback()

        # Re-query to get the project newly created
        result = session.scalars(stmt).first()

    ret["DB_ACTION_OUTPUT"].append(result)

    if not ret["DB_ACTION_WARNING"]:
        ret.pop("DB_ACTION_WARNING")

    return ret
