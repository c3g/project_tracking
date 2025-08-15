"""
Digesting data from the database
"""
# Standard library
import logging

# Third-party modules
from sqlalchemy import select, or_
from sqlalchemy.orm import selectinload

# Local modules
from .errors import DidNotFindError, RequestError, EnumValueError
from .utils import name_to_id
from .. import vocabulary as vb
from ..model import (
    NucleicAcidTypeEnum,
    StateEnum,
    Project,
    Specimen,
    Sample,
    Experiment,
    Run,
    Readset,
    Operation,
    Job,
    File,
    Location
    )

logger = logging.getLogger(__name__)


def endpoint_exists(session, endpoint):
    """
    Check if a location endpoint exists in the database.
    """
    stmt = select(Location).where(Location.endpoint == endpoint)
    result = session.execute(stmt).first()
    if result is None:
        raise RequestError(message=f"'endpoint' '{endpoint}' doesn't exist in the database.")


def nucleic_acid_type_exists(nucleic_acid_type):
    """
    Check if a nucleic acid type exists in the database.
    """
    if not nucleic_acid_type:
        raise RequestError(argument="experiment_nucleic_acid_type")
    try:
        nucleic_acid_type =  NucleicAcidTypeEnum(nucleic_acid_type)
    except ValueError:
        raise EnumValueError(value=nucleic_acid_type, enum=NucleicAcidTypeEnum)
    return nucleic_acid_type


def select_samples_from_specimens(session, ret, digest_data, nucleic_acid_type, project_id):
    """
    Returning Samples Objects based on requested specimens in digest_data
    """
    specimen_filters = []

    if vb.SPECIMEN_NAME in digest_data:
        specimen_filters.append(Specimen.name.in_(digest_data[vb.SPECIMEN_NAME]))
    if vb.SPECIMEN_ID in digest_data:
        specimen_filters.append(Specimen.id.in_(digest_data[vb.SPECIMEN_ID]))

    if not specimen_filters:
        return []

    stmt = (
        select(Specimen)
        .where(
            or_(*specimen_filters),
            Specimen.project_id == project_id
        )
        .options(
            selectinload(Specimen.samples)
            .selectinload(Sample.readsets)
            .selectinload(Readset.experiment)
        )
    )

    specimens = session.execute(stmt).scalars().all()
    samples = []

    for specimen in specimens:
        for sample in specimen.samples:
            readsets = sample.readsets
            if readsets:
                first_readset = readsets[0]
                if first_readset.experiment.nucleic_acid_type == nucleic_acid_type and not sample.deprecated and not sample.deleted:
                    samples.append(sample)
                else:
                    ret["DB_ACTION_WARNING"].append(
                        f"'Sample' with 'name' '{sample.name}' only exists with 'nucleic_acid_type' '{first_readset.experiment.nucleic_acid_type.value}' on database. Skipping..."
                    )
            else:
                ret["DB_ACTION_WARNING"].append(
                    f"'Sample' with 'name' '{sample.name}' has no associated readsets. Skipping..."
                )

    return samples


def select_samples_from_samples(session, ret, digest_data, nucleic_acid_type, project_id):
    """
    Returning Sample objects based on requested samples in digest_data
    """
    sample_filters = []

    if vb.SAMPLE_NAME in digest_data:
        sample_filters.append(Sample.name.in_(digest_data[vb.SAMPLE_NAME]))
    if vb.SAMPLE_ID in digest_data:
        sample_filters.append(Sample.id.in_(digest_data[vb.SAMPLE_ID]))

    if not sample_filters:
        return []

    stmt = (
        select(Sample)
        .join(Sample.specimen)
        .where(
            or_(*sample_filters),
            Specimen.project_id == project_id
        )
        .options(
            selectinload(Sample.readsets)
            .selectinload(Readset.experiment)
        )
    )

    samples = session.execute(stmt).scalars().all()
    filtered = []

    for sample in samples:
        readsets = sample.readsets
        if readsets:
            first_readset = readsets[0]
            if first_readset.experiment.nucleic_acid_type == nucleic_acid_type and not sample.deprecated and not sample.deleted:
                filtered.append(sample)
            else:
                ret["DB_ACTION_WARNING"].append(
                    f"'Sample' with 'name' '{sample.name}' only exists with 'nucleic_acid_type' '{first_readset.experiment.nucleic_acid_type.value}' on database. Skipping..."
                )
        else:
            ret["DB_ACTION_WARNING"].append(
                f"'Sample' with 'name' '{sample.name}' has no associated readsets. Skipping..."
            )

    return filtered


def select_samples_from_readsets(session, ret, digest_data, nucleic_acid_type, project_id):
    """
    Returning Sample objects based on requested readsets in digest_data
    """
    readset_filters = []

    if vb.READSET_NAME in digest_data:
        readset_filters.append(Readset.name.in_(digest_data[vb.READSET_NAME]))
    if vb.READSET_ID in digest_data:
        readset_filters.append(Readset.id.in_(digest_data[vb.READSET_ID]))

    if not readset_filters:
        return []

    stmt = (
        select(Readset)
        .join(Readset.sample)
        .join(Sample.specimen)
        .where(
            or_(*readset_filters),
            Specimen.project_id == project_id
        )
        .options(
            selectinload(Readset.sample)
            .selectinload(Sample.readsets)
            .selectinload(Readset.experiment)
        )
    )

    readsets = session.execute(stmt).scalars().all()
    samples = []

    for readset in readsets:
        sample = readset.sample
        if sample:
            readsets = sample.readsets
            if readsets:
                first_readset = readsets[0]
                if first_readset.experiment.nucleic_acid_type == nucleic_acid_type and not sample.deprecated and not sample.deleted:
                    samples.append(sample)
                else:
                    ret["DB_ACTION_WARNING"].append(
                        f"'Sample' with 'name' '{sample.name}' only exists with 'nucleic_acid_type' '{first_readset.experiment.nucleic_acid_type.value}' on database. Skipping..."
                    )
            else:
                ret["DB_ACTION_WARNING"].append(
                    f"'Sample' with 'name' '{sample.name}' has no associated readsets. Skipping..."
                )

    return samples


def select_readsets_from_specimens(session, ret, digest_data, nucleic_acid_type, project_id):
    """
    Returning Readset Objects based on requested specimens in digest_data
    """
    specimen_filters = []

    if vb.SPECIMEN_NAME in digest_data:
        specimen_filters.append(Specimen.name.in_(digest_data[vb.SPECIMEN_NAME]))
    if vb.SPECIMEN_ID in digest_data:
        specimen_filters.append(Specimen.id.in_(digest_data[vb.SPECIMEN_ID]))

    if not specimen_filters:
        return []

    stmt = (
        select(Specimen)
        .where(
            or_(*specimen_filters),
            Specimen.project_id == project_id
            )
        .options(
            selectinload(Specimen.samples)
            .selectinload(Sample.readsets)
            .selectinload(Readset.experiment),
            selectinload(Specimen.samples)
            .selectinload(Sample.readsets)
            .selectinload(Readset.operations)
            .selectinload(Operation.jobs)
            .selectinload(Job.files),
            selectinload(Specimen.samples)
            .selectinload(Sample.readsets)
            .selectinload(Readset.files)
            .selectinload(File.locations),
            selectinload(Specimen.samples)
            .selectinload(Sample.readsets)
            .selectinload(Readset.sample),
            selectinload(Specimen.samples)
            .selectinload(Sample.readsets)
            .selectinload(Readset.run),
        )
    )

    specimens = session.execute(stmt).scalars().all()
    readsets = []

    for specimen in specimens:
        for sample in specimen.samples:
            for readset in sample.readsets:
                if readset.experiment.nucleic_acid_type == nucleic_acid_type and not readset.deprecated and not readset.deleted:
                    readsets.append(readset)
                else:
                    ret["DB_ACTION_WARNING"].append(
                        f"'Readset' '{readset.name}' only exists with 'nucleic_acid_type' '{readset.experiment.nucleic_acid_type.value}'. Skipping..."
                    )

    return readsets


def select_readsets_from_samples(session, ret, digest_data, nucleic_acid_type, project_id):
    """
    Returning Readset Objects based on requested samples in digest_data
    """
    sample_filters = []

    if vb.SAMPLE_NAME in digest_data:
        sample_filters.append(Sample.name.in_(digest_data[vb.SAMPLE_NAME]))
    if vb.SAMPLE_ID in digest_data:
        sample_filters.append(Sample.id.in_(digest_data[vb.SAMPLE_ID]))

    if not sample_filters:
        return []

    stmt = (
        select(Sample)
        .join(Sample.specimen)
        .where(
            or_(*sample_filters),
            Specimen.project_id == project_id
        )
        .options(
            selectinload(Sample.readsets)
            .selectinload(Readset.experiment),
            selectinload(Sample.readsets)
            .selectinload(Readset.operations)
            .selectinload(Operation.jobs)
            .selectinload(Job.files),
            selectinload(Sample.readsets)
            .selectinload(Readset.files)
            .selectinload(File.locations),
            selectinload(Sample.readsets)
            .selectinload(Readset.sample),
            selectinload(Sample.readsets)
            .selectinload(Readset.run),
        )
    )

    samples = session.execute(stmt).scalars().all()
    readsets = []

    for sample in samples:
        for readset in sample.readsets:
            if readset.experiment.nucleic_acid_type == nucleic_acid_type and not readset.deprecated and not readset.deleted:
                readsets.append(readset)
            else:
                ret["DB_ACTION_WARNING"].append(
                    f"'Readset' '{readset.name}' only exists with 'nucleic_acid_type' '{readset.experiment.nucleic_acid_type.value}'. Skipping..."
                )

    return readsets


def select_readsets_from_readsets(session, ret, digest_data, nucleic_acid_type, project_id):
    """
    Returning Readset Objects based on requested readsets in digest_data
    """
    readset_filters = []

    if vb.READSET_NAME in digest_data:
        readset_filters.append(Readset.name.in_(digest_data[vb.READSET_NAME]))
    if vb.READSET_ID in digest_data:
        readset_filters.append(Readset.id.in_(digest_data[vb.READSET_ID]))

    if not readset_filters:
        return []

    stmt = (
        select(Readset)
        .join(Readset.sample)
        .join(Sample.specimen)
        .where(
            or_(*readset_filters),
            Specimen.project_id == project_id
        )
        .options(
            selectinload(Readset.experiment),
            selectinload(Readset.operations)
            .selectinload(Operation.jobs)
            .selectinload(Job.files),
            selectinload(Readset.files)
            .selectinload(File.locations),
            selectinload(Readset.sample),
            selectinload(Readset.run),
        )
    )

    readsets = session.execute(stmt).scalars().all()
    filtered = []

    for readset in readsets:
        if readset.experiment.nucleic_acid_type == nucleic_acid_type and not readset.deprecated and not readset.deleted:
            filtered.append(readset)
        else:
            ret["DB_ACTION_WARNING"].append(
                f"'Readset' '{readset.name}' only exists with 'nucleic_acid_type' '{readset.experiment.nucleic_acid_type.value}'. Skipping..."
            )

    return filtered


def extract_file_uri(file, location_endpoint):
    """
    Extract the URI of a file based on its location endpoint.
    """
    for location in file.locations:
        if location.endpoint == location_endpoint:
            return location.uri.split("://")[-1]
    return None


def digest_readset_file(project_id: str, digest_data, session):
    """
    Digesting readset file fields for GenPipes

    Expects:
        - experiment_nucleic_acid_type
        - Specimen/Sample/Readset identifiers
    Returns:
        - A dictionary with readset file information formatted for GenPipes
    """
    ret = {
        "DB_ACTION_WARNING": [],
        "DB_ACTION_OUTPUT": []
    }

    location_endpoint = digest_data.get(vb.LOCATION_ENDPOINT)
    nucleic_acid_type = digest_data.get(vb.EXPERIMENT_NUCLEIC_ACID_TYPE)

    # Check if the nucleic acid type is valid
    nucleic_acid_type = nucleic_acid_type_exists(nucleic_acid_type)

    readsets = (
        select_readsets_from_specimens(session, ret, digest_data, nucleic_acid_type, project_id) +
        select_readsets_from_samples(session, ret, digest_data, nucleic_acid_type, project_id) +
        select_readsets_from_readsets(session, ret, digest_data, nucleic_acid_type, project_id)
    )

    for readset in readsets:
        file_map = {"R1": None, "R2": None, "bam": None, "bed": None}
        readset_files = [
            file for op in readset.operations if op.name == "run_processing"
            for job in op.jobs for file in job.files if file in readset.files
        ]

        for file in readset_files:
            uri = None
            if file.type in ["fastq", "fq", "fq.gz", "fastq.gz"]:
                read_type = file.extra_metadata.get("read_type")
                if read_type in ["R1", "R2"]:
                    uri = extract_file_uri(file, location_endpoint)
                    if uri:
                        file_map[read_type] = uri
                    else:
                        ret["DB_ACTION_WARNING"].append(
                            f"Looking for {read_type} fastq 'File' for 'Sample' '{readset.sample.name}' and 'Readset' '{readset.name}' in '{location_endpoint}', file only exists on {[l.endpoint for l in file.locations]}."
                        )
            elif file.type == "bam":
                uri = extract_file_uri(file, location_endpoint)
                if uri:
                    file_map["bam"] = uri
                else:
                    ret["DB_ACTION_WARNING"].append(
                        f"Looking for bam 'File' for 'Sample' '{readset.sample.name}' and 'Readset' '{readset.name}' in '{location_endpoint}', file only exists on {[l.endpoint for l in file.locations]}."
                    )
            elif file.type == "bed":
                file_map["bed"] = file.name

        ret["DB_ACTION_OUTPUT"].append({
            "Sample": readset.sample.name,
            "Readset": readset.name,
            "LibraryType": readset.experiment.library_kit,
            "RunType": readset.sequencing_type.value,
            "Run": readset.run.name,
            "Lane": readset.lane.value,
            "Adapter1": readset.adapter1,
            "Adapter2": readset.adapter2,
            "QualityOffset": "33",
            "BED": file_map["bed"],
            "FASTQ1": file_map["R1"],
            "FASTQ2": file_map["R2"],
            "BAM": file_map["bam"]
        })

    if not ret["DB_ACTION_WARNING"]:
        ret.pop("DB_ACTION_WARNING")

    return ret  # Let Flask jsonify this


def digest_pair_file(project_id: str, digest_data, session):
    """
    Digesting pair file fields for GenPipes

    Expects:
        - experiment_nucleic_acid_type
        - Specimen/Sample/Readset identifiers
    Returns:
        - A list of specimen/sample tumor-normal pairs
    """
    pair_dict = {}
    samples = []
    ret = {
        "DB_ACTION_WARNING": [],
        "DB_ACTION_OUTPUT": []
    }

    nucleic_acid_type = digest_data.get(vb.EXPERIMENT_NUCLEIC_ACID_TYPE)
    # Check if the nucleic acid type is valid
    nucleic_acid_type = nucleic_acid_type_exists(nucleic_acid_type)

    # Collect samples from all sources
    samples = (
        select_samples_from_specimens(session, ret, digest_data, nucleic_acid_type, project_id) +
        select_samples_from_samples(session, ret, digest_data, nucleic_acid_type, project_id) +
        select_samples_from_readsets(session, ret, digest_data, nucleic_acid_type, project_id)
    )

    # Build tumor-normal pair dictionary
    for sample in samples:
        specimen_name = sample.specimen.name
        if specimen_name not in pair_dict:
            pair_dict[specimen_name] = {"T": None, "N": None}
        if sample.tumour:
            pair_dict[specimen_name]["T"] = sample.name
        else:
            pair_dict[specimen_name]["N"] = sample.name

    # Format output
    for specimen_name, dict_tn in pair_dict.items():
        ret["DB_ACTION_OUTPUT"].append({
            "Specimen": specimen_name,
            "Sample_N": dict_tn["N"],
            "Sample_T": dict_tn["T"]
        })

    if not ret["DB_ACTION_WARNING"]:
        ret.pop("DB_ACTION_WARNING")

    return ret  # Let Flask jsonify this


def digest_unanalyzed(project_id: str, digest_data, session):
    """
    Getting unanalyzed samples or readsets for GenPipes

    Expects:
        - One of: sample_name, sample_id, readset_name, readset_id
        - experiment_nucleic_acid_type
        - Optional: run_id or run_name
    Returns:
        - List of unanalyzed sample or readset identifiers
    """
    if isinstance(project_id, str):
        project_id = [project_id]

    ret = {
        "DB_ACTION_WARNING": [],
        "DB_ACTION_OUTPUT": []
    }

    sample_name_flag = digest_data.get(vb.SAMPLE_NAME)
    sample_id_flag = digest_data.get(vb.SAMPLE_ID)
    readset_name_flag = digest_data.get(vb.READSET_NAME)
    readset_id_flag = digest_data.get(vb.READSET_ID)
    run_id = digest_data.get(vb.RUN_ID)
    run_name = digest_data.get(vb.RUN_NAME)
    experiment_nucleic_acid_type = digest_data.get(vb.EXPERIMENT_NUCLEIC_ACID_TYPE)
    location_endpoint = digest_data.get(vb.LOCATION_ENDPOINT)

    # Check if the location endpoint exists in the database
    experiment_nucleic_acid_type = nucleic_acid_type_exists(experiment_nucleic_acid_type)

    # Resolve run_name to run_id if needed
    if run_name and not run_id:
        try:
            run_id = name_to_id("Run", run_name)[0]
        except Exception:
            raise DidNotFindError(f"'Run' with 'name' '{run_name}' doesn't exist in the database")

    # Determine base query and key
    if sample_name_flag:
        stmt = (
            select(Sample.name)
            .join(Sample.readsets)
            .join(Sample.specimen)
            .join(Specimen.project)
            .where(Project.id.in_(project_id))
        )
        key = "sample_name"
    elif sample_id_flag:
        stmt = (
            select(Sample.id)
            .join(Sample.readsets)
            .join(Sample.specimen)
            .join(Specimen.project)
            .where(Project.id.in_(project_id))
        )
        key = "sample_id"
    elif readset_name_flag:
        stmt = (
            select(Readset.name)
            .join(Readset.sample)
            .join(Sample.specimen)
            .join(Specimen.project)
            .where(Project.id.in_(project_id))
        )
        key = "readset_name"
    elif readset_id_flag:
        stmt = (
            select(Readset.id)
            .join(Readset.sample)
            .join(Sample.specimen)
            .join(Specimen.project)
            .where(Project.id.in_(project_id))
        )
        key = "readset_id"
    else:
        raise RequestError(argument="sample_name, sample_id, readset_name or readset_id")

    # Base filters
    stmt = stmt.where(
        Readset.state == StateEnum("VALID"),
        Readset.deprecated.is_(False),
        Readset.deleted.is_(False),
    )

    # Join and filter for unanalyzed (no genpipes operation)
    stmt = stmt.join(Readset.operations).where(Operation.name.notilike("%genpipes%"))

    # Optional filters
    if run_id:
        stmt = stmt.join(Readset.run).where(Run.id == run_id)

    if experiment_nucleic_acid_type:
        stmt = stmt.join(Readset.experiment).where(Experiment.nucleic_acid_type == experiment_nucleic_acid_type)

    results = session.scalars(stmt).all()

    ret["DB_ACTION_OUTPUT"].append({
        "location_endpoint": location_endpoint,
        "experiment_nucleic_acid_type": experiment_nucleic_acid_type.to_json(),
        key: results
    })

    if not ret["DB_ACTION_WARNING"]:
        ret.pop("DB_ACTION_WARNING")

    return ret  # Let Flask jsonify this


def digest_delivery(project_id: str, digest_data, session):
    """
    Getting delivery samples or readsets for GenPipes

    Expects:
        - experiment_nucleic_acid_type
        - Specimen/Sample/Readset identifiers
        - Optional location_endpoint
    Returns:
        - Structured delivery information including files, metrics, and operations
    """
    location_endpoint = digest_data.get(vb.LOCATION_ENDPOINT)
    # Check if the location endpoint exists in the database
    endpoint_exists(session, location_endpoint)

    nucleic_acid_type = digest_data.get(vb.EXPERIMENT_NUCLEIC_ACID_TYPE)
    # Check if the nucleic acid type is valid
    nucleic_acid_type = nucleic_acid_type_exists(nucleic_acid_type)

    ret = {
        "DB_ACTION_WARNING": [],
        "DB_ACTION_OUTPUT": []
    }

    readsets = (
        select_readsets_from_specimens(session, ret, digest_data, nucleic_acid_type, project_id) +
        select_readsets_from_samples(session, ret, digest_data, nucleic_acid_type, project_id) +
        select_readsets_from_readsets(session, ret, digest_data, nucleic_acid_type, project_id)
    )

    operation_config_ids = set()

    operations = []
    specimens = []

    for readset in readsets:
        readset_files = []
        readset_metrics = []

        # Collect operations
        for operation in readset.operations:
            op_config = operation.operation_config
            if op_config and op_config.id not in operation_config_ids:
                if operation.cmd_line and op_config.data:
                    operations.append({
                        "cmd_line": operation.cmd_line,
                        "config_data": op_config.data.decode("utf-8")
                    })
                operation_config_ids.add(op_config.id)

        # Collect deliverable files
        for file in readset.files:
            if file.deliverable and not file.deleted:
                file_uri = None
                if location_endpoint:
                    for location in file.locations:
                        if location.endpoint == location_endpoint and not location.deleted:
                            file_uri = location.uri.split("://")[-1]
                            break
                if file_uri:
                    readset_files.append({
                        "name": file.name,
                        "location": file_uri
                    })
                else:
                    ret["DB_ACTION_WARNING"].append(
                        f"Looking for 'File' '{file.name}' for 'Sample' '{readset.sample.name}' and 'Readset' '{readset.name}' in '{location_endpoint}', file only exists on {[l.endpoint for l in file.locations]}."
                    )

        # Collect deliverable metrics
        for metric in readset.metrics:
            if metric.deliverable:
                readset_metrics.append({
                    "name": metric.name,
                    "value": metric.value,
                    "aggregate": metric.aggregate,
                    "flag": metric.flag.to_json()
                })

        # Build nested structure
        sample_name = readset.sample.name
        specimen_name = readset.sample.specimen.name

        # Find or create specimen entry
        specimen_entry = next((s for s in specimens if s["name"] == specimen_name), None)
        if not specimen_entry:
            specimen_entry = {
                "name": specimen_name,
                "cohort": readset.sample.specimen.cohort,
                "institution": readset.sample.specimen.institution,
                "sample": []
            }
            specimens.append(specimen_entry)

        # Find or create sample entry
        sample_entry = next((s for s in specimen_entry["sample"] if s["name"] == sample_name), None)
        if not sample_entry:
            sample_entry = {
                "name": sample_name,
                "tumour": readset.sample.tumour,
                "readset": []
            }
            specimen_entry["sample"].append(sample_entry)

        if not readset_files:
            ret["DB_ACTION_WARNING"].append(
                f"No deliverable files found for 'Sample' '{sample_name}' and 'Readset' '{readset.name}' on 'LocationEndpoint' '{location_endpoint}'."
            )
        # Append readset info
        sample_entry["readset"].append({
            "name": readset.name,
            "file": readset_files,
            "metric": readset_metrics
        })

    ret["DB_ACTION_OUTPUT"].append(
        {
            "location_endpoint": location_endpoint,
            "experiment_nucleic_acid_type": nucleic_acid_type.value,
            "operation": operations,
            "specimen": specimens
        }
    )

    if not ret["DB_ACTION_WARNING"]:
        ret.pop("DB_ACTION_WARNING")

    return ret  # Let Flask jsonify this
