from __future__ import annotations

from dataclasses import dataclass, field
from typing import List
from datetime import datetime
from sqlalchemy import (
    Column,
    ForeignKey,
    Integer,
    Boolean,
    String,
    JSON,
    DateTime,
    )
from sqlalchemy.orm import (
    relationship,
    registry,
    Mapped,
    mapped_column
    )

mapper_registry = registry()


@dataclass
class BaseTable:
    """
    Define fields common of all tables in database
    BaseTable:
        id integer [PK]
        deprecated boolean
        deleted boolean
        creation timestamp
        modification timestamp
        extra_metadata json
    """
    __abstract__ = True

    __sa_dataclass_metadata_key__ = "sa"

    id: int = field(init=False, metadata={"sa": Column(Integer, primary_key=True)})
    deprecated: bool = field(default=None, metadata={"sa": Column(Boolean)})
    deleted: bool = field(default=None, metadata={"sa": Column(Boolean)})
    creation: datetime = field(default=None, metadata={"sa":  Column(DateTime, nullable=True)})
    modification: datetime = field(default=None, metadata={"sa":  Column(DateTime, nullable=True)})
    extra_metadata: dict = field(default=None, metadata={"sa":  Column(JSON, nullable=True)})


@mapper_registry.mapped
@dataclass
class Project(BaseTable):
    """
    Project:
        id integer [PK]
        fms_id integer
        name text (unique)
        deprecated boolean
        deleted boolean
        creation timestamp
        modification timestamp
        extra_metadata json
    """
    __tablename__ = "project"

    __sa_dataclass_metadata_key__ = "sa"

    fms_id: str = field(default=None, metadata={"sa": Column(String, nullable=True)})
    name: str = field(default=None, metadata={"sa": Column(String, nullable=False, unique=True)})


@mapper_registry.mapped
@dataclass
class Patient(BaseTable):
    """
    Patient:
        id integer [PK]
        project_id integer [ref: > project.id]
        fms_id integer
        name text (unique)
        alias blob
        cohort text
        institution text
        deprecated boolean
        deleted boolean
        creation timestamp
        modification timestamp
        extra_metadata json
    """
    __tablename__ = "patient"

    _sa_dataclass_metadata_key__ = "sa"

    project_id: int = field(default=None, metadata={"sa": Column(Integer, ForeignKey("project.id"))})
    fms_id: str = field(default=None, metadata={"sa": Column(String, nullable=True)})
    name: str = field(default=None, metadata={"sa": Column(String, nullable=False, unique=True)})
    alias: str = field(default=None, metadata={"sa": Column(String, nullable=True)})
    cohort: str = field(default=None, metadata={"sa": Column(String, nullable=True)})
    institution: str = field(default=None, metadata={"sa": Column(String, nullable=True)})

    project: List[Project] = field(default_factory=list,
                                   metadata={"sa": relationship("Project", backref="patient", lazy=False)})


@mapper_registry.mapped
@dataclass
class Experiment(BaseTable):
    """
    Experiment:
        id integer [PK]
        project_id integer [ref: > project.id]
        sequencing_technology text
        deprecated boolean
        deleted boolean
        creation timestamp
        modification timestamp
        extra_metadata json
    """
    __tablename__ = "experiment"
    _sa_dataclass_metadata_key__ = "sa"
    project_id: int = field(init=False, metadata={"sa": Column(Integer, ForeignKey("project.id"))})
    sequencing_technology: str = field(default=None, metadata={"sa": Column(String, nullable=True)})

    run: List[Run] = field(default=None, metadata={"sa": relationship('Run', secondary='experiment_run')})
    project: List[Project] = field(default=None,
                                   metadata={"sa": relationship("Project", backref="experiment", lazy=False)})


@mapper_registry.mapped
@dataclass
class Run(BaseTable):
    """
    Patient:
        id integer [PK]
        fms_id integer
        lab_id text
        name text (unique)
        date timestamp
        deprecated boolean
        deleted boolean
        creation timestamp
        modification timestamp
        extra_metadata json
    """
    __tablename__ = "run"
    _sa_dataclass_metadata_key__ = "sa"
    fms_id: str = field(default=None, metadata={"sa":  Column(String, nullable=True)})
    lab_id: str = field(default=None, metadata={"sa":  Column(String, nullable=True)})
    name: str = field(default=None, metadata={"sa":  Column(String, nullable=False, unique=True)})
    date: datetime = field(default=None, metadata={"sa":  Column(DateTime, nullable=True)})
    experiment: List[Experiment] = field(default_factory=list,
                                         metadata={"sa": relationship('Experiment', secondary='experiment_run')})


@mapper_registry.mapped
@dataclass
class Sample(BaseTable):
    """
    Sample:
        id integer [PK]
        patient_id integer [ref: > patient.id]
        experiment_id integer [ref: > experiment.id]
        fms_id integer
        name text (unique)
        tumour boolean
        alias blob
        deprecated boolean
        deleted boolean
        creation timestamp
        modification timestamp
        extra_metadata json
    """
    __tablename__ = "sample"
    _sa_dataclass_metadata_key__ = "sa"
    patient_id: int = field(init=False, metadata={"sa":  Column(Integer, ForeignKey("patient.id"))})
    experiment_id: int = field(init=False, metadata={"sa":  Column(Integer, ForeignKey("experiment.id"))})
    fms_id: str = field(default=None, metadata={"sa":  Column(String, nullable=True, default=None)})
    name: str = field(default=None, metadata={"sa":  Column(String, nullable=False, unique=True, default=None)})
    tumour: bool = field(default=None, metadata={"sa":  Column(Boolean, default=False)})
    alias: str = field(default=None, metadata={"sa":  Column(String, nullable=True, default=None)})

    patient: List[Patient] = field(default=None,
                                   metadata={"sa": relationship("Patient", backref="sample", lazy=False)})
    experiment: List[Experiment] = field(default=None,
                                         metadata={"sa": relationship("Experiment", backref="sample", lazy=False)})


@mapper_registry.mapped
@dataclass
class Readset(BaseTable):
    """
    Readset:
        id integer [PK]
        sample_id integer [ref: > sample.id]
        run_id integer [ref: > run.id]
        name text (unique)
        lane text
        adapter1 text
        adapter2 text
        sequencing_type text
        quality_offset text
        alias blob
        deprecated boolean
        deleted boolean
        creation timestamp
        modification timestamp
        extra_metadata json
    """
    __tablename__ = "readset"
    _sa_dataclass_metadata_key__ = "sa"
    sample_id: int = field(init=False, metadata={"sa":  Column(Integer, ForeignKey("sample.id"))})
    run_id: int = field(init=False, metadata={"sa":  Column(Integer, ForeignKey("run.id"))})
    name: str = field(default=None, metadata={"sa":  Column(String, nullable=False, unique=True, default=None)})
    lane: str = field(default=None, metadata={"sa":  Column(String, nullable=True, default=None)})
    adapter1: str = field(default=None, metadata={"sa":  Column(String, nullable=True, default=None)})
    adapter2: str = field(default=None, metadata={"sa":  Column(String, nullable=True, default=None)})
    sequencing_type: str = field(default=None, metadata={"sa":  Column(String, nullable=True, default=None)})
    quality_offset: str = field(default=None, metadata={"sa":  Column(String, nullable=True, default=None)})
    alias: str = field(default=None, metadata={"sa":  Column(String, nullable=True, default=None)})

    sample: List[Sample] = field(default=None,
                                 metadata={"sa": relationship("Sample", backref="readset", lazy=False)})
    run: List[Run] = field(default=None,
                           metadata={"sa": relationship("Run", backref="readset", lazy=False)})


@mapper_registry.mapped
@dataclass
class Step(BaseTable):
    """
    Step:
        id integer [PK]
        sample_id integer [ref: > sample.id]
        readset_id integer [ref: > readset.id]
        name text
        status text
        deprecated boolean
        deleted boolean
        creation timestamp
        modification timestamp
        extra_metadata json
    """
    __tablename__ = "step"
    _sa_dataclass_metadata_key__ = "sa"
    sample_id: int = field(init=False, metadata={"sa":  Column(Integer, ForeignKey("sample.id"))})
    readset_id: int = field(init=False, metadata={"sa":  Column(Integer, ForeignKey("readset.id"))})
    name: str = field(default=None, metadata={"sa":  Column(String, nullable=False, default=None)})
    status: str = field(default=None, metadata={"sa":  Column(String, nullable=True, default=None)})

    sample: List[Sample] = field(default=None,
                                 metadata={"sa": relationship("Sample", backref="step", lazy=False)})
    readset: List[Readset] = field(default=None,
                                   metadata={"sa": relationship("Readset", backref="step", lazy=False)})


@mapper_registry.mapped
@dataclass
class Job(BaseTable):
    """
    Job:
        id integer [PK]
        step_id integer [ref: > step.id]
        name text
        start timestamp
        stop timestamp
        status text
        type text
        deprecated boolean
        deleted boolean
        creation timestamp
        modification timestamp
        extra_metadata json
    """
    __tablename__ = "job"
    _sa_dataclass_metadata_key__ = "sa"
    step_id: id = field(init=False, metadata={"sa":  Column(Integer, ForeignKey("step.id"))})
    name: str = field(default=None, metadata={"sa":  Column(String, nullable=False, default=None)})
    start: datetime = field(default=None, metadata={"sa":  Column(DateTime, nullable=True, default=None)})
    stop: datetime = field(default=None, metadata={"sa":  Column(DateTime, nullable=True, default=None)})
    status: str = field(default=None, metadata={"sa":  Column(String, nullable=True, default=None)})
    type: str = field(default=None, metadata={"sa":  Column(String, nullable=True, default=None)})
    step: List[Readset] = field(default=None,
                                metadata={"sa": relationship("Step", backref="job", lazy=False)})


@mapper_registry.mapped
@dataclass
class Metric(BaseTable):
    """
    Metric:
        id integer [PK]
        job_id integer [ref: > job.id]
        name text
        value text
        flag text //pass, warn, fail
        deliverable boolean
        aggregate text //operation to perform for aggregating metric per sample
        deprecated boolean
        deleted boolean
        creation timestamp
        modification timestamp
        extra_metadata json
    """
    __tablename__ = "metric"
    _sa_dataclass_metadata_key__ = "sa"
    job_id: int = field(init=False, metadata={"sa":  Column(Integer, ForeignKey("job.id"))})
    name: str = field(default=None, metadata={"sa":  Column(String, nullable=False)})
    value: str = field(default=None, metadata={"sa":  Column(String, nullable=True)})
    flag: str = field(default=None, metadata={"sa":  Column(String, nullable=True)})
    deliverable: bool = field(default=None, metadata={"sa":  Column(Boolean)})
    aggregate: str = field(default=None, metadata={"sa":  Column(String, nullable=True)})

    job: List[Job] = field(default=None,
                           metadata={"sa": relationship("Job", backref="metric", lazy=False)})


@mapper_registry.mapped
@dataclass
class File(BaseTable):
    """
    File:
        id integer [PK]
        job_id integer [ref: > job.id]
        path text
        type text
        description text
        creation timestamp
        deliverable boolean
        deprecated boolean
        deleted boolean
        creation timestamp
        modification timestamp
        extra_metadata json
    """
    __tablename__ = "file"
    _sa_dataclass_metadata_key__ = "sa"
    job_id: int = field(init=False, metadata={"sa":  Column(Integer, ForeignKey("job.id"))})
    path: str = field(default=None, metadata={"sa":  Column(String, nullable=True, default=None)})
    type: str = field(default=None, metadata={"sa":  Column(String, nullable=True, default=None)})
    description: str = field(default=None, metadata={"sa":  Column(String, nullable=True, default=None)})
    creation: datetime = field(default=None, metadata={"sa":  Column(DateTime, nullable=True, default=None)})
    deliverable: bool = field(default=None, metadata={"sa":  Column(Boolean, default=False)})

    job: List[Job] = field(default=None,
                           metadata={"sa": relationship("Job", backref="file", lazy=False)})


@mapper_registry.mapped
@dataclass
class Tool(BaseTable):
    """
    Tool:
        id integer [PK]
        job_id integer [ref: > job.id]
        name text
        version text
        deprecated boolean
        deleted boolean
        creation timestamp
        modification timestamp
        extra_metadata json
    """
    __tablename__ = "tool"
    _sa_dataclass_metadata_key__ = "sa"
    job_id: int = field(init=False, metadata={"sa":  Column(Integer, ForeignKey("job.id"))})
    name: str = field(default=None, metadata={"sa":  Column(String, nullable=True, default=None)})
    version: str = field(default=None, metadata={"sa": Column(String, nullable=True, default=None)})

    job: List[Job] = field(default=None,
                           metadata={"sa": relationship("Job", backref="tool", lazy=False)})
