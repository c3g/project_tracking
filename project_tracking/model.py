from __future__ import annotations

import json
import enum
import logging
from datetime import datetime
from decimal import Decimal

logger = logging.getLogger(__name__)

from sqlalchemy import (
    Column,
    ForeignKey,
    Integer,
    JSON,
    Enum,
    Table
    )
from sqlalchemy.orm import (
    relationship,
    DeclarativeBase,
    Mapped,
    mapped_column,
    collections,
    attributes
    )


class LaneEnum(enum.Enum):
    """
    lane enum
    """
    ONE = "1"
    TWO = "2"
    THREE = "3"
    FOUR = "4"


class SequencingTypeEnum(enum.Enum):
    """
    sequencing_type enum
    """
    SINGLE_END = "SINGLE_END"
    PAIRED_END = "PAIRED_END"


class StatusEnum(enum.Enum):
    """
    status enum
    """
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    DONE = "DONE"
    FAILED = "FAILED"


class FlagEnum(enum.Enum):
    """
    flag enum
    """
    PASS = "PASS"
    WARNING = "WARNING"
    FAILED = "FAILED"


class AggregateEnum(enum.Enum):
    """
    aggregate enum
    """
    SUM = "SUM"
    AVERAGE = "AVERAGE"
    N = "N" # for NOT aggregating for metric at sample level


class Base(DeclarativeBase):
    """
    Base declarative table
    """
    # this is needed for the enum to work properly right now
    # see https://github.com/sqlalchemy/sqlalchemy/discussions/8856
    type_annotation_map = {
        LaneEnum: Enum(LaneEnum),
        SequencingTypeEnum: Enum(SequencingTypeEnum),
        StatusEnum: Enum(StatusEnum),
        FlagEnum: Enum(FlagEnum),
        AggregateEnum: Enum(AggregateEnum)
    }


readset_file = Table(
    "readset_file",
    Base.metadata,
    Column("readset_id", ForeignKey("readset.id"), primary_key=True),
    Column("file_id", ForeignKey("file.id"), primary_key=True),
)


readset_metric = Table(
    "readset_metric",
    Base.metadata,
    Column("readset_id", ForeignKey("readset.id"), primary_key=True),
    Column("metric_id", ForeignKey("metric.id"), primary_key=True),
)


readset_job = Table(
    "readset_job",
    Base.metadata,
    Column("readset_id", ForeignKey("readset.id"), primary_key=True),
    Column("job_id", ForeignKey("job.id"), primary_key=True),
)


readset_operation = Table(
    "readset_operation",
    Base.metadata,
    Column("readset_id", ForeignKey("readset.id"), primary_key=True),
    Column("operation_id", ForeignKey("operation.id"), primary_key=True),
)


operation_bundle = Table(
    "operation_bundle",
    Base.metadata,
    Column("operation_id", ForeignKey("operation.id"), primary_key=True),
    Column("bundle_id", ForeignKey("bundle.id"), primary_key=True),
)


bundle_bundle = Table(
    "bundle_bundle",
    Base.metadata,
    Column("bundle_id", Integer, ForeignKey("bundle.id"), primary_key=True),
    Column("reference_id", Integer, ForeignKey("bundle.id"), primary_key=True),
)


class BaseTable(Base):
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

    id: Mapped[int] = mapped_column(primary_key=True)
    deprecated: Mapped[bool] = mapped_column(default=False)
    deleted: Mapped[bool] = mapped_column(default=False)
    creation: Mapped[datetime] = mapped_column(default=datetime.now(), nullable=True)
    modification: Mapped[datetime] = mapped_column(default=None, nullable=True)
    extra_metadata: Mapped[dict] = mapped_column(JSON, default=None, nullable=True)


    def __repr__(self):
        """
        returns:
         {tablename: {mapped_columns}} only and not the relationships Attributes
        """
        dico = {}
        dico[self.__tablename__] = {c.key: getattr(self, c.key) for c in self.__table__.columns}
        return dico.__repr__()

    @property
    def dict(self):
        """
        Dictionary of table columns *and* of the relation columns
        """
        dico = {}
        # select the column and the relationship
        selected_col = (x for x in dir(self.__class__) # loops over all attribute of the class
                        if not x.startswith('_') and
                        isinstance(getattr(self.__class__, x), attributes.InstrumentedAttribute) and
                        getattr(self, x, False)   # To drop ref to join table that do exist in the class
                        )
        for column in selected_col:
            # check in class if column is instrumented
            key = column
            val = getattr(self, column)
            dico[key] = val
        return dico

    @property
    def flat_dict(self):
        """
        Flat casting of object, to be used in flask responses
        Returning only ids of the referenced objects
        """
        dumps = {}
        for key, val in self.dict.items():
            if isinstance(val, datetime):
                val = val.isoformat()
            elif isinstance(val, Decimal):
                val = float(val)
            elif isinstance(val, set):
                val = sorted(val)
            elif isinstance(val, (list, set, collections.List, collections.Set)):
                val = sorted([e.id for e in val])
            elif isinstance(val, DeclarativeBase):
                val = val.id
            elif isinstance(val, enum.Enum):
                val = val.value
            dumps[key] = val
        return dumps

    @property
    def dumps(self):
        """
        Dumping the flat_dict
        """
        return json.dumps(self.flat_dict)


class Project(BaseTable):
    """
    Project:
        id integer [PK]
        fms_id integer
        name text (unique)
        alias json
        deprecated boolean
        deleted boolean
        creation timestamp
        modification timestamp
        extra_metadata json
    """
    __tablename__ = "project"

    fms_id: Mapped[str] = mapped_column(default=None, nullable=True)
    name: Mapped[str] = mapped_column(default=None, nullable=False, unique=True)
    alias: Mapped[dict] = mapped_column(JSON, default=None, nullable=True)

    patient: Mapped[list["Patient"]] = relationship(back_populates="project")


class Patient(BaseTable):
    """
    Patient:
        id integer [PK]
        project_id integer [ref: > project.id]
        fms_id integer
        name text (unique)
        alias json
        cohort text
        institution text
        deprecated boolean
        deleted boolean
        creation timestamp
        modification timestamp
        extra_metadata json
    """
    __tablename__ = "patient"

    project_id: Mapped[int] = mapped_column(ForeignKey("project.id"), default=None)
    fms_id: Mapped[str] = mapped_column(default=None, nullable=True)
    name: Mapped[str] = mapped_column(default=None, nullable=False, unique=True)
    alias: Mapped[dict] = mapped_column(JSON, default=None, nullable=True)
    cohort: Mapped[str] = mapped_column(default=None, nullable=True)
    institution: Mapped[str] = mapped_column(default=None, nullable=True)

    project: Mapped["Project"] = relationship(back_populates="patient")
    sample: Mapped[list["Sample"]] = relationship(back_populates="patient")


class Sample(BaseTable):
    """
    Sample:
        id integer [PK]
        patient_id integer [ref: > patient.id]
        fms_id integer
        name text (unique)
        alias json
        tumour boolean
        deprecated boolean
        deleted boolean
        creation timestamp
        modification timestamp
        extra_metadata json
    """
    __tablename__ = "sample"

    patient_id: Mapped[int] = mapped_column(ForeignKey("patient.id"), default=None)
    fms_id: Mapped[str] = mapped_column(default=None, nullable=True)
    name: Mapped[str] = mapped_column(default=None, nullable=False, unique=True)
    alias: Mapped[dict] = mapped_column(JSON, default=None, nullable=True)
    tumour: Mapped[bool] = mapped_column(default=False)

    patient: Mapped["Patient"] = relationship(back_populates="sample")
    readset: Mapped[list["Readset"]] = relationship(back_populates="sample")

class Experiment(BaseTable):
    """
    Experiment:
        id integer [PK]
        sequencing_technology text
        type text
        library_kit text
        kit_expiration_date text
        deprecated boolean
        deleted boolean
        creation timestamp
        modification timestamp
        extra_metadata json
    """
    __tablename__ = "experiment"

    sequencing_technology: Mapped[str] = mapped_column(default=None, nullable=True)
    type: Mapped[str] = mapped_column(default=None, nullable=True)
    library_kit: Mapped[str] = mapped_column(default=None, nullable=True)
    kit_expiration_date: Mapped[str] = mapped_column(default=None, nullable=True)

    readset: Mapped[list["Readset"]] = relationship(back_populates="experiment")

class Run(BaseTable):
    """
    Patient:
        id integer [PK]
        fms_id text
        name text
        instrument text
        date timestamp
        deprecated boolean
        deleted boolean
        creation timestamp
        modification timestamp
        extra_metadata json
    """
    __tablename__ = "run"

    fms_id: Mapped[str] = mapped_column(default=None, nullable=True)
    name: Mapped[str] = mapped_column(default=None, nullable=True)
    instrument: Mapped[str] = mapped_column(default=None, nullable=True)
    date: Mapped[datetime] = mapped_column(default=None, nullable=True)

    readset: Mapped[list["Readset"]] = relationship(back_populates="run")

class Readset(BaseTable):
    """
    Readset:
        id integer [PK]
        sample_id integer [ref: > sample.id]
        experiment_id  text [ref: > experiment.id]
        run_id integer [ref: > run.id]
        name text (unique)
        alias json
        lane lane
        adapter1 text
        adapter2 text
        sequencing_type sequencing_type
        quality_offset text
        deprecated boolean
        deleted boolean
        creation timestamp
        modification timestamp
        extra_metadata json
    """
    __tablename__ = "readset"

    sample_id: Mapped[int] = mapped_column(ForeignKey("sample.id"), default=None)
    experiment_id: Mapped[int] = mapped_column(ForeignKey("experiment.id"), default=None)
    run_id: Mapped[int] = mapped_column(ForeignKey("run.id"), default=None)
    name: Mapped[str] = mapped_column(default=None, nullable=False, unique=True)
    alias: Mapped[dict] = mapped_column(JSON, default=None, nullable=True)
    lane: Mapped[LaneEnum]  =  mapped_column(default=None, nullable=True)
    adapter1: Mapped[str] = mapped_column(default=None, nullable=True)
    adapter2: Mapped[str] = mapped_column(default=None, nullable=True)
    sequencing_type: Mapped[SequencingTypeEnum] = mapped_column(default=None, nullable=True)
    quality_offset: Mapped[str] = mapped_column(default=None, nullable=True)

    sample: Mapped["Sample"] = relationship(back_populates="readset")
    experiment: Mapped["Experiment"] = relationship(back_populates="readset")
    run: Mapped["Run"] = relationship(back_populates="readset")
    file: Mapped[list["File"]] = relationship(secondary=readset_file, back_populates="readset")
    operation: Mapped[list["Operation"]] = relationship(secondary=readset_operation, back_populates="readset")
    job: Mapped[list["Job"]] = relationship(secondary=readset_job, back_populates="readset")
    metric: Mapped[list["Metric"]] = relationship(secondary=readset_metric, back_populates="readset")

class Operation(BaseTable):
    """
    Operation:
        id integer [PK]
        operation_config_id integer [ref: > operation_config.id]
        platform text
        cmd_line text
        name text
        status status
        deprecated boolean
        deleted boolean
        creation timestamp
        modification timestamp
        extra_metadata json
    """
    __tablename__ = "operation"

    operation_config_id: Mapped[int] = mapped_column(ForeignKey("operation_config.id"), default=None)
    project_id: Mapped[int] = mapped_column(ForeignKey("project.id"))
    platform: Mapped[str] = mapped_column(default=None, nullable=True)
    cmd_line: Mapped[str] = mapped_column(default=None, nullable=True)
    name: Mapped[str] = mapped_column(default=None, nullable=True)
    status: Mapped[StatusEnum] = mapped_column(default=StatusEnum.PENDING)

    operation_config: Mapped["OperationConfig"] = relationship(back_populates="operation")
    project: Mapped["Project"] = relationship()
    job: Mapped[list["Job"]] = relationship(back_populates="operation")
    bundle: Mapped[list["Bundle"]] = relationship(secondary=operation_bundle, back_populates="operation")
    readset: Mapped[list["Readset"]] = relationship(secondary=readset_operation, back_populates="operation")


class OperationConfig(BaseTable):
    """
    OperationConfig:
        id integer [PK]
        config_bundle_id integer [ref: > bundle.id]
        name text
        version test
        deprecated boolean
        deleted boolean
        creation timestamp
        modification timestamp
        extra_metadata json
    """
    __tablename__ = "operation_config"

    config_bundle_id: Mapped[int] = mapped_column(ForeignKey("bundle.id"), default=None, nullable=True)
    name: Mapped[str] = mapped_column(default=None, nullable=True)
    version: Mapped[str] = mapped_column(default=None, nullable=True)

    operation: Mapped[list["Operation"]] = relationship(back_populates="operation_config")
    bundle: Mapped["Bundle"] = relationship(back_populates = "operation_config")


class Job(BaseTable):
    """
    Job:
        id integer [PK]
        operation_id integer [ref: > operation.id]
        readset_id integer
        name text
        start timestamp
        stop timestamp
        status status
        type text
        deprecated boolean
        deleted boolean
        creation timestamp
        modification timestamp
        extra_metadata json
    """
    __tablename__ = "job"

    operation_id: Mapped[int] = mapped_column(ForeignKey("operation.id"), default=None)
    name: Mapped[str] = mapped_column(default=None, nullable=True)
    start: Mapped[datetime] = mapped_column(default=None, nullable=True)
    stop: Mapped[datetime] = mapped_column(default=None, nullable=True)
    status: Mapped[StatusEnum] = mapped_column(default=None, nullable=True)
    type: Mapped[str] = mapped_column(default=None, nullable=True)

    operation: Mapped["Operation"] = relationship(back_populates="job")
    metric: Mapped[list["Metric"]] = relationship(back_populates="job")
    bundle: Mapped[list["Bundle"]] = relationship(back_populates="job")
    readset: Mapped[list["Readset"]] = relationship(secondary=readset_job, back_populates="job")

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

    job_id: Mapped[int] = mapped_column(ForeignKey("job.id"), default=None)
    name: Mapped[str] = mapped_column(default=None, nullable=True)
    value: Mapped[str] = mapped_column()
    flag: Mapped[FlagEnum] = mapped_column(default=None, nullable=True)
    deliverable: Mapped[bool] = mapped_column(default=False)
    aggregate: Mapped[AggregateEnum] = mapped_column(default=None, nullable=True)

    job: Mapped["Job"] = relationship(back_populates="metric")
    readset: Mapped[list["Readset"]] = relationship(secondary=readset_metric, back_populates="metric")


class Bundle(BaseTable):
    """
    Bundle:
        id integer [PK]
        job_id integer [ref: > job.id]
        uri text
        deliverable bool
        deprecated boolean
        deleted boolean
        creation timestamp
        modification timestamp
        extra_metadata json
    """
    __tablename__ = "bundle"

    job_id: Mapped[int] = mapped_column(ForeignKey("job.id"), default=None, nullable=True)
    uri: Mapped[str] = mapped_column(default=None, nullable=True)
    deliverable: Mapped[bool] = mapped_column(default=False)

    job: Mapped["Job"] = relationship(back_populates="bundle")
    file: Mapped[list["File"]] = relationship(back_populates="bundle")
    operation_config: Mapped[list["OperationConfig"]] = relationship(back_populates="bundle")
    operation: Mapped[list["Operation"]] = relationship(secondary=operation_bundle, back_populates="bundle")

    reference: Mapped[list["Bundle"]] = relationship("Bundle", secondary=bundle_bundle,
                                                     primaryjoin="Bundle.id ==bundle_bundle.c.bundle_id",
                                                     secondaryjoin="Bundle.id ==bundle_bundle.c.reference_id")


class File(BaseTable):
    """
    File:
        id integer [PK]
        bundle_id integer [ref: > bundle.id]
        content text
        type text
        deliverable boolean
        deprecated boolean
        deleted boolean
        creation timestamp
        modification timestamp
        extra_metadata json
    """
    __tablename__ = "file"

    content: Mapped[str] = mapped_column()
    bundle_id: Mapped[int] = mapped_column(ForeignKey("bundle.id"), default=None, nullable=True)
    type: Mapped[str] = mapped_column(default=None, nullable=True)
    deliverable: Mapped[bool] = mapped_column(default=False)

    readset: Mapped[list["Readset"]] = relationship(secondary=readset_file, back_populates="file")
    bundle: Mapped["Bundle"] = relationship(back_populates="file")
