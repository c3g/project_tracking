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
    DateTime,
    select,
    Table,
    LargeBinary
    )

from sqlalchemy.orm import (
    relationship,
    DeclarativeBase,
    Mapped,
    mapped_column,
    collections,
    attributes
    )

from sqlalchemy.sql import func

from sqlalchemy_json import mutable_json_type

from . import database

class NucleicAcidTypeEnum(enum.Enum):
    """nucleic_acid_type enum"""
    DNA = "DNA"
    RNA = "RNA"

    def to_json(self):
        """Serialize the enum to json"""
        return self.value


class LaneEnum(enum.Enum):
    """
    lane enum
    """
    ONE = "1"
    TWO = "2"
    THREE = "3"
    FOUR = "4"
    FIVE = "5"
    SIX = "6"
    SEVEN = "7"
    EIGHT = "8"

    def to_json(self):
        """Serialize the enum to json"""
        return self.value


class SequencingTypeEnum(enum.Enum):
    """
    sequencing_type enum
    """
    SINGLE_END = "SINGLE_END"
    PAIRED_END = "PAIRED_END"

    def to_json(self):
        """Serialize the enum to json"""
        return self.value

class StateEnum(enum.Enum):
    """
    state enum
    """
    VALID = "VALID"
    ON_HOLD = "ON_HOLD"
    INVALID = "INVALID"

    def to_json(self):
        """Serialize the enum to json"""
        return self.value


class StatusEnum(enum.Enum):
    """
    status enum
    """
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    OUT_OF_MEMORY = "OUT_OF_MEMORY"
    CANCELLED = "CANCELLED"

    def to_json(self):
        """Serialize the enum to json"""
        return self.value


class FlagEnum(enum.Enum):
    """
    flag enum
    """
    PASS = "PASS"
    WARNING = "WARNING"
    FAILED = "FAILED"
    MISSING = "MISSING"
    NOT_APPLICABLE = "NOT_APPLICABLE"

    def to_json(self):
        """Serialize the enum to json"""
        return self.value


class AggregateEnum(enum.Enum):
    """
    aggregate enum
    """
    SUM = "SUM"
    AVERAGE = "AVERAGE"
    N = "N" # for NOT aggregating for metric at sample level

    def to_json(self):
        """Serialize the enum to json"""
        return self.value


class Base(DeclarativeBase):
    """
    Base declarative table
    """
    # this is needed for the enum to work properly right now
    # see https://github.com/sqlalchemy/sqlalchemy/discussions/8856
    type_annotation_map = {
        NucleicAcidTypeEnum: Enum(NucleicAcidTypeEnum),
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

job_file = Table(
    "job_file",
    Base.metadata,
    Column("file_id", ForeignKey("file.id"), primary_key=True),
    Column("job_id", ForeignKey("job.id"), primary_key=True),
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
        ext_id integer
        ext_src text
    """
    __abstract__ = True

    id: Mapped[int] = mapped_column(primary_key=True)
    deprecated: Mapped[bool] = mapped_column(default=False)
    deleted: Mapped[bool] = mapped_column(default=False)
    creation: Mapped[DateTime] = Column(DateTime(timezone=True), server_default=func.now())
    modification: Mapped[DateTime] = Column(DateTime(timezone=True), onupdate=func.now())
    extra_metadata: Mapped[dict] = mapped_column(mutable_json_type(dbtype=JSON, nested=True), default=None, nullable=True)
    ext_id: Mapped[int] = mapped_column(default=None, nullable=True)
    ext_src: Mapped[str] = mapped_column(default=None, nullable=True)

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
        Returning only ids of the referenced objects except for
        file where the locations details are also returned
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
            if self.__tablename__ == 'file' and key == 'locations':
                dumps[key] = [v.flat_dict for v in getattr(self,'locations')]

        dumps['tablename'] = self.__tablename__
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
        name text (unique)
        alias json
        deprecated boolean
        deleted boolean
        creation timestamp
        modification timestamp
        extra_metadata json
    """
    __tablename__ = "project"

    name: Mapped[str] = mapped_column(default=None, nullable=False, unique=True)
    alias: Mapped[dict] = mapped_column(mutable_json_type(dbtype=JSON, nested=True), default=None, nullable=True)

    specimens: Mapped[list["Specimen"]] = relationship(back_populates="project", cascade="all, delete")
    operations: Mapped[list["Operation"]] = relationship(back_populates="project", cascade="all, delete")


class Specimen(BaseTable):
    """
    Specimen:
        id integer [PK]
        project_id integer [ref: > project.id]
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
    __tablename__ = "specimen"

    project_id: Mapped[int] = mapped_column(ForeignKey("project.id"), default=None)
    name: Mapped[str] = mapped_column(default=None, nullable=False, unique=True)
    alias: Mapped[dict] = mapped_column(mutable_json_type(dbtype=JSON, nested=True), default=None, nullable=True)
    cohort: Mapped[str] = mapped_column(default=None, nullable=True)
    institution: Mapped[str] = mapped_column(default=None, nullable=True)

    project: Mapped["Project"] = relationship(back_populates="specimens")
    samples: Mapped[list["Sample"]] = relationship(back_populates="specimen", cascade="all, delete")

    @classmethod
    def from_name(cls, name, project, cohort=None, institution=None, session=None, deprecated=False, deleted=False):
        """
        get specimen if it exist, set it if it does not exist
        """
        if session is None:
            session = database.get_session()

        # Name is unique
        specimen = session.query(cls).filter(
            cls.name == name,
            cls.deprecated.is_(deprecated),
            cls.deleted.is_(deleted)
        ).first()

        if not specimen:
            specimen = cls(name=name, cohort=cohort, institution=institution, project=project)
            session.add(specimen)
        else:
            if specimen.project != project:
                logger.error(f"specimen {specimen.name} already in project {specimen.project}")

        return specimen


class Sample(BaseTable):
    """
    Sample:
        id integer [PK]
        specimen_id integer [ref: > specimen.id]
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

    specimen_id: Mapped[int] = mapped_column(ForeignKey("specimen.id"), default=None)
    name: Mapped[str] = mapped_column(default=None, nullable=False, unique=True)
    alias: Mapped[dict] = mapped_column(mutable_json_type(dbtype=JSON, nested=True), default=None, nullable=True)
    tumour: Mapped[bool] = mapped_column(default=False)

    specimen: Mapped["Specimen"] = relationship(back_populates="samples")
    readsets: Mapped[list["Readset"]] = relationship(back_populates="sample", cascade="all, delete")

    @classmethod
    def from_name(cls, name, specimen, tumour=None, session=None, deprecated=False, deleted=False):
        """
        get sample if it exist, set it if it does not exist
        """
        if not session:
            session = database.get_session()

        # Name is unique
        sample = session.query(cls).filter(
            cls.name == name,
            cls.deprecated.is_(deprecated),
            cls.deleted.is_(deleted)
        ).first()

        if not sample:
            sample = cls(name=name, specimen=specimen, tumour=tumour)
            session.add(sample)
        else:
            if sample.specimen != specimen:
                logger.error(f"sample {sample.specimen} already attatched to project {specimen.name}")

        return sample


class Experiment(BaseTable):
    """
    Experiment:
        id integer [PK]
        sequencing_technology text
        type text
        nucleic_acid_type nucleic_acid_type
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
    nucleic_acid_type: Mapped[NucleicAcidTypeEnum] = mapped_column(default=None, nullable=False)
    library_kit: Mapped[str] = mapped_column(default=None, nullable=True)
    kit_expiration_date: Mapped[datetime] = mapped_column(default=None, nullable=True)

    readsets: Mapped[list["Readset"]] = relationship(back_populates="experiment", cascade="all, delete")

    @classmethod
    def from_attributes(
        cls,
        nucleic_acid_type,
        sequencing_technology=None,
        type=None,
        library_kit=None,
        kit_expiration_date=None,
        session=None,
        deprecated=False,
        deleted=False
        ):
        """
        get experiment if it exist, set it if it does not exist
        """
        if not session:
            session = database.get_session()
        experiment = session.query(cls).filter(
            cls.sequencing_technology == sequencing_technology,
            cls.type == type,
            cls.nucleic_acid_type == nucleic_acid_type,
            cls.library_kit == library_kit,
            cls.kit_expiration_date == kit_expiration_date,
            cls.deprecated.is_(deprecated),
            cls.deleted.is_(deleted)
        ).first()

        if not experiment:
            experiment = cls(
                sequencing_technology=sequencing_technology,
                type=type,
                nucleic_acid_type=nucleic_acid_type,
                library_kit=library_kit,
                kit_expiration_date=kit_expiration_date
            )
            session.add(experiment)

        return experiment


class Run(BaseTable):
    """
    Specimen:
        id integer [PK]
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

    name: Mapped[str] = mapped_column(default=None, nullable=True)
    instrument: Mapped[str] = mapped_column(default=None, nullable=True)
    date: Mapped[datetime] = mapped_column(default=None, nullable=True)

    readsets: Mapped[list["Readset"]] = relationship(back_populates="run", cascade="all, delete")

    @classmethod
    def from_attributes(cls, ext_id=None, ext_src=None, name=None, instrument=None, date=None, session=None, deprecated=False, deleted=False):
        """
        get run if it exist, set it if it does not exist
        """
        if not session:
            session = database.get_session()
        run = session.query(cls).filter(
            cls.ext_id == ext_id,
            cls.ext_src == ext_src,
            cls.name == name,
            cls.instrument == instrument,
            cls.date == date,
            cls.deprecated.is_(deprecated),
            cls.deleted.is_(deleted)
        ).first()

        if not run:
            run = cls(
                ext_id=ext_id,
                ext_src=ext_src,
                name=name,
                instrument=instrument,
                date=date
            )
            session.add(run)

        return run


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
        state state
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
    alias: Mapped[dict] = mapped_column(mutable_json_type(dbtype=JSON, nested=True), default=None, nullable=True)
    lane: Mapped[LaneEnum]  =  mapped_column(default=None, nullable=True)
    adapter1: Mapped[str] = mapped_column(default=None, nullable=True)
    adapter2: Mapped[str] = mapped_column(default=None, nullable=True)
    sequencing_type: Mapped[SequencingTypeEnum] = mapped_column(default=None, nullable=True)
    state: Mapped[StateEnum] = mapped_column(default=StateEnum.VALID, nullable=True)

    sample: Mapped["Sample"] = relationship(back_populates="readsets")
    experiment: Mapped["Experiment"] = relationship(back_populates="readsets")
    run: Mapped["Run"] = relationship(back_populates="readsets")
    files: Mapped[list["File"]] = relationship(secondary=readset_file, back_populates="readsets")
    operations: Mapped[list["Operation"]] = relationship(secondary=readset_operation, back_populates="readsets")
    jobs: Mapped[list["Job"]] = relationship(secondary=readset_job, back_populates="readsets")
    metrics: Mapped[list["Metric"]] = relationship(secondary=readset_metric, back_populates="readsets")

    @classmethod
    def from_name(cls, name, sample, alias=None, session=None, deprecated=False, deleted=False):
        """
        get readset if it exist, set it if it does not exist
        """
        if session is None:
            session = database.get_session()

        # Name is unique
        readset = session.query(cls).filter(
            cls.name == name,
            cls.deprecated.is_(deprecated),
            cls.deleted.is_(deleted)
        ).first()

        if not readset:
            readset = cls(name=name, alias=alias, sample=sample)
            session.add(readset)
        else:
            if readset.sample != sample:
                logger.error(f"readset {readset.name} already attached to sample {sample.readset}")

        return readset


class Operation(BaseTable):
    """
    Operation:
        id integer [PK]
        operation_config_id integer [ref: > operation_config.id]
        reference_id integer [ref: > operation_config.id]
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

    project_id: Mapped[int] = mapped_column(ForeignKey("project.id"), default=None)
    operation_config_id: Mapped[int] = mapped_column(ForeignKey("operation_config.id"), default=None, nullable=True)
    reference_id: Mapped[int] = mapped_column(ForeignKey("reference.id"), default=None, nullable=True)
    platform: Mapped[str] = mapped_column(default=None, nullable=True)
    cmd_line: Mapped[str] = mapped_column(default=None, nullable=True)
    name: Mapped[str] = mapped_column(default=None, nullable=True)
    status: Mapped[StatusEnum] = mapped_column(default=StatusEnum.PENDING)

    operation_config: Mapped["OperationConfig"] = relationship(back_populates="operations")
    reference: Mapped["Reference"] = relationship(back_populates="operations")
    project: Mapped["Project"] = relationship(back_populates="operations")
    jobs: Mapped[list["Job"]] = relationship(back_populates="operation", cascade="all, delete")
    readsets: Mapped[list["Readset"]] = relationship(secondary=readset_operation, back_populates="operations")

    @classmethod
    def from_attributes(
        cls,
        operation_config,
        project,
        reference=None,
        platform=None,
        cmd_line=None,
        name=None,
        status=None,
        session=None,
        deprecated=False,
        deleted=False
        ):
        """
        get operation if it exist, set it if it does not exist
        """
        warning = None
        if not session:
            session = database.get_session()
        operation = session.query(cls).filter(
            cls.operation_config == operation_config,
            cls.reference == reference,
            cls.project == project,
            cls.platform == platform,
            cls.cmd_line == cmd_line,
            cls.name == name,
            cls.status == status,
            cls.deprecated.is_(deprecated),
            cls.deleted.is_(deleted)
        ).first()

        if operation:
            warning = f"Operation with id {operation.id} already exists, informations will be attached to this one."
        else:
            operation = cls(
                operation_config=operation_config,
                project=project,
                reference=reference,
                platform=platform,
                cmd_line=cmd_line,
                name=name,
                status=status
            )
            session.add(operation)

        return operation, warning

class Reference(BaseTable):
    """
    Reference:
        name text // scientific name
        alias text
        assembly text
        version test
        taxon_id text
        source text
        deprecated boolean
        deleted boolean
        creation timestamp
        modification timestamp
        extra_metadata json
    """
    __tablename__ = "reference"

    name: Mapped[str] = mapped_column(default=None, nullable=True)
    alias: Mapped[str] = mapped_column(default=None, nullable=True)
    assembly: Mapped[str] = mapped_column(default=None, nullable=True)
    version: Mapped[str] = mapped_column(default=None, nullable=True)
    taxon_id: Mapped[str] = mapped_column(default=None, nullable=True)
    source: Mapped[str] = mapped_column(default=None, nullable=True)

    operations: Mapped[list["Operation"]] = relationship(back_populates="reference", cascade="all, delete")


class OperationConfig(BaseTable):
    """
    OperationConfig:
        id integer [PK]
        file_id integer [ref: > file.id]
        name text
        version test
        md5sum text
        data bytes
        deprecated boolean
        deleted boolean
        creation timestamp
        modification timestamp
        extra_metadata json
    """
    __tablename__ = "operation_config"

    name: Mapped[str] = mapped_column(default=None, nullable=True)
    version: Mapped[str] = mapped_column(default=None, nullable=True)
    md5sum: Mapped[str] = mapped_column(unique=True, default=None, nullable=True)
    data: Mapped[bytes] = mapped_column(LargeBinary, default=None, nullable=True)

    operations: Mapped[list["Operation"]] = relationship(back_populates="operation_config", cascade="all, delete")

    @classmethod
    def from_attributes(cls, name=None, version=None, md5sum=None, data=None, session=None, deprecated=False, deleted=False):
        """
        get operation_config if it exist, set it if it does not exist
        """
        if not session:
            session = database.get_session()
        # Use ORM query
        operation_config = session.query(cls).filter(
            cls.name == name,
            cls.version == version,
            cls.md5sum == md5sum,
            cls.data == data,
            cls.deprecated.is_(deprecated),
            cls.deleted.is_(deleted)
        ).first()

        if not operation_config:
            operation_config = cls(
                name=name,
                version=version,
                md5sum=md5sum,
                data=data
            )
            session.add(operation_config)

        return operation_config


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

    operation: Mapped["Operation"] = relationship(back_populates="jobs")
    metrics: Mapped[list["Metric"]] = relationship(back_populates="job", cascade="all, delete")
    files: Mapped[list["File"]] = relationship(secondary=job_file,back_populates="jobs")
    readsets: Mapped[list["Readset"]] = relationship(secondary=readset_job, back_populates="jobs")


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

    name: Mapped[str] = mapped_column(nullable=False)
    job_id: Mapped[int] = mapped_column(ForeignKey("job.id"), default=None)
    value: Mapped[str] = mapped_column(default=None, nullable=True)
    flag: Mapped[FlagEnum] = mapped_column(default=None, nullable=True)
    deliverable: Mapped[bool] = mapped_column(default=False)
    aggregate: Mapped[AggregateEnum] = mapped_column(default=None, nullable=True)

    job: Mapped["Job"] = relationship(back_populates="metrics")
    readsets: Mapped[list["Readset"]] = relationship(secondary=readset_metric, back_populates="metrics")

    @classmethod
    def get_or_create(
        cls,
        name,
        readsets,
        value=None,
        flag=None,
        deliverable=False,
        job=None,
        session=None,
        deprecated=False,
        deleted=False
        ):
        """
        Retrieve or create a metric based on the provided name and value.

        This method checks if a metric with the given name and value already exists for the specified readset.
        If such a metric exists, it links the job and readset to the existing metric.
        If a metric with the same name but a different value exists, it deprecates the old metric and adds a warning.
        If no matching metric is found, it creates a new metric.

        Args:
            name (str): The name of the metric.
            readsets (list[Readset]): A list of readsets associated with the metric.
            value (str, optional): The value of the metric. Defaults to None.
            flag (FlagEnum, optional): The flag status of the metric. Defaults to None.
            deliverable (bool, optional): Indicates if the metric is deliverable. Defaults to False.
            job (Job, optional): The job associated with the metric. Defaults to None.
            session (Session, optional): The database session to use. Defaults to None.

        Returns:
            tuple: A tuple containing the metric instance and a warning message (if any).
        """
        if session is None:
            session = database.get_session()

        # Assuming readsets contains a single unique readset
        readset = readsets[0]

        # Combine checks into a single query
        metrics = session.query(cls).join(cls.readsets).filter(
            cls.name == name,
            cls.deprecated.is_(deprecated),
            cls.deleted.is_(deleted),
            Readset.id == readset.id
        ).all()

        warning = None
        metric = None
        for m in metrics:
            if m.value == value:
                metric = m
                break
            if m.value != value:
                m.deprecated = True
                warning = f"Metric '{name}' already exists for readset '{readset.id}' with a different value (old: '{m.value}', new: '{value})'. Deprecating the old metric."

        if metric:
            # Metric with the same name and value exists, link to job and readset
            metric.job = job
            if readset not in metric.readsets:
                metric.readsets.append(readset)
        else:
            # Create a new metric
            metric = cls(name=name, value=value, flag=flag, deliverable=deliverable, job=job, readsets=readsets)
            session.add(metric)

        return metric, warning


class Location(BaseTable):
    """
    Uri:
        id integer [PK]
        file_id integer [ref: > file.id]
        uri text
        endpoint text
        deliverable boolean
        deprecated boolean
        deleted boolean
        creation timestamp
        modification timestamp
        extra_metadata json
    """
    __tablename__ = "location"

    file_id: Mapped[int] = mapped_column(ForeignKey("file.id"), nullable=False)
    uri: Mapped[str] = mapped_column(nullable=False, unique=True)
    endpoint: Mapped[str] = mapped_column(nullable=False)
    deliverable: Mapped[bool] = mapped_column(default=False)

    file: Mapped["File"] = relationship(back_populates="locations")

    @classmethod
    def from_uri(cls, uri, file, endpoint=None, session=None, deprecated=False, deleted=False):
        """
        Sets endpoint from uri
        """
        if not session:
            session = database.get_session()

        location = session.query(cls).filter(
            cls.uri == uri,
            cls.deprecated.is_(deprecated),
            cls.deleted.is_(deleted)
        ).first()

        if not location:
            if endpoint is None:
                endpoint = uri.split(':///')[0]
            location = cls(uri=uri, file=file, endpoint=endpoint)
            session.add(location)

        return location


class File(BaseTable):
    """
    File:
        id integer [PK]
        name text
        type text
        md5sum txt
        deliverable boolean
        deprecated boolean
        deleted boolean
        creation timestamp
        modification timestamp
        extra_metadata json
    """
    __tablename__ = "file"

    name: Mapped[str] = mapped_column(nullable=False)
    type: Mapped[str] = mapped_column(default=None, nullable=True)
    md5sum: Mapped[str] = mapped_column(default=None, nullable=True)
    deliverable: Mapped[bool] = mapped_column(default=False)

    locations: Mapped[list["Location"]] = relationship(back_populates="file", cascade="all, delete")
    readsets: Mapped[list["Readset"]] = relationship(secondary=readset_file, back_populates="files")
    jobs: Mapped[list["Job"]] = relationship(secondary=job_file, back_populates="files")

    @classmethod
    def get_or_create(
        cls,
        name,
        readsets,
        jobs,
        type=None,
        md5sum=None,
        deliverable=False,
        extra_metadata=None,
        session=None,
        deprecated=False,
        deleted=False
        ):
        """
        Retrieve or create a file based on the provided attributes.

        This method checks if a file with the given name, type, and md5sum already exists for the specified readset and job.
        If such a file exists, it links the readset and job to the existing file.
        If a file with the same name but different md5sum exists, it deprecates the old file and adds a warning.
        If no matching file is found, it creates a new file.

        Args:
            name (str): The name of the file.
            readsets (list[Readset]): A list of readsets associated with the file.
            jobs (list[Job]): A list of jobs associated with the file.
            type (str, optional): The type of the file. Defaults to None.
            md5sum (str, optional): The MD5 checksum of the file. Defaults to None.
            deliverable (bool, optional): Indicates if the file is deliverable. Defaults to False.
            extra_metadata (dict, optional): Additional metadata for the file. Defaults to None.
            session (Session, optional): The database session to use. Defaults to None.

        Returns:
            tuple: A tuple containing the file instance and a warning message (if any).
        """
        if not session:
            session = database.get_session()

        # Assuming readsets and jobs each contain a single unique item
        readset = readsets[0]
        job = jobs[0]

        # Combine checks into a single query
        files = session.query(cls).join(cls.readsets).join(cls.jobs).filter(
            cls.name == name,
            cls.deprecated.is_(deprecated),
            cls.deleted.is_(deleted),
            Readset.id == readset.id,
            Job.id == job.id
        ).all()

        warning = None
        file = None
        for f in files:
            if f.md5sum == md5sum:
                file = f
                break
            if f.md5sum != md5sum:
                f.deprecated = True
                warning = f"Warning: File '{name}' already exists for readset '{readset.id}' and job '{job.id}' with a different MD5 checksum (old: '{f.md5sum}', new: '{md5sum}). Deprecating the old file."

        if file:
            # File with the same name and md5sum exists, link to readset and job
            if readset not in file.readsets:
                file.readsets.append(readset)
            if job not in file.jobs:
                file.jobs.append(job)
        else:
            # Create a new file
            file = cls(name=name, type=type, md5sum=md5sum, deliverable=deliverable, extra_metadata=extra_metadata, readsets=readsets, jobs=jobs)
            session.add(file)

        return file, warning
