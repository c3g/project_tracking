"""Models for the database using SQLAlchemy ORM."""
from __future__ import annotations

import enum
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

from sqlalchemy import (
    Column,
    Index,
    ForeignKey,
    JSON,
    Enum,
    DateTime,
    Table,
    LargeBinary,
    select
    )

from sqlalchemy.orm import (
    relationship,
    DeclarativeBase,
    Mapped,
    mapped_column
    )

from sqlalchemy.sql import func

from sqlalchemy.dialects.postgresql import JSONB

from sqlalchemy.ext.mutable import MutableDict, MutableList

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
    DELIVERED = "DELIVERED"

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
    Index("ix_readset_file_file_id", "file_id"),
    Index("ix_readset_file_readset_id_file_id", "readset_id", "file_id"),
)


readset_metric = Table(
    "readset_metric",
    Base.metadata,
    Column("readset_id", ForeignKey("readset.id"), primary_key=True),
    Column("metric_id", ForeignKey("metric.id"), primary_key=True),
    Index("ix_readset_metric_metric_id", "metric_id"),
    Index("ix_readset_metric_readset_id_metric_id", "readset_id", "metric_id"),
)


readset_job = Table(
    "readset_job",
    Base.metadata,
    Column("readset_id", ForeignKey("readset.id"), primary_key=True),
    Column("job_id", ForeignKey("job.id"), primary_key=True),
    Index("ix_readset_job_job_id", "job_id"),
    Index("ix_readset_job_readset_id_job_id", "readset_id", "job_id"),
)


readset_operation = Table(
    "readset_operation",
    Base.metadata,
    Column("readset_id", ForeignKey("readset.id"), primary_key=True),
    Column("operation_id", ForeignKey("operation.id"), primary_key=True),
    Index("ix_readset_operation_operation_id", "operation_id"),
    Index("ix_readset_operation_readset_id_operation_id", "readset_id", "operation_id"),
)


job_file = Table(
    "job_file",
    Base.metadata,
    Column("job_id", ForeignKey("job.id"), primary_key=True),
    Column("file_id", ForeignKey("file.id"), primary_key=True),
    Index("ix_job_file_file_id", "file_id"),
    Index("ix_job_file_job_id_file_id", "job_id", "file_id"),
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
    # extra_metadata: Mapped[dict] = mapped_column(mutable_json_type(dbtype=JSON, nested=True), default=None, nullable=True)
    extra_metadata: Mapped[dict] = mapped_column(MutableDict.as_mutable(JSON().with_variant(JSONB, 'postgresql')), default=dict, nullable=True)
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

    # The following properties are not used for now but kep for legacy until next release
    # @property
    # def dict(self):
    #     """
    #     Dictionary of table columns *and* of the relation columns
    #     """
    #     dico = {}
    #     # select the column and the relationship
    #     selected_col = (
    #         x for x in dir(self.__class__) # loops over all attribute of the class
    #         if not x.startswith('_') and
    #         isinstance(getattr(self.__class__, x), attributes.InstrumentedAttribute) and
    #         getattr(self, x, False)   # To drop ref to join table that do exist in the class
    #     )
    #     for column in selected_col:
    #         # check in class if column is instrumented
    #         key = column
    #         val = getattr(self, column)
    #         dico[key] = val
    #     return dico

    # @property
    # def flat_dict(self):
    #     """
    #     Flat casting of object, to be used in flask responses
    #     Returning only ids of the referenced objects except for
    #     file where the locations details are also returned
    #     """
    #     dumps = {}
    #     try:
    #         loaded_keys = set(self.__dict__.keys())
    #         for key, val in self.dict.items():
    #             if key not in loaded_keys:
    #                 continue # Skip unloaded attributes

    #             if isinstance(val, datetime):
    #                 val = val.isoformat()
    #             elif isinstance(val, Decimal):
    #                 val = float(val)
    #             elif isinstance(val, set):
    #                 val = sorted(val)
    #             elif isinstance(val, (list, set, collections.List, collections.Set)):
    #                 val = sorted([e.id for e in val if hasattr(e, 'id')])
    #             elif isinstance(val, DeclarativeBase):
    #                 val = getattr(val, 'id', None)
    #             elif isinstance(val, enum.Enum):
    #                 val = val.value

    #             dumps[key] = val

    #             if self.__tablename__ == 'file' and key == 'locations' and key in loaded_keys:
    #                 dumps[key] = [v.flat_dict for v in getattr(self, 'locations', [])]

    #         dumps['tablename'] = self.__tablename__
    #     except DetachedInstanceError as e:
    #         class_name = type(self).__name__
    #         obj_address = hex(id(self))
    #         dumps = {"DB_ACTION_ERROR": f"DetachedInstanceError: Instance of {class_name} at memory address {obj_address} is not bound to a session. Cannot access unloaded attributes."}

    #     return dumps

    # @property
    # def dumps(self):
    #     """
    #     Dumping the flat_dict
    #     """
    #     return json.dumps(self.flat_dict)


class Project(BaseTable):
    """
    Project:
        id integer [PK]
        name text (unique)
        alias list
        deprecated boolean
        deleted boolean
        creation timestamp
        modification timestamp
        extra_metadata json
    """
    __tablename__ = "project"
    __table_args__ = (
        Index("ix_project_name", "name"),
    )

    name: Mapped[str] = mapped_column(default=None, nullable=False, unique=True)
    alias: Mapped[list] = mapped_column(MutableList.as_mutable(JSON().with_variant(JSONB, 'postgresql')), default=list, nullable=True)

    specimens: Mapped[list[Specimen]] = relationship(back_populates="project", cascade="all, delete")
    operations: Mapped[list[Operation]] = relationship(back_populates="project", cascade="all, delete")

class Specimen(BaseTable):
    """
    Specimen:
        id integer [PK]
        project_id integer [ref: > project.id]
        name text (unique)
        alias list
        cohort text
        institution text
        deprecated boolean
        deleted boolean
        creation timestamp
        modification timestamp
        extra_metadata json
    """
    __tablename__ = "specimen"
    __table_args__ = (
        Index("ix_specimen_project_id", "project_id"),
        Index("ix_specimen_name", "name"),
    )

    project_id: Mapped[int] = mapped_column(ForeignKey("project.id"), default=None)
    name: Mapped[str] = mapped_column(default=None, nullable=False, unique=True)
    alias: Mapped[list] = mapped_column(MutableList.as_mutable(JSON().with_variant(JSONB, 'postgresql')), default=list, nullable=True)
    cohort: Mapped[str] = mapped_column(default=None, nullable=True)
    institution: Mapped[str] = mapped_column(default=None, nullable=True)

    project: Mapped[Project] = relationship(back_populates="specimens")
    samples: Mapped[list[Sample]] = relationship(back_populates="specimen", cascade="all, delete")

    @property
    def readset_ids(self) -> list[int]:
        """
        Get all readset ids associated with the sample.
        """
        return list({r.id for s in self.samples for r in s.readsets if not r.deprecated or not r.deleted})

    @property
    def sample_ids(self) -> list[int]:
        """
        Get all sample ids associated with the specimen.
        """
        return list({s.id for s in self.samples if not s.deprecated or not s.deleted})

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
            # Create new specimen
            specimen = cls(name=name, cohort=cohort, institution=institution, project=project)
            session.add(specimen)
            session.flush()
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
        alias list
        tumour boolean
        deprecated boolean
        deleted boolean
        creation timestamp
        modification timestamp
        extra_metadata json
    """
    __tablename__ = "sample"
    __table_args__ = (
        Index("ix_sample_specimen_id", "specimen_id"),
        Index("ix_sample_name", "name"),
    )

    specimen_id: Mapped[int] = mapped_column(ForeignKey("specimen.id"), default=None)
    name: Mapped[str] = mapped_column(default=None, nullable=False, unique=True)
    alias: Mapped[list] = mapped_column(MutableList.as_mutable(JSON().with_variant(JSONB, 'postgresql')), default=list, nullable=True)
    tumour: Mapped[bool] = mapped_column(default=False)

    specimen: Mapped[Specimen] = relationship(back_populates="samples")
    readsets: Mapped[list[Readset]] = relationship(back_populates="sample", cascade="all, delete")

    @property
    def readset_ids(self) -> list[int]:
        """
        Get all readset ids associated with the sample.
        """
        return [r.id if not r.deprecated or not r.deleted else None for r in self.readsets]

    @classmethod
    def from_name(cls, name, specimen, alias=None, tumour=None, session=None, deprecated=False, deleted=False):
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
            # Create new sample
            sample = cls(name=name, specimen=specimen, alias=[alias], tumour=tumour)
            session.add(sample)
            session.flush()
        else:
            if alias:
                for a in alias:
                    if a not in sample.alias:
                        sample.alias.append(a)
                # session.flush()
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

    readsets: Mapped[list[Readset]] = relationship(back_populates="experiment", cascade="all, delete")

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
        warning = None
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

        if experiment:
            warning = f"Experiment with id {experiment.id} already exists, informations will be attached to this one."

        if not experiment:
            # Create new experiment
            experiment = cls(
                sequencing_technology=sequencing_technology,
                type=type,
                nucleic_acid_type=nucleic_acid_type,
                library_kit=library_kit,
                kit_expiration_date=kit_expiration_date
            )
            session.add(experiment)
            session.flush()

        return experiment, warning

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

    readsets: Mapped[list[Readset]] = relationship(back_populates="run", cascade="all, delete")

    @classmethod
    def from_attributes(cls, ext_id=None, ext_src=None, name=None, instrument=None, date=None, session=None, deprecated=False, deleted=False):
        """
        get run if it exist, set it if it does not exist
        """
        warning = None
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

        if run:
            warning = f"Run with id {run.id} and name {run.name} already exists, informations will be attached to this one."

        if not run:
            # Create new run
            run = cls(
                ext_id=ext_id,
                ext_src=ext_src,
                name=name,
                instrument=instrument,
                date=date
            )
            session.add(run)
            session.flush()

        return run, warning


class Readset(BaseTable):
    """
    Readset:
        id integer [PK]
        sample_id integer [ref: > sample.id]
        experiment_id  text [ref: > experiment.id]
        run_id integer [ref: > run.id]
        name text (unique)
        alias list
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
    __table_args__ = (
        Index("ix_readset_sample_id", "sample_id"),
        Index("ix_readset_experiment_id", "experiment_id"),
        Index("ix_readset_run_id", "run_id"),
        Index("ix_readset_name", "name"),
    )

    sample_id: Mapped[int] = mapped_column(ForeignKey("sample.id"), default=None)
    experiment_id: Mapped[int] = mapped_column(ForeignKey("experiment.id"), default=None)
    run_id: Mapped[int] = mapped_column(ForeignKey("run.id"), default=None)
    name: Mapped[str] = mapped_column(default=None, nullable=False, unique=True)
    alias: Mapped[list] = mapped_column(MutableList.as_mutable(JSON().with_variant(JSONB, 'postgresql')), default=list, nullable=True)
    lane: Mapped[LaneEnum]  =  mapped_column(default=None, nullable=True)
    adapter1: Mapped[str] = mapped_column(default=None, nullable=True)
    adapter2: Mapped[str] = mapped_column(default=None, nullable=True)
    sequencing_type: Mapped[SequencingTypeEnum] = mapped_column(default=None, nullable=True)
    state: Mapped[StateEnum] = mapped_column(default=StateEnum.VALID, nullable=True)

    sample: Mapped[Sample] = relationship(back_populates="readsets")
    experiment: Mapped[Experiment] = relationship(back_populates="readsets")
    run: Mapped[Run] = relationship(back_populates="readsets")
    files: Mapped[list[File]] = relationship(secondary=readset_file, back_populates="readsets")
    operations: Mapped[list[Operation]] = relationship(secondary=readset_operation, back_populates="readsets")
    jobs: Mapped[list[Job]] = relationship(secondary=readset_job, back_populates="readsets")
    metrics: Mapped[list[Metric]] = relationship(secondary=readset_metric, back_populates="readsets")

    @property
    def specimen_id(self):
        """
        Get the specimen id associated with the readset.
        """
        return self.sample.specimen.id if self.sample and self.sample.specimen and (not self.sample.specimen.deprecated or not self.sample.specimen.deleted) else None

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
            # Create new readset
            readset = cls(name=name, alias=alias, sample=sample)
            session.add(readset)
            session.flush()
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
    __table_args__ = (
        Index("ix_operation_project_id", "project_id"),
        Index("ix_operation_reference_id", "reference_id"),
        Index("ix_operation_operation_config_id", "operation_config_id"),
    )

    project_id: Mapped[int] = mapped_column(ForeignKey("project.id"), default=None)
    operation_config_id: Mapped[int] = mapped_column(ForeignKey("operation_config.id"), default=None, nullable=True)
    reference_id: Mapped[int] = mapped_column(ForeignKey("reference.id"), default=None, nullable=True)
    platform: Mapped[str] = mapped_column(default=None, nullable=True)
    cmd_line: Mapped[str] = mapped_column(default=None, nullable=True)
    name: Mapped[str] = mapped_column(default=None, nullable=True)
    status: Mapped[StatusEnum] = mapped_column(default=StatusEnum.PENDING)

    operation_config: Mapped[OperationConfig] = relationship(back_populates="operations")
    reference: Mapped[Reference] = relationship(back_populates="operations")
    project: Mapped[Project] = relationship(back_populates="operations")
    jobs: Mapped[list[Job]] = relationship(back_populates="operation", cascade="all, delete")
    readsets: Mapped[list[Readset]] = relationship(secondary=readset_operation, back_populates="operations")

    @property
    def readset_ids(self) -> list[int]:
        """
        Get all readset ids associated with the operation.
        """
        return [r.id if not r.deleted or not r.deprecated else None for r in self.readsets]

    @classmethod
    def from_attributes(
        cls,
        project,
        operation_config=None,
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
            warning = f"Operation with id {operation.id} and name {operation.name} already exists, informations will be attached to this one."
        else:
            # Create new operation
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
            session.flush()

        return operation, warning


class Reference(BaseTable):
    """
    Reference:
        name text // scientific name
        alias list
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
    alias: Mapped[list] = mapped_column(MutableList.as_mutable(JSON().with_variant(JSONB, 'postgresql')), default=list, nullable=True)
    assembly: Mapped[str] = mapped_column(default=None, nullable=True)
    version: Mapped[str] = mapped_column(default=None, nullable=True)
    taxon_id: Mapped[str] = mapped_column(default=None, nullable=True)
    source: Mapped[str] = mapped_column(default=None, nullable=True)

    operations: Mapped[list[Operation]] = relationship(back_populates="reference", cascade="all, delete")


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

    operations: Mapped[list[Operation]] = relationship(back_populates="operation_config", cascade="all, delete")

    @classmethod
    def from_attributes(cls, name=None, version=None, md5sum=None, data=None, session=None, deprecated=False, deleted=False):
        """
        get operation_config if it exist, set it if it does not exist
        """
        warning = None
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

        if operation_config:
            warning = f"OperationConfig with id {operation_config.id} and name {operation_config.name} already exists, informations will be attached to this one."

        if not operation_config:
            # Create new operation_config
            operation_config = cls(
                name=name,
                version=version,
                md5sum=md5sum,
                data=data
            )
            session.add(operation_config)
            session.flush()

        return operation_config, warning


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
    __table_args__ = (
        Index("ix_job_operation_id", "operation_id"),
        Index("ix_job_status", "status"),
        Index("ix_job_name", "name"),
    )

    operation_id: Mapped[int] = mapped_column(ForeignKey("operation.id"), default=None)
    name: Mapped[str] = mapped_column(default=None, nullable=True)
    start: Mapped[datetime] = mapped_column(default=None, nullable=True)
    stop: Mapped[datetime] = mapped_column(default=None, nullable=True)
    status: Mapped[StatusEnum] = mapped_column(default=None, nullable=True)
    type: Mapped[str] = mapped_column(default=None, nullable=True)

    operation: Mapped[Operation] = relationship(back_populates="jobs")
    metrics: Mapped[list[Metric]] = relationship(back_populates="job", cascade="all, delete")
    files: Mapped[list[File]] = relationship(secondary=job_file,back_populates="jobs")
    readsets: Mapped[list[Readset]] = relationship(secondary=readset_job, back_populates="jobs")

    # Not used for now but if the property readset_ids is too slow this classmethod might be faster
    @classmethod
    def get_readset_ids(cls, session, job_id):
        """
        Get all readset ids associated with the job.
        """
        stmt = (
            select(Readset.id)
            .join(readset_job, Readset.id == readset_job.c.readset_id)
            .where(readset_job.c.job_id == job_id)
        )
        return [row[0] for row in session.execute(stmt)]

    @property
    def readset_ids(self) -> list[int]:
        """
        Get all readset ids associated with the job.
        """
        return [r.id if not r.deprecated or not r.deleted else None for r in self.readsets]

    @classmethod
    def from_attributes(
        cls,
        operation,
        name=None,
        start=None,
        stop=None,
        status=None,
        type=None,
        session=None,
        deprecated=False,
        deleted=False
        ):
        """
        get job if it exist, set it if it does not exist
        """
        warning = None
        if not session:
            session = database.get_session()
        job = session.query(cls).filter(
            cls.operation == operation,
            cls.name == name,
            cls.start == start,
            cls.stop == stop,
            cls.status == status,
            cls.type == type,
            cls.deprecated.is_(deprecated),
            cls.deleted.is_(deleted)
        ).first()

        if job:
            warning = f"Job with id {job.id} and name {job.name} already exists, informations will be attached to this one."

        if not job:
            # Create new job
            job = cls(
                operation=operation,
                name=name,
                start=start,
                stop=stop,
                status=status,
                type=type
            )
            session.add(job)
            session.flush()

        return job, warning

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
    __table_args__ = (
        Index("ix_metric_job_id", "job_id"),
        Index("ix_metric_name", "name"),
    )

    name: Mapped[str] = mapped_column(nullable=False)
    job_id: Mapped[int] = mapped_column(ForeignKey("job.id"), default=None)
    value: Mapped[str] = mapped_column(default=None, nullable=True)
    flag: Mapped[FlagEnum] = mapped_column(default=None, nullable=True)
    deliverable: Mapped[bool] = mapped_column(default=False)
    aggregate: Mapped[AggregateEnum] = mapped_column(default=None, nullable=True)

    job: Mapped[Job] = relationship(back_populates="metrics")
    readsets: Mapped[list[Readset]] = relationship(secondary=readset_metric, back_populates="metrics")

    @property
    def readset_ids(self) -> list[int]:
        """
        List of readset ids associated with the metric
        """
        return list({r.id for r in self.readsets if not r.deprecated or not r.deleted})

    @property
    def sample_ids(self) -> list[int]:
        """
        List of sample ids associated with the metric through readsets
        """
        return list({r.sample.id for r in self.readsets if r.sample and (not r.sample.deprecated or not r.sample.deleted)})

    @property
    def specimen_ids(self) -> list[int]:
        """
        List of specimen ids associated with the metric through readsets and samples
        """
        return list({r.sample.specimen.id for r in self.readsets if r.sample and r.sample.specimen and (not r.sample.specimen.deprecated or not r.sample.specimen.deleted)})

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
        deleted=False,
        dont_merge=False
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

        # If dont_merge is True, always create a new metric
        if dont_merge:
            # Create a new metric
            metric = cls(name=name, value=value, flag=flag, deliverable=deliverable, job=job, readsets=readsets)
            session.add(metric)
            session.flush()
            return metric, None

        # Combine checks into a single query
        stmt = (
            select(cls)
            .distinct()
            .join(cls.readsets)
            .join(cls.job)
            .where(
                cls.name == name,
                cls.deprecated.is_(deprecated),
                cls.deleted.is_(deleted),
                Job.id == job.id
                # Readset.id.in_([readset.id])
            )
        )
        metrics = session.execute(stmt).scalars().all()

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
            session.flush()

        return metric, warning


class File(BaseTable):
    """
    File:
        id integer [PK]
        name text
        type text
        md5sum txt
        deliverable boolean
        state state
        deprecated boolean
        deleted boolean
        creation timestamp
        modification timestamp
        extra_metadata json
    """
    __tablename__ = "file"
    __table_args__ = (
        Index("ix_file_name", "name"),
        Index("ix_file_md5sum", "md5sum"),
    )

    name: Mapped[str] = mapped_column(nullable=False)
    type: Mapped[str] = mapped_column(default=None, nullable=True)
    md5sum: Mapped[str] = mapped_column(default=None, nullable=True)
    deliverable: Mapped[bool] = mapped_column(default=False)
    state: Mapped[StateEnum] = mapped_column(default=StateEnum.VALID, nullable=True)

    locations: Mapped[list[Location]] = relationship(back_populates="file", cascade="all, delete")
    readsets: Mapped[list[Readset]] = relationship(secondary=readset_file, back_populates="files")
    jobs: Mapped[list[Job]] = relationship(secondary=job_file, back_populates="files")

    @property
    def readset_ids(self) -> list[int]:
        """
        List of readset ids associated with the metric
        """
        return list({r.id for r in self.readsets if not r.deprecated or not r.deleted})

    @property
    def sample_ids(self) -> list[int]:
        """
        List of sample ids associated with the metric through readsets
        """
        return list({r.sample.id for r in self.readsets if r.sample and (not r.sample.deprecated or not r.sample.deleted)})

    @property
    def specimen_ids(self) -> list[int]:
        """
        List of specimen ids associated with the metric through readsets and samples
        """
        return list({r.sample.specimen.id for r in self.readsets if r.sample and r.sample.specimen and (not r.sample.specimen.deprecated or not r.sample.specimen.deleted)})

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
        stmt = (
            select(cls)
            .distinct()
            .join(cls.readsets)
            .join(cls.jobs)
            .where(
                cls.name == name,
                cls.deprecated.is_(deprecated),
                cls.deleted.is_(deleted),
                Job.id.in_([job.id])
            )
        )
        files = session.execute(stmt).scalars().all()
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
            session.flush()

        return file, warning


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
    __table_args__ = (
        Index("ix_location_file_id", "file_id"),
    )

    file_id: Mapped[int] = mapped_column(ForeignKey("file.id"), nullable=False)
    uri: Mapped[str] = mapped_column(nullable=False, unique=True)
    endpoint: Mapped[str] = mapped_column(nullable=False)
    deliverable: Mapped[bool] = mapped_column(default=False)

    file: Mapped[File] = relationship(back_populates="locations")

    @classmethod
    def from_uri(cls, uri, file, endpoint=None, session=None, deprecated=False, deleted=False):
        """
        Sets endpoint from uri
        """
        if not session:
            session = database.get_session()

        warning = None
        stmt = (
            select(cls)
            .where(
                cls.uri == uri
            )
        )
        location = session.execute(stmt).scalar_one_or_none()

        if not location:
            # Create new location
            if endpoint is None:
                endpoint = uri.split(':///')[0]
            location = cls(uri=uri, file=file, endpoint=endpoint)
            session.add(location)
            session.flush()
        else:
            warning = f"Warning: Location with id {location.id} and uri {location.uri} already exists, informations will be attached to this one."

        return location, warning
