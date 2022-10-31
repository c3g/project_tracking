"""Module providing database tables and operations support."""
from sqlalchemy import (
    Column,
    ForeignKey,
    Integer,
    Boolean,
    String,
    JSON,
    DateTime,
    select
    )
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker
from sqlalchemy.inspection import inspect

Base = declarative_base()
# session = sessionmaker()(bind=engine)

class BaseTable(Base):
    """
    Define fields common of all tables in database
    BaseTable:
        id integer [PK]
        deprecated boolean
        deleted boolean
        extra_metadata json
    """
    __abstract__ = True

    id = Column(Integer, primary_key=True)
    deprecated = Column(Boolean, default=False)
    deleted = Column(Boolean, default=False)
    extra_metadata = Column(JSON, nullable=True, default=None)

    # def __init__(self, deleted=False, extra_metadata=None):
    #     self.deleted = deleted
    #     self.extra_metadata = extra_metadata

    # def insert(self, engine):
    #     """
    #     pwet
    #     """
    #     local_session = sessionmaker(autoflush=False, autocommit=False, bind=engine)
    #     # With this we get a session to do whatever we want to do
    #     session = local_session()

    #     # new_project = self.Project
    #     try:
    #         session.add(self)
    #         session.commit()
    #     except Exception as error:
    #         print(f"Error: {error}")
    #         session.rollback()

class Project(BaseTable):
    """
    Project:
        id integer [PK]
        fms_id integer
        name text (unique)
        deprecated boolean
        deleted boolean
        extra_metadata json
    """
    __tablename__ = "project"

    fms_id = Column(String, nullable=True, default=None)
    name = Column(String, nullable=False, unique=True, default=None)

    # def __init__(self, name=None):
    #     super().__init__()
    #     self.name = name

    def __repr__(self):
        return f"Project({self.fms_id!r}, {self.name!r}, {self.deprecated!r}, {self.deleted!r}, {self.extra_metadata!r})"

    # def insert(self, engine):
    #     """
    #     pwet
    #     """
    #     local_session = sessionmaker(autoflush=False, autocommit=False, bind=engine)
    #     # With this we get a session to do whatever we want to do
    #     session = local_session()

    #     # new_project = self.Project
    #     try:
    #         session.add(self)
    #         session.commit()
    #     except Exception as error:
    #         print(f"Error: {error}")
    #         session.rollback()

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
        extra_metadata json
    """
    __tablename__ = "patient"

    project_id = Column(Integer, ForeignKey("project.id"))
    fms_id = Column(String, nullable=True, default=None)
    name = Column(String, nullable=False, unique=True, default=None)
    alias = Column(String, nullable=True, default=None)
    cohort = Column(String, nullable=True, default=None)
    institution = Column(String, nullable=True, default=None)

    project = relationship("Project", backref="patient", lazy=False)

    # def __init__(self):
    #     super().__init__()
    #     self.project = relationship("Project", backref="patient", lazy=False)
    # def __init__(self, name=None, alias=None, cohort=None, institution=None, project=Project()):
    #     super().__init__()
    #     self.name = name
    #     self.alias = alias
    #     self.cohort = cohort
    #     self.institution = institution
    #     self.project = project

    def __repr__(self):
        return f"Patient({self.project!r}, {self.fms_id!r}, {self.name!r}, {self.alias!r}, {self.cohort!r}, {self.institution!r}, {self.deprecated!r}, {self.deleted!r}, {self.extra_metadata!r})"

class Experiment(BaseTable):
    """
    Experiment:
        id integer [PK]
        project_id integer [ref: > project.id]
        sequencing_technology text
        deprecated boolean
        deleted boolean
        extra_metadata json
    """
    __tablename__ = "experiment"

    # run_id = Column(Integer, ForeignKey("run.id"))
    project_id = Column(Integer, ForeignKey("project.id"))
    sequencing_technology = Column(String, nullable=True, default=None)

    run = relationship('Run', secondary='experiment_run')
    project = relationship("Project", backref="experiment", lazy=False)

    # def __init__(self, sequencing_technology=None):
    #     super().__init__()
    #     self.sequencing_technology = sequencing_technology

    def __repr__(self):
        return f"Experiment({self.run!r}, {self.project!r}, {self.sequencing_technology!r}, {self.deprecated!r}, {self.deleted!r}, {self.extra_metadata!r})"

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
        extra_metadata json
    """
    __tablename__ = "sample"

    patient_id = Column(Integer, ForeignKey("patient.id"))
    experiment_id = Column(Integer, ForeignKey("experiment.id"))
    fms_id = Column(String, nullable=True, default=None)
    name = Column(String, nullable=False, unique=True, default=None)
    tumour = Column(Boolean, default=False)
    alias = Column(String, nullable=True, default=None)

    patient = relationship("Patient", backref="sample", lazy=False)
    experiment = relationship("Experiment", backref="sample", lazy=False)

    # def __init__(self, name=None, tumour=False, alias=None):
    #     super().__init__()
    #     self.name = name
    #     self.tumour = tumour
    #     self.alias = alias

    def __repr__(self):
        return f"Sample({self.patient!r}, {self.experiment!r}, {self.fms_id!r}, {self.name!r}, {self.tumour!r}, {self.alias!r}, {self.deprecated!r}, {self.deleted!r}, {self.extra_metadata!r})"

class ExperimentRun(Base):
    """
    ExperimentRun:
        experiment_id integer [PK, ref: > experiment.id]
        run_id integer [PK, ref: > run.id]
    """
    __tablename__ = "experiment_run"

    experiment_id = Column(Integer, ForeignKey("experiment.id"), primary_key=True)
    run_id = Column(Integer, ForeignKey("run.id"), primary_key=True)

    # def __repr__(self):
    #     return f"ExperimentRun({self.})"

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
        extra_metadata json
    """
    __tablename__ = "run"

    fms_id = Column(String, nullable=True, default=None)
    lab_id = Column(String, nullable=True, default=None)
    name = Column(String, nullable=False, unique=True, default=None)
    date = Column(DateTime, nullable=True, default=None)

    experiment = relationship('Experiment', secondary='experiment_run')

    # def __init__(self, lab_id=None, name=None, date=None):
    #     super().__init__()
    #     self.lab_id = lab_id
    #     self.name = name
    #     self.date = date

    def __repr__(self):
        return f"Run({self.experiment!r}, {self.fms_id!r}, {self.lab_id!r}, {self.name!r}, {self.date!r}, {self.deprecated!r}, {self.deleted!r}, {self.extra_metadata!r})"

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
        extra_metadata json
    """
    __tablename__ = "readset"

    sample_id = Column(Integer, ForeignKey("sample.id"))
    run_id = Column(Integer, ForeignKey("run.id"))
    name = Column(String, nullable=False, unique=True, default=None)
    lane = Column(String, nullable=True, default=None)
    adapter1 = Column(String, nullable=True, default=None)
    adapter2 = Column(String, nullable=True, default=None)
    sequencing_type = Column(String, nullable=True, default=None)
    quality_offset = Column(String, nullable=True, default=None)
    alias = Column(String, nullable=True, default=None)

    sample = relationship("Sample", backref="readset", lazy=False)
    run = relationship("Run", backref="readset", lazy=False)

    # def __init__(self, name=None, lane=None, adapter1=None, adapter2=None, sequencing_type=None, quality_offset=None, alias=None):
    #     super().__init__()
    #     self.name = name
    #     self.lane = lane
    #     self.adapter1 = adapter1
    #     self.adapter2 = adapter2
    #     self.sequencing_type = sequencing_type
    #     self.quality_offset = quality_offset
    #     self.alias = alias

    def __repr__(self):
        return f"Readset({self.sample!r}, {self.run!r}, {self.name!r}, {self.lane!r}, {self.adapter1!r}, {self.adapter2!r}, {self.sequencing_type!r}, {self.quality_offset!r}, {self.alias!r}, {self.deprecated!r}, {self.deleted!r}, {self.extra_metadata!r})"

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
        extra_metadata json
    """
    __tablename__ = "step"

    sample_id = Column(Integer, ForeignKey("sample.id"))
    readset_id = Column(Integer, ForeignKey("readset.id"))
    name = Column(String, nullable=False, default=None)
    status = Column(String, nullable=True, default=None)

    sample = relationship("Sample", backref="step", lazy=False)
    readset = relationship("Readset", backref="step", lazy=False)

    # def __init__(self, name=None, status=None):
    #     super().__init__()
    #     self.name = name
    #     self.status = status

    def __repr__(self):
        return f"Step({self.sample!r}, {self.readset!r}, {self.name!r}, {self.status!r}, {self.deprecated!r}, {self.deleted!r}, {self.extra_metadata!r})"

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
        extra_metadata json
    """
    __tablename__ = "job"

    step_id = Column(Integer, ForeignKey("step.id"))
    name = Column(String, nullable=False, default=None)
    start = Column(DateTime, nullable=True, default=None)
    stop = Column(DateTime, nullable=True, default=None)
    status = Column(String, nullable=True, default=None)
    type = Column(String, nullable=True, default=None)

    step = relationship("Step", backref="job", lazy=False)

    # def __init__(self, name=None, start=None, stop=None, status=None, type=None):
    #     super().__init__()
    #     self.name = name
    #     self.start = start
    #     self.stop = stop
    #     self.status = status
    #     self.type = type

    def __repr__(self):
        return f"Job({self.step!r}, {self.name!r}, {self.start!r}, {self.stop!r}, {self.status!r}, {self.type!r}, {self.deprecated!r}, {self.deleted!r}, {self.extra_metadata!r})"

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
        extra_metadata json
    """
    __tablename__ = "metric"

    job_id = Column(Integer, ForeignKey("job.id"))
    name = Column(String, nullable=False, default=None)
    value = Column(String, nullable=True, default=None)
    flag = Column(String, nullable=True, default=None)
    deliverable = Column(Boolean, default=False)
    aggregate = Column(String, nullable=True, default=None)

    job = relationship("Job", backref="metric", lazy=False)

    # def __init__(self, name=None, value=None, flag=None):
    #     super().__init__()
    #     self.name = name
    #     self.value = value
    #     self.flag = flag

    def __repr__(self):
        return f"Metric({self.job!r}, {self.name!r}, {self.value!r}, {self.flag!r}, {self.deliverable!r}, {self.aggregate!r}, {self.deprecated!r}, {self.deleted!r}, {self.extra_metadata!r})"

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
        extra_metadata json
    """
    __tablename__ = "file"

    job_id = Column(Integer, ForeignKey("job.id"))
    path = Column(String, nullable=True, default=None)
    type = Column(String, nullable=True, default=None)
    description = Column(String, nullable=True, default=None)
    creation = Column(DateTime, nullable=True, default=None)
    deliverable = Column(Boolean, default=False)

    job = relationship("Job", backref="file", lazy=False)

    # def __init__(self, path=None, type=None, description=None, creation=None):
    #     super().__init__()
    #     self.path = path
    #     self.type = type
    #     self.description = description
    #     self.creation = creation

    def __repr__(self):
        return f"File({self.job!r}, {self.path!r}, {self.type!r}, {self.description!r}, {self.creation!r}, {self.deliverable!r}, {self.deprecated!r}, {self.deleted!r}, {self.extra_metadata!r})"

class Tool(BaseTable):
    """
    Tool:
        id integer [PK]
        job_id integer [ref: > job.id]
        name text
        version text
        deprecated boolean
        deleted boolean
        extra_metadata json
    """
    __tablename__ = "tool"

    job_id = Column(Integer, ForeignKey("job.id"))
    name = Column(String, nullable=True, default=None)
    version = Column(String, nullable=True, default=None)

    job = relationship("Job", backref="tool", lazy=False)

    # def __init__(self, path=None, type=None, description=None, creation=None):
    #     super().__init__()
    #     self.path = path
    #     self.type = type
    #     self.description = description
    #     self.creation = creation

    def __repr__(self):
        return f"Tool({self.job!r}, {self.name!r}, {self.version!r}, {self.deprecated!r}, {self.deleted!r}, {self.extra_metadata!r})"

def add_patient(engine, project_name, patient):
    """
    engine: engine
    patient: Table object ex. Patient(name="Robocop")
    project_name: String, project to link patient with
    """
    local_session = sessionmaker(autoflush=False, autocommit=False, bind=engine)
    # With this we get a session to do whatever we want to do
    session = local_session()

    existing_project = session.query(Project).filter(Project.name == project_name).all()
    if len(existing_project) == 0:
        project = Project(name=project_name)
        project.patient = [patient]
        try:
            session.add(project)
            session.commit()
        except Exception as error:
            print(f"Error: {error}")
            session.rollback()
    else:
        project = existing_project[0]
        project.patient.append(patient)
        try:
            session.merge(project)
            session.commit()
        except Exception as error:
            print(f"Error: {error}")
            session.rollback()


def insert(engine, update, entry, *relations):
    """
    engine: Engine instance from sqlalchemy.engine
    update: True/False - if True updates an existing entry
    entry: Table object ex. Patient(name="Zbla")
    *relations: Table object having a relation with entry ex. Project(name="Pwet")
    """
    local_session = sessionmaker(autoflush=False, autocommit=False, bind=engine)
    session = local_session()

    # for relation in i.relationships:
        # print(relation.direction.name)
        # print(relation.remote_side)
        # print(relation._reverse_property)
        # dir(relation)
    # Get all the child context relationships
    # parent_name = type(entry).__table__.name
    # for rel in inspect(type(entry)).relationships:
    #     print(rel.mapper.class_.__table__)
    # print(inspect(type(entry)).relationships)
    # clss = [rel.mapper.class_ for rel in inspect(type(entry)).relationships]
    # print(clss)
    # rels = [list(rel._calculated_foreign_keys)[0] for rel in inspect(type(entry)).relationships if r.back_populates == parent_name]
    # for rel in rels:
        # foreign_key = list(rel._calculated_foreign_keys)[0]

    if update:
        stmt = select(type(entry))
        for attr, _ in inspect(entry.__class__).c.items():
            value = getattr(entry, attr)
            if value:
                stmt = stmt.where(getattr(type(entry), attr) == value)
                # entry_dict[attr] = current_attr
                # print(attr, current_attr)
        entry = session.execute(stmt).first()[0]

    flag = False
    for relation_entry in relations:
        stmt = select(type(relation_entry))
        for attr, _ in inspect(relation_entry.__class__).c.items():
            value = getattr(relation_entry, attr)
            if value:
                stmt = stmt.where(getattr(type(relation_entry), attr) == value)
        existing_relation = session.execute(stmt).first()
        if not existing_relation:
            setattr(entry, type(relation_entry).__table__.name, relation_entry)
            # try:
            #     session.add(entry)
            #     session.commit()
            # except Exception as error:
            #     print(f"Error: {error}")
            #     session.rollback()
        else:
            flag = True
            existing_relation = existing_relation[0]
            setattr(entry, type(relation_entry).__table__.name + "_id", existing_relation.id)
            # try:
            #     session.merge(entry)
            #     session.commit()
            # except Exception as error:
            #     print(f"Error: {error}")
            #     session.rollback()
    if flag:
        try:
            session.merge(entry)
            session.commit()
        except Exception as error:
            print(f"Error: {error}")
            session.rollback()
    else:
        try:
            session.add(entry)
            session.commit()
        except Exception as error:
            print(f"Error: {error}")
            session.rollback()


def init_db():
    db = get_db()

    with current_app.open_resource('schema.sql') as f:
        db.executescript(f.read().decode('utf8'))

@click.command('init-db')
def init_db_command():
    """Clear the existing data and create new tables."""
    init_db()
    click.echo('Initialized the database.')

def init_app(app):
    app.teardown_appcontext(close_db)
    app.cli.add_command(init_db_command)
