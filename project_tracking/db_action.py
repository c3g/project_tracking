import inspect

from . import database
from .model import Project
from sqlalchemy import select


def projects():
    session = database.get_session()
    return [i[0] for i in session.execute((select(Project))).fetchall()]

def add_patient(engine, project_name, patient):
    """
    engine: engine
    patient: Table object ex. Patient(name="Robocop")
    project_name: String, project to link patient with
    """
    local_session = database.get_session()
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
    local_session = database.get_session()
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

