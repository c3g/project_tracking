import inspect

from . import database
from .model import Project
from sqlalchemy import select


def projects():
    session = database.get_session()
    return [i[0] for i in session.execute((select(Project))).fetchall()]

def ingest_run_processing(ingest_data):
    print("PWET")
    return ingest_data

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
