import json
from project_tracking import database, db_actions
from project_tracking.model import Specimen, Sample

# Helper function to reset database state
def reset_database(session, run_processing_json):
    """Re-ingest or recreate necessary entities for tests."""
    db_actions.create_project("TestProject", session=session)
    project_id = db_actions.name_to_id("Project", "TestProject", session=session)
    db_actions.ingest_run_processing(project_id, run_processing_json, session=session)

def test_curate_route_app(client, app, run_processing_json):
    """Test the /curate route using app session."""
    with app.app_context():
        reset_database(database.get_session(), run_processing_json)
        session = database.get_session()

        response = client.post('/modification/curate', json={
            "modification": [{"table": "Specimen", "id": [1]}]
        })
        assert response.status_code == 200

        specimen = session.query(Specimen).filter_by(id=1).first()
        assert specimen is None

def test_edit_route_app(client, app, run_processing_json):
    """Test the /edit route using app session."""
    with app.app_context():
        reset_database(database.get_session(), run_processing_json)
        session = database.get_session()

        response = client.post('/modification/edit', json={
            "modification": [{"table": "Specimen", "id": [1], "column": "name", "new": "UpdatedName"}]
        })
        assert response.status_code == 200

        specimen = session.query(Specimen).filter_by(id=1).first()
        assert specimen.name == "UpdatedName"

def test_delete_route_app(client, app, run_processing_json):
    """Test the /delete route and validate database changes."""
    with app.app_context():
        reset_database(database.get_session(), run_processing_json)
        session = database.get_session()

        response = client.post('/modification/delete', json={
            "modification": [{"table": "Specimen", "id": [1]}]
        })
        assert response.status_code == 200

        specimen = session.query(Specimen).filter_by(id=1).first()
        assert specimen.deleted is True  # Specimen should be marked as deleted

        response = client.post('/modification/undelete', json={
            "modification": [{"table": "Specimen", "id": [1]}]
        })
        assert response.status_code == 200

        session = database.get_session()
        specimen = session.query(Specimen).filter_by(id=1).first()
        assert specimen.deleted is False  # Specimen should be undeleted

def test_deprecate_route_app(client, app, run_processing_json):
    """Test the /deprecate route and validate database changes."""
    with app.app_context():
        reset_database(database.get_session(), run_processing_json)
        session = database.get_session()

        response = client.post('/modification/deprecate', json={
            "modification": [{"table": "Specimen", "id": [1]}]
        })
        assert response.status_code == 200

        # Validate database changes
        specimen = session.query(Specimen).filter_by(id=1).first()
        assert specimen.deprecated is True  # Specimen should be marked as deprecated

        response = client.post('/modification/undeprecate', json={
            "modification": [{"table": "Specimen", "id": [1]}]
        })
        assert response.status_code == 200

        # Validate database changes
        specimen = session.query(Specimen).filter_by(id=1).first()
        assert specimen.deprecated is False  # Specimen should be undeprecated

def test_cascade_delete_app(client, app, run_processing_json):
    """Test cascade delete modification."""
    with app.app_context():
        reset_database(database.get_session(), run_processing_json)
        session = database.get_session()
        response = client.post('/modification/delete', data=json.dumps({
            "modification": [{"table": "Sample", "id": [1, 2]}],
            "cascade": True
        }))
        assert response.status_code == 200
        samples = session.query(Sample).filter(Sample.specimen_id == 1).all()
        for sample in samples:
            assert sample.deleted is True
        specimen = session.query(Specimen).filter_by(id=1).first()
        assert specimen.deleted is True

        response = client.post('/modification/undelete', json={
            "modification": [{"table": "Sample", "id": [1, 2]}],
            "cascade": True
        })
        assert response.status_code == 200
        samples = session.query(Sample).filter(Sample.specimen_id == 1).all()
        for sample in samples:
            assert sample.deleted is False
        specimen = session.query(Specimen).filter_by(id=1).first()
        assert specimen.deleted is False

def test_cascade_deprecate(client, app, run_processing_json):
    """Test cascade deprecate modification."""
    with app.app_context():
        reset_database(database.get_session(), run_processing_json)
        session = database.get_session()
        response = client.post('/modification/deprecate', data=json.dumps({
            "modification": [{"table": "Sample", "id": [1, 2]}],
            "cascade": True
        }))
        assert response.status_code == 200
        samples = session.query(Sample).filter(Sample.specimen_id == 1).all()
        for sample in samples:
            assert sample.deprecated is True
        specimen = session.query(Specimen).filter_by(id=1).first()
        assert specimen.deprecated is True

        response = client.post('/modification/undeprecate', json={
            "modification": [{"table": "Sample", "id": [1, 2]}],
            "cascade": True
        })
        assert response.status_code == 200
        samples = session.query(Sample).filter(Sample.specimen_id == 1).all()
        for sample in samples:
            assert sample.deprecated is False
        specimen = session.query(Specimen).filter_by(id=1).first()
        assert specimen.deprecated is False
