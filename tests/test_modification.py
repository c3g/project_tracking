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
    """Test the /curate route using app session: a specimen with samples needs a cascade that reaches them."""
    with app.app_context():
        reset_database(database.get_session(), run_processing_json)
        session = database.get_session()

        response = client.post('/project/TestProject/modification/curate', json={
            "modification": [{"table": "Specimen", "id": [1]}]
        })
        assert "Nothing was curated" in response.get_json()["DB_ACTION_ERROR"]
        response = client.post('/project/TestProject/modification/curate', json={
            "modification": [{"table": "Specimen", "id": [1]}], "cascade_down": True
        })
        assert response.status_code == 200

        specimen = session.query(Specimen).filter_by(id=1).first()
        assert specimen is None

def test_edit_route_app(client, app, run_processing_json):
    """Test the /edit route using app session."""
    with app.app_context():
        reset_database(database.get_session(), run_processing_json)
        session = database.get_session()

        response = client.post('/project/TestProject/modification/edit', json={
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

        response = client.post('/project/TestProject/modification/delete', json={
            "modification": [{"table": "Specimen", "id": [1]}]
        })
        assert response.status_code == 200

        specimen = session.query(Specimen).filter_by(id=1).first()
        assert specimen.deleted is True  # Specimen should be marked as deleted

        response = client.post('/project/TestProject/modification/undelete', json={
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

        response = client.post('/project/TestProject/modification/deprecate', json={
            "modification": [{"table": "Specimen", "id": [1]}]
        })
        assert response.status_code == 200

        # Validate database changes
        specimen = session.query(Specimen).filter_by(id=1).first()
        assert specimen.deprecated is True  # Specimen should be marked as deprecated

        response = client.post('/project/TestProject/modification/undeprecate', json={
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
        response = client.post('/project/TestProject/modification/delete', data=json.dumps({
            "modification": [{"table": "Sample", "id": [1, 2]}],
            "cascade": True
        }))
        assert response.status_code == 200
        samples = session.query(Sample).filter(Sample.specimen_id == 1).all()
        for sample in samples:
            assert sample.deleted is True
        specimen = session.query(Specimen).filter_by(id=1).first()
        assert specimen.deleted is True

        response = client.post('/project/TestProject/modification/undelete', json={
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
        response = client.post('/project/TestProject/modification/deprecate', data=json.dumps({
            "modification": [{"table": "Sample", "id": [1, 2]}],
            "cascade": True
        }))
        assert response.status_code == 200
        samples = session.query(Sample).filter(Sample.specimen_id == 1).all()
        for sample in samples:
            assert sample.deprecated is True
        specimen = session.query(Specimen).filter_by(id=1).first()
        assert specimen.deprecated is True

        response = client.post('/project/TestProject/modification/undeprecate', json={
            "modification": [{"table": "Sample", "id": [1, 2]}],
            "cascade": True
        })
        assert response.status_code == 200
        samples = session.query(Sample).filter(Sample.specimen_id == 1).all()
        for sample in samples:
            assert sample.deprecated is False
        specimen = session.query(Specimen).filter_by(id=1).first()
        assert specimen.deprecated is False


def test_edit_refuses_ids_links_and_unknown_columns(client, app, run_processing_json):
    """Ids and links between rows don't change, and an unknown column is an error, not a server crash."""
    with app.app_context():
        reset_database(database.get_session(), run_processing_json)

    def edit(table, column, new):
        payload = {"modification": [{"table": table, "id": [1], "column": column, "new": new}]}
        return client.post('/project/TestProject/modification/edit', json=payload).get_json()

    assert "it is an id or a link to another row" in edit("specimen", "id", 99)["DB_ACTION_ERROR"]
    assert "it is an id or a link to another row" in edit("sample", "specimen_id", 2)["DB_ACTION_ERROR"]
    assert "has no column 'nickname'" in edit("specimen", "nickname", "x")["DB_ACTION_ERROR"]
    assert "'project' rows can't be changed" in edit("project", "name", "OtherProject")["DB_ACTION_ERROR"]
    with app.app_context():
        session = database.get_session()
        assert session.query(Specimen).filter_by(id=1).one().id == 1
        assert session.query(Sample).filter_by(id=1).one().specimen_id == 1
    curate = client.post('/project/TestProject/modification/curate', json={"modification": [{"table": "project", "id": [1]}]})
    assert "'project' rows can't be changed" in curate.get_json()["DB_ACTION_ERROR"]


def test_failed_save_is_an_error(client, app, run_processing_json):
    """A change the database refuses (here a duplicate unique name) is reported as an error, and nothing changes."""
    with app.app_context():
        reset_database(database.get_session(), run_processing_json)
        session = database.get_session()
        first, second = session.query(Specimen).order_by(Specimen.id).limit(2).all()
        first_name, second_id = first.name, second.id

    payload = {"modification": [{"table": "specimen", "id": [second_id], "column": "name", "new": first_name}]}
    result = client.post('/project/TestProject/modification/edit', json=payload).get_json()

    assert "Nothing was changed" in result["DB_ACTION_ERROR"]
    assert "DB_ACTION_OUTPUT" not in result
    with app.app_context():
        assert database.get_session().query(Specimen).filter_by(id=second_id).one().name != first_name
