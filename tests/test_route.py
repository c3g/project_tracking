"""Test cases for the project tracking routes."""
from project_tracking import database, db_actions

def reset_database(session, run_processing_json):
    """
    Reset the database to a clean state and ingest a run processing JSON.
    """
    db_actions.create_project("TestProject", session=session)
    project_id = db_actions.name_to_id("Project", "TestProject", session=session)[0]
    db_actions.ingest_run_processing(project_id, run_processing_json, session=session)
    return project_id
def extract_expected_entities(run_processing_json):
    expected = {
        "specimens": [],
        "samples": [],
        "readsets": [],
        "files": [],
        "metrics": []
    }
    for specimen in run_processing_json.get("specimen", []):
        expected["specimens"].append({
            "specimen_name": specimen.get("specimen_name"),
            "specimen_cohort": specimen.get("specimen_cohort"),
            "specimen_institution": specimen.get("specimen_institution")
        })
        for sample in specimen.get("sample", []):
            expected["samples"].append({
                "sample_name": sample.get("sample_name"),
                "sample_tumour": sample.get("sample_tumour")
            })
            for readset in sample.get("readset", []):
                expected["readsets"].append({
                    "readset_name": readset.get("readset_name"),
                    "readset_lane": readset.get("readset_lane"),
                    "readset_sequencing_type": readset.get("readset_sequencing_type")
                })
                for file in readset.get("file", []):
                    expected["files"].append({
                        "file_name": file.get("file_name"),
                        "location_uri": file.get("location_uri")
                    })
                for metric in readset.get("metric", []):
                    expected["metrics"].append({
                        "metric_name": metric.get("metric_name"),
                        "metric_value": metric.get("metric_value"),
                        "metric_flag": metric.get("metric_flag")
                    })
    return expected

def test_projects_route(client, app, run_processing_json):
    with app.app_context():
        session = database.get_session()
        project_id = reset_database(session, run_processing_json)
        response = client.get(f"/project/{project_id}")
        assert response.status_code == 200
        data = response.json
        assert "DB_ACTION_OUTPUT" in data
        assert any(p["name"] == "TestProject" for p in data["DB_ACTION_OUTPUT"])
        assert any(p["id"] == project_id for p in data["DB_ACTION_OUTPUT"])
        assert any(p["tablename"] == "project" for p in data["DB_ACTION_OUTPUT"])

def test_specimens_route(client, app, run_processing_json):
    specimen_key_map = {
        "specimen_ext_id": "ext_id",
        "specimen_ext_src": "ext_src",
        "specimen_name": "name",
        "specimen_cohort": "cohort",
        "specimen_institution": "institution"
    }

    with app.app_context():
        session = database.get_session()
        project_id = reset_database(session, run_processing_json)
        response = client.get(f"/project/{project_id}/specimens")
        assert response.status_code == 200
        data = response.json
        assert "DB_ACTION_OUTPUT" in data
        returned_items = data["DB_ACTION_OUTPUT"]

        expected_items = []
        for specimen in run_processing_json.get("specimen", []):
            expected_items.append({
                "specimen_name": specimen.get("specimen_name"),
                "specimen_cohort": specimen.get("specimen_cohort"),
                "specimen_institution": specimen.get("specimen_institution")
            })

        assert len(returned_items) == len(expected_items)

        for returned, expected in zip(returned_items, expected_items):
            assert returned["tablename"] == "specimen"
            for input_key, api_key in specimen_key_map.items():
                if expected.get(input_key) is not None:
                    assert api_key in returned, f"Missing key: {api_key}"
                    assert returned[api_key] == expected[input_key], (
                        f"Mismatch for key '{api_key}': expected '{expected[input_key]}', got '{returned[api_key]}'"
                    )

def test_samples_route(client, app, run_processing_json):
    sample_key_map = {
        "sample_ext_id": "ext_id",
        "sample_ext_src": "ext_src",
        "sample_name": "name",
        "sample_tumour": "tumour",
        "sample_alias": "alias"
    }

    with app.app_context():
        session = database.get_session()
        project_id = reset_database(session, run_processing_json)
        response = client.get(f"/project/{project_id}/samples")
        assert response.status_code == 200
        data = response.json
        assert "DB_ACTION_OUTPUT" in data
        returned_items = data["DB_ACTION_OUTPUT"]

        expected_items = []
        for specimen in run_processing_json.get("specimen", []):
            for sample in specimen.get("sample", []):
                expected_items.append({
                    "sample_name": sample.get("sample_name"),
                    "sample_tumour": sample.get("sample_tumour")
                })

        assert len(returned_items) == len(expected_items)

        for returned, expected in zip(returned_items, expected_items):
            assert returned["tablename"] == "sample"
            for input_key, api_key in sample_key_map.items():
                if expected.get(input_key) is not None:
                    assert api_key in returned
                    assert returned[api_key] == expected[input_key]

def test_readsets_route(client, app, run_processing_json):
    readset_key_map = {
        "readset_name": "name",
        "readset_lane": "lane",
        "readset_adapter1": "adapter1",
        "readset_adapter2": "adapter2",
        "readset_sequencing_type": "sequencing_type",
        "readset_quality_offset": "quality_offset"
    }

    with app.app_context():
        session = database.get_session()
        project_id = reset_database(session, run_processing_json)
        response = client.get(f"/project/{project_id}/readsets")
        assert response.status_code == 200
        data = response.json
        assert "DB_ACTION_OUTPUT" in data
        returned_items = data["DB_ACTION_OUTPUT"]

        expected_items = []
        for specimen in run_processing_json.get("specimen", []):
            for sample in specimen.get("sample", []):
                for readset in sample.get("readset", []):
                    expected_items.append({
                        "readset_name": readset.get("readset_name"),
                        "readset_lane": readset.get("readset_lane"),
                        "readset_sequencing_type": readset.get("readset_sequencing_type")
                    })

        assert len(returned_items) == len(expected_items)

        for returned, expected in zip(returned_items, expected_items):
            assert returned["tablename"] == "readset"
            for input_key, api_key in readset_key_map.items():
                if expected.get(input_key) is not None:
                    assert api_key in returned
                    assert returned[api_key] == expected[input_key]

def test_metrics_route(client, app, run_processing_json):
    metric_key_map = {
        "metric_name": "name",
        "metric_value": "value",
        "metric_flag": "flag",
        "metric_deliverable": "deliverable"
    }

    with app.app_context():
        session = database.get_session()
        project_id = reset_database(session, run_processing_json)
        response = client.get(f"/project/{project_id}/metrics")
        assert response.status_code == 200
        data = response.json
        assert "DB_ACTION_OUTPUT" in data
        returned_items = data["DB_ACTION_OUTPUT"]

        expected_items = []
        for specimen in run_processing_json.get("specimen", []):
            for sample in specimen.get("sample", []):
                for readset in sample.get("readset", []):
                    for metric in readset.get("metric", []):
                        expected_items.append({
                            "metric_name": metric.get("metric_name"),
                            "metric_value": metric.get("metric_value"),
                            "metric_flag": metric.get("metric_flag")
                        })

        assert len(returned_items) == len(expected_items)

        for returned, expected in zip(returned_items, expected_items):
            assert returned["tablename"] == "metric"
            for input_key, api_key in metric_key_map.items():
                if expected.get(input_key) is not None:
                    assert api_key in returned
                    assert returned[api_key] == expected[input_key]

def test_files_route(client, app, run_processing_json):
    file_key_map = {
        "file_name": "name",
        "file_extra_metadata": "extra_metadata",
        "file_deliverable": "deliverable",
    }

    with app.app_context():
        session = database.get_session()
        project_id = reset_database(session, run_processing_json)
        response = client.get(f"/project/{project_id}/files")
        assert response.status_code == 200
        data = response.json
        assert "DB_ACTION_OUTPUT" in data
        returned_items = data["DB_ACTION_OUTPUT"]
        expected_items = []
        for specimen in run_processing_json.get("specimen", []):
            for sample in specimen.get("sample", []):
                for readset in sample.get("readset", []):
                    for file in readset.get("file", []):
                        expected_items.append({
                            "file_name": file.get("file_name"),
                            "location_uri": file.get("location_uri")
                        })

        assert len(returned_items) == len(expected_items)

        for returned, expected in zip(returned_items, expected_items):
            assert returned["tablename"] == "file"
            for input_key, api_key in file_key_map.items():
                if expected.get(input_key) is not None:
                    assert api_key in returned
                    assert returned[api_key] == expected[input_key]

def test_operations_route(client, app, run_processing_json):
    operation_key_map = {
        "operation_platform": "platform",
        "operation_name": "name"
    }

    with app.app_context():
        session = database.get_session()
        project_id = reset_database(session, run_processing_json)
        response = client.get(f"/project/{project_id}/operations")
        assert response.status_code == 200
        data = response.json
        assert "DB_ACTION_OUTPUT" in data
        returned_items = data["DB_ACTION_OUTPUT"]

        expected_items = [{
            "operation_platform": run_processing_json.get("operation_platform"),
            "operation_name": "run_processing"
        }]

        assert len(returned_items) == len(expected_items)

        for returned, expected in zip(returned_items, expected_items):
            assert returned["tablename"] == "operation"
            for input_key, api_key in operation_key_map.items():
                if expected.get(input_key) is not None:
                    assert api_key in returned
                    assert returned[api_key] == expected[input_key]

def test_jobs_route(client, app, run_processing_json):
    with app.app_context():
        session = database.get_session()
        project_id = reset_database(session, run_processing_json)
        response = client.get(f"/project/{project_id}/jobs")
        assert response.status_code == 200
        data = response.json
        assert "DB_ACTION_OUTPUT" in data
        returned_items = data["DB_ACTION_OUTPUT"]

        # Construct expected job from input assumptions
        expected_items = [{
            "name": "run_processing",
            "status": "COMPLETED"
        }]

        assert len(returned_items) == len(expected_items)

        for returned, expected in zip(returned_items, expected_items):
            assert returned["tablename"] == "job"
            for key in expected:
                assert key in returned, f"Missing key: {key}"
                assert returned[key] == expected[key], (
                    f"Mismatch for key '{key}': expected '{expected[key]}', got '{returned[key]}'"
                )
            assert "start" in returned
            assert "stop" in returned
            assert isinstance(returned.get("readsets", []), list)
            assert len(returned["readsets"]) > 0
