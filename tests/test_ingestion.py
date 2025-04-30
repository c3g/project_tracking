import json
import re
import os
import logging

from sqlalchemy import select

from flask import g
from project_tracking import model, database, db_action
from project_tracking import vocabulary as vb
from project_tracking import create_app

logger = logging.getLogger(__name__)

def test_create_api(client, run_processing_json, transfer_json, genpipes_json, app):
    project_name = run_processing_json[vb.PROJECT_NAME]
    response = client.get(f'admin/create_project/{project_name}')
    assert response.status_code == 200
    assert json.loads(response.data)['name'] == f"{project_name}"
    assert json.loads(response.data)['id'] == 1
    response = client.post(f'project/{project_name}/ingest_run_processing', data=json.dumps(run_processing_json))
    assert response.status_code == 200
    assert json.loads(response.data)["DB_ACTION_OUTPUT"][0]['name'] == "run_processing"
    assert json.loads(response.data)["DB_ACTION_OUTPUT"][0]['id'] == 1
    response = client.post(f'project/{project_name}/ingest_transfer', data=json.dumps(transfer_json))
    assert response.status_code == 200
    assert json.loads(response.data)["DB_ACTION_OUTPUT"][0]['name'] == "transfer"
    response = client.post(f'project/{project_name}/ingest_genpipes', data=json.dumps(genpipes_json))
    assert response.status_code == 200
    assert json.loads(response.data)["DB_ACTION_OUTPUT"][0]['name'] == "genpipes"
    with app.app_context():
        s = database.get_session()


def test_create(not_app_db, run_processing_json, transfer_json, genpipes_json):
    project_name = run_processing_json[vb.PROJECT_NAME]
    db_action.create_project(project_name, session=not_app_db)
    project_id = db_action.name_to_id("Project", project_name, session=not_app_db)

    run_processing_out = db_action.ingest_run_processing(project_id, run_processing_json, not_app_db)

    assert isinstance(run_processing_out["DB_ACTION_OUTPUT"][0], model.Operation)
    # assert isinstance(run_processing_job, model.Job)
    assert not_app_db.scalars(select(model.Project)).first().name == project_name

    for specimen_json in run_processing_json[vb.SPECIMEN]:
        assert not_app_db.scalars(select(model.Specimen).where(model.Specimen.name == specimen_json[vb.SPECIMEN_NAME])).first().name == specimen_json[vb.SPECIMEN_NAME]
        for sample_json in specimen_json[vb.SAMPLE]:
            assert not_app_db.scalars(select(model.Sample).where(model.Sample.name == sample_json[vb.SAMPLE_NAME])).first().name == sample_json[vb.SAMPLE_NAME]
            for readset_json in sample_json[vb.READSET]:
                assert not_app_db.scalars(select(model.Readset).where(model.Readset.name == readset_json[vb.READSET_NAME])).first().name == readset_json[vb.READSET_NAME]

    transfer_out = db_action.ingest_transfer(project_id, transfer_json, not_app_db)

    assert isinstance(transfer_out["DB_ACTION_OUTPUT"][0], model.Operation)
    # assert isinstance(transfer_job, model.Job)

    genpipes_out = db_action.ingest_genpipes(project_id, genpipes_json, not_app_db)

    assert isinstance(genpipes_out["DB_ACTION_OUTPUT"][0], model.Operation)
    # for job in genpipes_jobs:
    #     assert isinstance(job, model.Job)

    # assert 1 == 2
