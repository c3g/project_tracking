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

def test_create_api(client, run_processing_json, app):
    response = client.get('admin/create_project/MOH-Q')
    assert response.status_code == 200
    assert json.loads(response.data)['name'] == 'MOH-Q'
    assert json.loads(response.data)['id'] == 1
    response = client.post('project/MOH-Q/ingest_run_processing', data=json.dumps(run_processing_json))
    assert response.status_code == 200
    assert json.loads(response.data)[0]['name'] == "run_processing"
    assert json.loads(response.data)[0]['id'] == 1
    with app.app_context():
        s = database.get_session()


def test_create(not_app_db, run_processing_json, transfer_json, genpipes_json):
    project_name = run_processing_json[vb.PROJECT_NAME]
    db_action.create_project(project_name, session=not_app_db)

    [run_processing_operation, run_processing_job] = db_action.ingest_run_processing(project_name, run_processing_json, not_app_db)

    assert isinstance(run_processing_operation, model.Operation)
    assert isinstance(run_processing_job, model.Job)
    assert not_app_db.scalars(select(model.Project)).first().name == project_name

    for patient_json in run_processing_json[vb.PATIENT]:
        assert not_app_db.scalars(select(model.Patient).where(model.Patient.name == patient_json[vb.PATIENT_NAME])).first().name == patient_json[vb.PATIENT_NAME]
        for sample_json in patient_json[vb.SAMPLE]:
            assert not_app_db.scalars(select(model.Sample).where(model.Sample.name == sample_json[vb.SAMPLE_NAME])).first().name == sample_json[vb.SAMPLE_NAME]
            for readset_json in sample_json[vb.READSET]:
                assert not_app_db.scalars(select(model.Readset).where(model.Readset.name == readset_json[vb.READSET_NAME])).first().name == readset_json[vb.READSET_NAME]

    [transfer_operation, transfer_job] = db_action.ingest_transfer(project_name, transfer_json, not_app_db)

    assert isinstance(transfer_operation, model.Operation)
    assert isinstance(transfer_job, model.Job)

    [genpipes_operation, genpipes_jobs] = db_action.ingest_genpipes(project_name, genpipes_json, not_app_db)

    # assert 1 == 2
