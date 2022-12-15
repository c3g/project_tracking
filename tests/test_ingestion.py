import json
import re
import os

from sqlalchemy import select

from flask import g
from project_tracking import model, database, db_action
from project_tracking import vocabulary as vb
from project_tracking import create_app


def test_create_api(client, ingestion_json, app):
    response = client.get('admin/create_project/MoH')
    assert response.status_code == 200
    assert json.loads(response.data)['name'] == 'MoH'
    assert json.loads(response.data)['id'] == 1
    response = client.post('project/MoH/ingest_run_processing', data=json.dumps(ingestion_json))
    assert response.status_code == 200
    assert json.loads(response.data)['name'] == "ingestion"
    assert json.loads(response.data)['id'] == 1
    with app.app_context():
        s = database.get_session()


def test_create(not_app_db, ingestion_json):
    project_name = ingestion_json[vb.PROJECT_NAME]
    db_action.create_project(project_name, session=not_app_db)

    ret = db_action.ingest_run_processing(project_name, ingestion_json, not_app_db)

    assert isinstance(ret, model.Operation)
    assert not_app_db.scalars(select(model.Project)).first().name == project_name

    for patient_json in ingestion_json[vb.PATIENT]:
        assert not_app_db.scalars(select(model.Patient).where(model.Patient.name == patient_json[vb.PATIENT_NAME])).first().name == patient_json[vb.PATIENT_NAME]
        for sample_json in patient_json[vb.SAMPLE]:
            assert not_app_db.scalars(select(model.Sample).where(model.Sample.name == sample_json[vb.SAMPLE_NAME])).first().name == sample_json[vb.SAMPLE_NAME]
            for readset_json in sample_json[vb.READSET]:
                assert not_app_db.scalars(select(model.Readset).where(model.Readset.name == readset_json[vb.READSET_NAME])).first().name == readset_json[vb.READSET_NAME]

    db_action.digest_readset(ingestion_json[vb.RUN_NAME], os.path.join(os.path.dirname(__file__), "data/readset_file.tsv"), session=not_app_db)
    db_action.digest_pair(ingestion_json[vb.RUN_NAME], os.path.join(os.path.dirname(__file__), "data/pair_file.csv"), session=not_app_db)

    # assert 1 == 2



