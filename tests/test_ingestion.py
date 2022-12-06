import json
import re
import pprint
import os

from sqlalchemy import select

from flask import g
from project_tracking import model, database, db_action
from project_tracking import vocabulary as vb

print = pprint.pprint

def test_create_api(client, app, ingestion_json):
    with app.app_context():
        assert client.post('project/big_project/ingest_run_processing', data=ingestion_json).status_code == 404
        p = model.Project(name='big_project')
        db = database.get_session()
        db.add(p)
        db.commit()
        # print('toto')
        assert client.get('project/big_project/ingest_run_processing').status_code == 200


        assert client.post('project/big_project/ingest_run_processing', data=ingestion_json).status_code == 200


    # check here that project, readset et all is created properly
    # with app.app_context():
    #     db = get_db()
    #     count = db.execute('SELECT COUNT(id) FROM post').fetchone()[0]
    #     assert count == 2


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
