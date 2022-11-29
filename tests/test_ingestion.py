import json
import re

from sqlalchemy import select

from flask import g
from project_tracking import model, database, db_action



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
    project_name = "MoH"
    project = model.Project(name=project_name)
    with not_app_db as db:
        db.add(project)
        db.commit()

    ret = db_action.ingest_run_processing(project_name, ingestion_json, not_app_db)

    assert isinstance(ret, model.Operation)
    assert not_app_db.query(model.Project).one().name == project_name

    ingest_data = json.loads(ingestion_json)
    for line in ingest_data:
        sample_name = line["Sample Name"]
        result = re.search(r"^((MoHQ-(JG|CM|GC|MU|MR|XX)-\w+)-\w+)-\w+-\w+(D|R)(T|N)", sample_name)
        patient_name = result.group(1)
        assert len(not_app_db.execute(select(model.Patient).where(model.Patient.name == patient_name)).all()) == 1
        assert len(not_app_db.execute(select(model.Sample).where(model.Sample.name == sample_name)).all()) == 1
        print(not_app_db.execute(select(type(model.Readset)).where(model.Readset.name == f"{sample_name}_{line['Library ID']}_{line['Lane']}")).all())
        assert len(not_app_db.execute(select(model.Readset).where(model.Readset.name == f"{sample_name}_{line['Library ID']}_{line['Lane']}")).all()) == 1
    # with not_app_db as db:
        # Query
        # db.add(project)
