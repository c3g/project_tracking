from flask import g
from project_tracking import model, database
def test_create(client, app, ingestion_json):



    with app.app_context():
        assert client.post('project/big_project/ingest_run_processing', data=ingestion_json).status_code == 404
        p = model.Project(name='big_project')
        db = database.get_session()
        db.add(p)
        db.commit()
        print('toto')
        assert client.get('project/big_project/ingest_run_processing').status_code == 200


        assert client.post('project/big_project/ingest_run_processing', data=ingestion_json).status_code == 404


    # check here that project, readset et all is created properly
    # with app.app_context():
    #     db = get_db()
    #     count = db.execute('SELECT COUNT(id) FROM post').fetchone()[0]
    #     assert count == 2


if __name__ == '__main__':

    import os,csv,json
    data = []
    with open(os.path.join(os.path.dirname(__file__), 'data/event.csv'), 'r') as fp:
        csvReader = csv.DictReader(fp)

        for row in csvReader:
            # Assuming a column named 'No' to
            # be the primary key
            data.append(row)

    data = json.dumps(data)

    import project_tracking
    app = project_tracking.create_app()

    test_create(app.test_client(),app, data)
