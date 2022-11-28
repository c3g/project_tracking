def test_create(client, app, ingestion_json):

    assert client.get('/ingest_run_processing').status_code == 200

    client.post('/ingest_run_processing', data=ingestion_json)

    # check here that project, readset et all is created properly
    # with app.app_context():
    #     db = get_db()
    #     count = db.execute('SELECT COUNT(id) FROM post').fetchone()[0]
    #     assert count == 2
