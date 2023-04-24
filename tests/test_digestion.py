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

def test_digest_api(client, run_processing_json, readset_file_json, app):
    response = client.get('admin/create_project/MOH-Q')
    response = client.post('project/MOH-Q/ingest_run_processing', data=json.dumps(run_processing_json))
    response = client.post('project/MOH-Q/digest_readset_file', data=json.dumps(readset_file_json))
    assert response.status_code == 200
    response = client.post('project/MOH-Q/digest_pair_file', data=json.dumps(readset_file_json))
    assert response.status_code == 200

    with app.app_context():
        s = database.get_session()
