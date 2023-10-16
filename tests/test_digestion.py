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
    project_name = run_processing_json[vb.PROJECT_NAME]
    project_id = "1"
    response = client.get(f'admin/create_project/{project_name}')
    response = client.post(f'project/{project_id}/ingest_run_processing', data=json.dumps(run_processing_json))
    response = client.post(f'project/{project_id}/digest_readset_file', data=json.dumps(readset_file_json))
    assert response.status_code == 200
    response = client.post(f'project/{project_id}/digest_pair_file', data=json.dumps(readset_file_json))
    assert response.status_code == 200

    with app.app_context():
        s = database.get_session()
