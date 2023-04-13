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

def test_digest_api(client, ingestion_json, digest_readset_file_json,  app):
    response = client.post('project/moh-q/nouveau_nom',data=digest_readset_file_json)
    assert response.status_code == 200

