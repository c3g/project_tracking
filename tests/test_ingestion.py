import json
import re
import os
import logging

from sqlalchemy import select

from flask import g
from project_tracking import model, database, db_actions
from project_tracking import vocabulary as vb
from project_tracking import create_app

logger = logging.getLogger(__name__)

def test_create_api(client, run_processing_json, transfer_json, genpipes_json, delivery_json):
    # First create the project using the admin API
    project_name = run_processing_json[vb.PROJECT_NAME]
    response = client.post(f'admin/create_project/{project_name}')
    assert response.status_code == 200
    assert json.loads(response.data)["DB_ACTION_OUTPUT"][0]['name'] == f"{project_name}"
    assert json.loads(response.data)["DB_ACTION_OUTPUT"][0]['id'] == 1
    # Test ingesting run processing
    response = client.post(f'project/{project_name}/ingest_run_processing', data=json.dumps(run_processing_json))
    assert response.status_code == 200
    assert json.loads(response.data)["DB_ACTION_OUTPUT"][0]['name'] == "run_processing"
    assert json.loads(response.data)["DB_ACTION_OUTPUT"][0]['id'] == 1
    assert json.loads(response.data)["DB_ACTION_OUTPUT"][0]['platform'] == "abacus"
    assert json.loads(response.data)["DB_ACTION_OUTPUT"][0]['project_id'] == 1
    assert json.loads(response.data)["DB_ACTION_OUTPUT"][0]['status'] == "COMPLETED"
    # Test ingesting transfer
    response = client.post(f'project/{project_name}/ingest_transfer', data=json.dumps(transfer_json))
    assert response.status_code == 200
    assert json.loads(response.data)["DB_ACTION_OUTPUT"][0]['cmd_line'] == "globus transfer --submission-id $sub_id --label MoHQ-JG-9-23 --batch /lb/project/mugqic/projects/MOH/TEMP/2023-01-13T14.21.42_transfer_log.txt 6c66d53d-a79d-11e8-96fa-0a6d4e044368 278b9bfe-24da-11e9-9fa2-0a06afd4a22e"
    assert json.loads(response.data)["DB_ACTION_OUTPUT"][0]['id'] == 2
    assert json.loads(response.data)["DB_ACTION_OUTPUT"][0]['name'] == "transfer"
    assert json.loads(response.data)["DB_ACTION_OUTPUT"][0]['platform'] == "beluga"
    assert json.loads(response.data)["DB_ACTION_OUTPUT"][0]['project_id'] == 1
    assert json.loads(response.data)["DB_ACTION_OUTPUT"][0]['status'] == "COMPLETED"
    # Test ingesting genpipes
    response = client.post(f'project/{project_name}/ingest_genpipes', data=json.dumps(genpipes_json))
    assert response.status_code == 200
    assert json.loads(response.data)["DB_ACTION_OUTPUT"][0]['cmd_line'] == "module purge\nmodule load python/3.10.2 mugqic/genpipes/4.2.0\nrnaseq_light.py \n    -j slurm \n    -r readset.txt \n    -s 1-5 \n    -c $MUGQIC_PIPELINES_HOME/pipelines/rnaseq_light/rnaseq_light.base.ini \n        $MUGQIC_PIPELINES_HOME/pipelines/common_ini/beluga.ini \n        $MUGQIC_PIPELINES_HOME/resources/genomes/config/Homo_sapiens.GRCh38.ini \n        RNA_light.custom.ini \n  > RNASeq_light_run.sh\nrm -r RNA_CHUNKS;\nmkdir RNA_CHUNKS;\n$MUGQIC_PIPELINES_HOME/utils/chunk_genpipes.sh -n 100 RNASeq_light_run.sh RNA_CHUNKS"
    assert json.loads(response.data)["DB_ACTION_OUTPUT"][0]['id'] == 3
    assert json.loads(response.data)["DB_ACTION_OUTPUT"][0]['name'] == "genpipes"
    assert json.loads(response.data)["DB_ACTION_OUTPUT"][0]['operation_config_id'] == 1
    assert json.loads(response.data)["DB_ACTION_OUTPUT"][0]['platform'] == "beluga"
    assert json.loads(response.data)["DB_ACTION_OUTPUT"][0]['project_id'] == 1
    assert json.loads(response.data)["DB_ACTION_OUTPUT"][0]['status'] == "COMPLETED"
    # Test ingesting genpipes a 2nd time to make sure it fetches the already existing entrities and links the new information to them
    response = client.post(f'project/{project_name}/ingest_genpipes', data=json.dumps(genpipes_json))
    assert json.loads(response.data)["DB_ACTION_WARNING"] == ['OperationConfig with id 1 and name genpipes_ini already exists, informations will be attached to this one.', 'Operation with id 3 and name genpipes already exists, informations will be attached to this one.', 'Job with id 3 and name trimmomatic already exists, informations will be attached to this one.', 'Job with id 4 and name kallisto already exists, informations will be attached to this one.', 'Job with id 5 and name trimmomatic already exists, informations will be attached to this one.', 'Job with id 4 and name kallisto already exists, informations will be attached to this one.', 'Job with id 6 and name trimmomatic already exists, informations will be attached to this one.', 'Job with id 4 and name kallisto already exists, informations will be attached to this one.']
    # Test ingesting delivery
    response = client.post(f'project/{project_name}/ingest_delivery', data=json.dumps(delivery_json))
    assert response.status_code == 200
    assert json.loads(response.data)["DB_ACTION_OUTPUT"][0]['cmd_line'] == "bucket_delivery.py -i MoHQ-JG-9-23_DNA_2022-06-25T19.04.54.json -l Delivery_MoHQ-JG-9-23_DNA_2025-06-25T19.04.54.list"
    assert json.loads(response.data)["DB_ACTION_OUTPUT"][0]['id'] == 4
    assert json.loads(response.data)["DB_ACTION_OUTPUT"][0]['name'] == "delivery"
    assert json.loads(response.data)["DB_ACTION_OUTPUT"][0]['platform'] == "beluga"
    assert json.loads(response.data)["DB_ACTION_OUTPUT"][0]['project_id'] == 1
    assert json.loads(response.data)["DB_ACTION_OUTPUT"][0]['status'] == "COMPLETED"


def test_create(not_app_db, run_processing_json, transfer_json, genpipes_json):
    project_name = run_processing_json[vb.PROJECT_NAME]
    db_actions.create_project(project_name, session=not_app_db)
    project_id = db_actions.name_to_id("Project", project_name, session=not_app_db)

    run_processing_out = db_actions.ingest_run_processing(project_id, run_processing_json, not_app_db)

    assert isinstance(run_processing_out["DB_ACTION_OUTPUT"][0], model.Operation)
    assert not_app_db.scalars(select(model.Project)).first().name == project_name

    for specimen_json in run_processing_json[vb.SPECIMEN]:
        assert not_app_db.scalars(select(model.Specimen).where(model.Specimen.name == specimen_json[vb.SPECIMEN_NAME])).first().name == specimen_json[vb.SPECIMEN_NAME]
        for sample_json in specimen_json[vb.SAMPLE]:
            assert not_app_db.scalars(select(model.Sample).where(model.Sample.name == sample_json[vb.SAMPLE_NAME])).first().name == sample_json[vb.SAMPLE_NAME]
            for readset_json in sample_json[vb.READSET]:
                assert not_app_db.scalars(select(model.Readset).where(model.Readset.name == readset_json[vb.READSET_NAME])).first().name == readset_json[vb.READSET_NAME]

    transfer_out = db_actions.ingest_transfer(project_id, transfer_json, not_app_db)
    assert isinstance(transfer_out["DB_ACTION_OUTPUT"][0], model.Operation)

    genpipes_out = db_actions.ingest_genpipes(project_id, genpipes_json, not_app_db)
    assert isinstance(genpipes_out["DB_ACTION_OUTPUT"][0], model.Operation)
