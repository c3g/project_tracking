import copy
import json
import re
import os
import logging

import pytest
from sqlalchemy import select

from flask import g
from project_tracking import model, database, db_actions
from project_tracking import vocabulary as vb
from project_tracking import create_app
from project_tracking.db_actions.errors import RequestError

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
    # Test ingesting an analysis
    response = client.post(f'project/{project_name}/ingest_analysis', data=json.dumps(genpipes_json))
    assert response.status_code == 200
    assert json.loads(response.data)["DB_ACTION_OUTPUT"][0]['cmd_line'] == "module purge\nmodule load python/3.10.2 mugqic/genpipes/4.2.0\nrnaseq_light.py \n    -j slurm \n    -r readset.txt \n    -s 1-5 \n    -c $MUGQIC_PIPELINES_HOME/pipelines/rnaseq_light/rnaseq_light.base.ini \n        $MUGQIC_PIPELINES_HOME/pipelines/common_ini/beluga.ini \n        $MUGQIC_PIPELINES_HOME/resources/genomes/config/Homo_sapiens.GRCh38.ini \n        RNA_light.custom.ini \n  > RNASeq_light_run.sh\nrm -r RNA_CHUNKS;\nmkdir RNA_CHUNKS;\n$MUGQIC_PIPELINES_HOME/utils/chunk_genpipes.sh -n 100 RNASeq_light_run.sh RNA_CHUNKS"
    assert json.loads(response.data)["DB_ACTION_OUTPUT"][0]['id'] == 3
    assert json.loads(response.data)["DB_ACTION_OUTPUT"][0]['name'] == "genpipes"
    assert json.loads(response.data)["DB_ACTION_OUTPUT"][0]['operation_config_id'] == 1
    assert json.loads(response.data)["DB_ACTION_OUTPUT"][0]['platform'] == "beluga"
    assert json.loads(response.data)["DB_ACTION_OUTPUT"][0]['project_id'] == 1
    assert json.loads(response.data)["DB_ACTION_OUTPUT"][0]['status'] == "COMPLETED"
    # Test ingesting an analysis a 2nd time to make sure it fetches the already existing entrities and links the new information to them
    response = client.post(f'project/{project_name}/ingest_analysis', data=json.dumps(genpipes_json))
    assert json.loads(response.data)["DB_ACTION_WARNING"] == ["OperationConfig with id 1 and name genpipes_ini already exists, informations will be attached to this one.","Operation with id 3 and name genpipes already exists, informations will be attached to this one.","Job with id 3 and name trimmomatic already exists, informations will be attached to this one.","Warning: Location with id 21 and uri beluga:///lustre03/project/6007512/C3G/projects/MOH_PROCESSING/MAIN/trim/MoHQ-CM-1-3-6929-1RT/MoHQ-CM-1-3-6929-1RT.A01433_0157_3.trim.pair1.fastq.gz already exists, informations will be attached to this one.","Warning: Location with id 22 and uri beluga:///lustre03/project/6007512/C3G/projects/MOH_PROCESSING/MAIN/trim/MoHQ-CM-1-3-6929-1RT/MoHQ-CM-1-3-6929-1RT.A01433_0157_3.trim.single1.fastq.gz already exists, informations will be attached to this one.","Warning: Location with id 23 and uri beluga:///lustre03/project/6007512/C3G/projects/MOH_PROCESSING/MAIN/trim/MoHQ-CM-1-3-6929-1RT/MoHQ-CM-1-3-6929-1RT.A01433_0157_3.trim.pair2.fastq.gz already exists, informations will be attached to this one.","Warning: Location with id 24 and uri beluga:///lustre03/project/6007512/C3G/projects/MOH_PROCESSING/MAIN/trim/MoHQ-CM-1-3-6929-1RT/MoHQ-CM-1-3-6929-1RT.A01433_0157_3.trim.single2.fastq.gz already exists, informations will be attached to this one.","Warning: Location with id 25 and uri beluga:///lustre03/project/6007512/C3G/projects/MOH_PROCESSING/MAIN/trim/MoHQ-CM-1-3-6929-1RT/MoHQ-CM-1-3-6929-1RT.A01433_0157_3.trim.log already exists, informations will be attached to this one.","Job with id 4 and name kallisto already exists, informations will be attached to this one.","Warning: Location with id 26 and uri beluga:///lustre03/project/6007512/C3G/projects/MOH_PROCESSING/MAIN/kallisto/MoHQ-CM-1-3-6929-1RT/abundance_transcripts.tsv already exists, informations will be attached to this one.","Warning: Location with id 27 and uri beluga:///lustre03/project/6007512/C3G/projects/MOH_PROCESSING/MAIN/kallisto/MoHQ-CM-1-3-6929-1RT/abundance_genes.tsv already exists, informations will be attached to this one.","Warning: Location with id 28 and uri beluga:///lustre03/project/6007512/C3G/projects/MOH_PROCESSING/MAIN/kallisto/MoHQ-CM-1-3-6929-1RT/kallisto_quant.log already exists, informations will be attached to this one.","Job with id 5 and name trimmomatic already exists, informations will be attached to this one.","Warning: Location with id 29 and uri beluga:///lustre03/project/6007512/C3G/projects/MOH_PROCESSING/MAIN/trim/MoHQ-CM-1-3-6929-1RT/MoHQ-CM-1-3-6929-1RT.A01433_0157_2.trim.pair1.fastq.gz already exists, informations will be attached to this one.","Warning: Location with id 30 and uri beluga:///lustre03/project/6007512/C3G/projects/MOH_PROCESSING/MAIN/trim/MoHQ-CM-1-3-6929-1RT/MoHQ-CM-1-3-6929-1RT.A01433_0157_2.trim.single1.fastq.gz already exists, informations will be attached to this one.","Warning: Location with id 31 and uri beluga:///lustre03/project/6007512/C3G/projects/MOH_PROCESSING/MAIN/trim/MoHQ-CM-1-3-6929-1RT/MoHQ-CM-1-3-6929-1RT.A01433_0157_2.trim.pair2.fastq.gz already exists, informations will be attached to this one.","Warning: Location with id 32 and uri beluga:///lustre03/project/6007512/C3G/projects/MOH_PROCESSING/MAIN/trim/MoHQ-CM-1-3-6929-1RT/MoHQ-CM-1-3-6929-1RT.A01433_0157_2.trim.single2.fastq.gz already exists, informations will be attached to this one.","Warning: Location with id 33 and uri beluga:///lustre03/project/6007512/C3G/projects/MOH_PROCESSING/MAIN/trim/MoHQ-CM-1-3-6929-1RT/MoHQ-CM-1-3-6929-1RT.A01433_0157_2.trim.log already exists, informations will be attached to this one.","Job with id 4 and name kallisto already exists, informations will be attached to this one.","Warning: Location with id 26 and uri beluga:///lustre03/project/6007512/C3G/projects/MOH_PROCESSING/MAIN/kallisto/MoHQ-CM-1-3-6929-1RT/abundance_transcripts.tsv already exists, informations will be attached to this one.","Warning: Location with id 27 and uri beluga:///lustre03/project/6007512/C3G/projects/MOH_PROCESSING/MAIN/kallisto/MoHQ-CM-1-3-6929-1RT/abundance_genes.tsv already exists, informations will be attached to this one.","Warning: Location with id 28 and uri beluga:///lustre03/project/6007512/C3G/projects/MOH_PROCESSING/MAIN/kallisto/MoHQ-CM-1-3-6929-1RT/kallisto_quant.log already exists, informations will be attached to this one.","Job with id 6 and name trimmomatic already exists, informations will be attached to this one.","Warning: Location with id 34 and uri beluga:///lustre03/project/6007512/C3G/projects/MOH_PROCESSING/MAIN/trim/MoHQ-CM-1-3-6929-1RT/MoHQ-CM-1-3-6929-1RT.A01433_0157_1.trim.pair1.fastq.gz already exists, informations will be attached to this one.","Warning: Location with id 35 and uri beluga:///lustre03/project/6007512/C3G/projects/MOH_PROCESSING/MAIN/trim/MoHQ-CM-1-3-6929-1RT/MoHQ-CM-1-3-6929-1RT.A01433_0157_1.trim.single1.fastq.gz already exists, informations will be attached to this one.","Warning: Location with id 36 and uri beluga:///lustre03/project/6007512/C3G/projects/MOH_PROCESSING/MAIN/trim/MoHQ-CM-1-3-6929-1RT/MoHQ-CM-1-3-6929-1RT.A01433_0157_1.trim.pair2.fastq.gz already exists, informations will be attached to this one.","Warning: Location with id 37 and uri beluga:///lustre03/project/6007512/C3G/projects/MOH_PROCESSING/MAIN/trim/MoHQ-CM-1-3-6929-1RT/MoHQ-CM-1-3-6929-1RT.A01433_0157_1.trim.single2.fastq.gz already exists, informations will be attached to this one.","Warning: Location with id 38 and uri beluga:///lustre03/project/6007512/C3G/projects/MOH_PROCESSING/MAIN/trim/MoHQ-CM-1-3-6929-1RT/MoHQ-CM-1-3-6929-1RT.A01433_0157_1.trim.log already exists, informations will be attached to this one.","Job with id 4 and name kallisto already exists, informations will be attached to this one.","Warning: Location with id 26 and uri beluga:///lustre03/project/6007512/C3G/projects/MOH_PROCESSING/MAIN/kallisto/MoHQ-CM-1-3-6929-1RT/abundance_transcripts.tsv already exists, informations will be attached to this one.","Warning: Location with id 27 and uri beluga:///lustre03/project/6007512/C3G/projects/MOH_PROCESSING/MAIN/kallisto/MoHQ-CM-1-3-6929-1RT/abundance_genes.tsv already exists, informations will be attached to this one.","Warning: Location with id 28 and uri beluga:///lustre03/project/6007512/C3G/projects/MOH_PROCESSING/MAIN/kallisto/MoHQ-CM-1-3-6929-1RT/kallisto_quant.log already exists, informations will be attached to this one."]
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
    project_id = db_actions.name_to_id("Project", project_name, session=not_app_db)[0]

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

    genpipes_out = db_actions.ingest_analysis(project_id, genpipes_json, not_app_db)
    assert isinstance(genpipes_out["DB_ACTION_OUTPUT"][0], model.Operation)


def test_create_project_accepts_optional_ext_fields(client):
    response = client.post(
        'admin/create_project/EXT-PROJECT',
        data=json.dumps({"ext_id": 123, "ext_src": "lims"}),
        content_type='application/json'
    )
    assert response.status_code == 200
    data = json.loads(response.data)
    assert data["DB_ACTION_OUTPUT"][0]["name"] == "EXT-PROJECT"
    assert data["DB_ACTION_OUTPUT"][0]["ext_id"] == 123
    assert data["DB_ACTION_OUTPUT"][0]["ext_src"] == "lims"


def test_create_project_rejects_non_integer_ext_id(client):
    response = client.post(
        'admin/create_project/BAD-EXT',
        data=json.dumps({"ext_id": "abc", "ext_src": "lims"}),
        content_type='application/json'
    )
    assert response.status_code == 400
    data = json.loads(response.data)
    assert "DB_ACTION_ERROR" in data
    assert "ext_id must be an integer when provided." in data["DB_ACTION_ERROR"]


def test_run_processing_skips_file_without_md5sum(not_app_db, run_processing_json):
    project_name = run_processing_json[vb.PROJECT_NAME]
    db_actions.create_project(project_name, session=not_app_db)
    project_id = db_actions.name_to_id("Project", project_name, session=not_app_db)[0]

    data = copy.deepcopy(run_processing_json)
    first_file = data[vb.SPECIMEN][0][vb.SAMPLE][0][vb.READSET][0][vb.FILE][0]
    first_file_name = first_file[vb.FILE_NAME]
    first_file[vb.FILE_MD5SUM] = None

    result = db_actions.ingest_run_processing(project_id, data, not_app_db)

    assert any("has no md5sum, skipping" in w for w in result["DB_ACTION_WARNING"])
    assert not_app_db.execute(select(model.File).where(model.File.name == first_file_name)).scalar_one_or_none() is None


def test_run_processing_force_ingest_file_without_md5sum(not_app_db, run_processing_json):
    project_name = run_processing_json[vb.PROJECT_NAME]
    db_actions.create_project(project_name, session=not_app_db)
    project_id = db_actions.name_to_id("Project", project_name, session=not_app_db)[0]

    data = copy.deepcopy(run_processing_json)
    first_file = data[vb.SPECIMEN][0][vb.SAMPLE][0][vb.READSET][0][vb.FILE][0]
    first_file_name = first_file[vb.FILE_NAME]
    first_file[vb.FILE_MD5SUM] = None
    data[vb.FORCE] = True

    result = db_actions.ingest_run_processing(project_id, data, not_app_db)

    assert any("ingesting anyway (force=True)" in w for w in result["DB_ACTION_WARNING"])
    assert not_app_db.execute(select(model.File).where(model.File.name == first_file_name)).scalar_one_or_none() is not None


def test_genpipes_skips_file_without_md5sum(not_app_db, run_processing_json, genpipes_json):
    project_name = run_processing_json[vb.PROJECT_NAME]
    db_actions.create_project(project_name, session=not_app_db)
    project_id = db_actions.name_to_id("Project", project_name, session=not_app_db)[0]
    db_actions.ingest_run_processing(project_id, run_processing_json, not_app_db)

    data = copy.deepcopy(genpipes_json)
    first_file = data[vb.SAMPLE][0][vb.READSET][0][vb.JOB][0][vb.FILE][0]
    first_file_name = first_file[vb.FILE_NAME]
    first_file[vb.FILE_MD5SUM] = None

    result = db_actions.ingest_analysis(project_id, data, not_app_db)

    assert any("has no md5sum, skipping" in w for w in result["DB_ACTION_WARNING"])
    assert not_app_db.execute(select(model.File).where(model.File.name == first_file_name)).scalar_one_or_none() is None


def test_genpipes_force_ingest_file_without_md5sum(not_app_db, run_processing_json, genpipes_json):
    project_name = run_processing_json[vb.PROJECT_NAME]
    db_actions.create_project(project_name, session=not_app_db)
    project_id = db_actions.name_to_id("Project", project_name, session=not_app_db)[0]
    db_actions.ingest_run_processing(project_id, run_processing_json, not_app_db)

    data = copy.deepcopy(genpipes_json)
    first_file = data[vb.SAMPLE][0][vb.READSET][0][vb.JOB][0][vb.FILE][0]
    first_file_name = first_file[vb.FILE_NAME]
    first_file[vb.FILE_MD5SUM] = None
    data[vb.FORCE] = True

    result = db_actions.ingest_analysis(project_id, data, not_app_db)

    assert any("ingesting anyway (force=True)" in w for w in result["DB_ACTION_WARNING"])
    assert not_app_db.execute(select(model.File).where(model.File.name == first_file_name)).scalar_one_or_none() is not None


TUMOUR_SAMPLE = "MoHQ-JG-9-23-15000863775-19933DT"


def _set_oncotree_code(data, code):
    for specimen_json in data[vb.SPECIMEN]:
        for sample_json in specimen_json[vb.SAMPLE]:
            if sample_json[vb.SAMPLE_NAME] == TUMOUR_SAMPLE:
                sample_json[vb.SAMPLE_ONCOTREE_CODE] = code


def _new_run(data, run_id):
    """Same samples, new run: readset names, files and uris all carry the run id."""
    return json.loads(json.dumps(data).replace("A01433_0157", run_id))


def _oncotree_code(session):
    return session.execute(select(model.Sample.oncotree_code).where(model.Sample.name == TUMOUR_SAMPLE)).scalar_one()


def test_run_processing_oncotree_code(not_app_db, run_processing_json):
    """Code is stored (normalized), a conflicting code is kept with a warning, and force replaces it."""
    project_name = run_processing_json[vb.PROJECT_NAME]
    db_actions.create_project(project_name, session=not_app_db)
    project_id = db_actions.name_to_id("Project", project_name, session=not_app_db)[0]

    data = copy.deepcopy(run_processing_json)
    _set_oncotree_code(data, " luad ")
    db_actions.ingest_run_processing(project_id, data, not_app_db)
    assert _oncotree_code(not_app_db) == "LUAD"

    conflicting_run = _new_run(data, "A01433_0998")
    _set_oncotree_code(conflicting_run, "LUSC")
    result = db_actions.ingest_run_processing(project_id, conflicting_run, not_app_db)
    assert any("already has 'oncotree_code' 'LUAD'" in w for w in result["DB_ACTION_WARNING"])
    assert _oncotree_code(not_app_db) == "LUAD"

    forced_run = _new_run(data, "A01433_0999")
    _set_oncotree_code(forced_run, "LUSC")
    forced_run[vb.FORCE] = True
    result = db_actions.ingest_run_processing(project_id, forced_run, not_app_db)
    assert any("updated from 'LUAD' to 'LUSC'" in w for w in result["DB_ACTION_WARNING"])
    assert _oncotree_code(not_app_db) == "LUSC"


def test_run_processing_rejects_malformed_oncotree_code(not_app_db, run_processing_json):
    project_name = run_processing_json[vb.PROJECT_NAME]
    db_actions.create_project(project_name, session=not_app_db)
    project_id = db_actions.name_to_id("Project", project_name, session=not_app_db)[0]

    data = copy.deepcopy(run_processing_json)
    _set_oncotree_code(data, "LU-AD")
    with pytest.raises(RequestError) as error:
        db_actions.ingest_run_processing(project_id, data, not_app_db)
    assert "LU-AD" in error.value.message[0]
    assert not_app_db.execute(select(model.Sample)).first() is None



def _set_cancer_type(data, cancer_type):
    for specimen_json in data[vb.SPECIMEN]:
        for sample_json in specimen_json[vb.SAMPLE]:
            if sample_json[vb.SAMPLE_NAME] == TUMOUR_SAMPLE:
                sample_json[vb.SAMPLE_CANCER_TYPE] = cancer_type


def _cancer_type(session):
    return session.execute(select(model.Sample.cancer_type).where(model.Sample.name == TUMOUR_SAMPLE)).scalar_one()


def test_run_processing_cancer_type(not_app_db, run_processing_json):
    """Type is stored with underscores for spaces and its case, a conflicting type is kept with a warning, and force replaces it."""
    project_name = run_processing_json[vb.PROJECT_NAME]
    db_actions.create_project(project_name, session=not_app_db)
    project_id = db_actions.name_to_id("Project", project_name, session=not_app_db)[0]

    data = copy.deepcopy(run_processing_json)
    _set_oncotree_code(data, "LUAD")
    _set_cancer_type(data, " Lung  Adenocarcinoma ")
    db_actions.ingest_run_processing(project_id, data, not_app_db)
    assert (_oncotree_code(not_app_db), _cancer_type(not_app_db)) == ("LUAD", "Lung_Adenocarcinoma")

    conflicting_run = _new_run(data, "A01433_0998")
    _set_cancer_type(conflicting_run, "Lung Squamous Cell Carcinoma")
    result = db_actions.ingest_run_processing(project_id, conflicting_run, not_app_db)
    assert any("already has 'cancer_type' 'Lung_Adenocarcinoma', keeping it instead of 'Lung_Squamous_Cell_Carcinoma'" in w for w in result["DB_ACTION_WARNING"])
    assert _cancer_type(not_app_db) == "Lung_Adenocarcinoma"

    forced_run = _new_run(data, "A01433_0999")
    _set_cancer_type(forced_run, "Lung Squamous Cell Carcinoma")
    forced_run[vb.FORCE] = True
    result = db_actions.ingest_run_processing(project_id, forced_run, not_app_db)
    assert any("updated from 'Lung_Adenocarcinoma' to 'Lung_Squamous_Cell_Carcinoma'" in w for w in result["DB_ACTION_WARNING"])
    assert _cancer_type(not_app_db) == "Lung_Squamous_Cell_Carcinoma"


def test_run_processing_rejects_non_string_cancer_type(not_app_db, run_processing_json):
    project_name = run_processing_json[vb.PROJECT_NAME]
    db_actions.create_project(project_name, session=not_app_db)
    project_id = db_actions.name_to_id("Project", project_name, session=not_app_db)[0]

    data = copy.deepcopy(run_processing_json)
    _set_cancer_type(data, ["Lung", "Adenocarcinoma"])
    with pytest.raises(RequestError) as error:
        db_actions.ingest_run_processing(project_id, data, not_app_db)
    assert "invalid 'cancer_type' ['Lung', 'Adenocarcinoma']" in error.value.message[0]
    assert not_app_db.execute(select(model.Sample)).first() is None


def _readset_state(session, readset_name):
    return session.execute(select(model.Readset.state).where(model.Readset.name == readset_name)).scalar_one()


def _readsets(data):
    return [readset for specimen in data[vb.SPECIMEN] for sample in specimen[vb.SAMPLE] for readset in sample[vb.READSET]]


def test_run_processing_readset_state(not_app_db, run_processing_json):
    """Exact states are stored as sent; a missing or null state gives VALID, a null metric flag no flag."""
    project_name = run_processing_json[vb.PROJECT_NAME]
    db_actions.create_project(project_name, session=not_app_db)
    project_id = db_actions.name_to_id("Project", project_name, session=not_app_db)[0]

    data = copy.deepcopy(run_processing_json)
    readsets = _readsets(data)
    readsets[0][vb.READSET_STATE] = "ON_HOLD"
    readsets[1][vb.READSET_STATE] = "INVALID"
    readsets[2][vb.READSET_STATE] = None
    readsets[3].pop(vb.READSET_STATE, None)
    readsets[0][vb.METRIC][0][vb.METRIC_FLAG] = None
    db_actions.ingest_run_processing(project_id, data, not_app_db)

    expected = [model.StateEnum.ON_HOLD, model.StateEnum.INVALID, model.StateEnum.VALID, model.StateEnum.VALID]
    assert [_readset_state(not_app_db, readset[vb.READSET_NAME]) for readset in readsets[:4]] == expected


def test_run_processing_rejects_invalid_enum_values(client, run_processing_json):
    """All invalid enum values are listed in one clear error, not a 500, and nothing is written."""
    project_name = run_processing_json[vb.PROJECT_NAME]
    client.post(f'admin/create_project/{project_name}')

    data = copy.deepcopy(run_processing_json)
    readsets = _readsets(data)
    readsets[0][vb.READSET_STATE] = "on_hold"
    readsets[1][vb.READSET_LANE] = 4
    readsets[2][vb.READSET_SEQUENCING_TYPE] = "PAIRED"
    readsets[3][vb.METRIC][0][vb.METRIC_FLAG] = "OK"
    response = client.post(f'project/{project_name}/ingest_run_processing', data=json.dumps(data))
    errors = json.loads(response.data)["DB_ACTION_ERROR"]

    assert len(errors) == 4
    assert f"'{readsets[0][vb.READSET_NAME]}' has an invalid 'readset_state' 'on_hold'" in errors[0]
    assert "'ON_HOLD'" in errors[0]
    assert "invalid 'readset_lane' 4: expected one of '1', '2'" in errors[1]
    assert "invalid 'readset_sequencing_type' 'PAIRED'" in errors[2]
    assert "invalid 'metric_flag' 'OK'" in errors[3] and readsets[3][vb.READSET_NAME] in errors[3]

    response = client.get(f'project/{project_name}/readsets')
    assert not json.loads(response.data).get("DB_ACTION_OUTPUT")


def test_genpipes_rejects_invalid_enum_values(not_app_db, run_processing_json, genpipes_json):
    project_name = run_processing_json[vb.PROJECT_NAME]
    db_actions.create_project(project_name, session=not_app_db)
    project_id = db_actions.name_to_id("Project", project_name, session=not_app_db)[0]
    db_actions.ingest_run_processing(project_id, run_processing_json, not_app_db)

    data = copy.deepcopy(genpipes_json)
    job = data[vb.SAMPLE][0][vb.READSET][0][vb.JOB][0]
    job[vb.JOB_STATUS] = "DONE"
    with pytest.raises(RequestError) as error:
        db_actions.ingest_analysis(project_id, data, not_app_db)
    assert len(error.value.message) == 1
    assert f"'{job[vb.JOB_NAME]}'" in error.value.message[0] and "invalid 'job_status' 'DONE'" in error.value.message[0]
    assert not_app_db.execute(select(model.Job).where(model.Job.name == job[vb.JOB_NAME])).first() is None


def test_analysis_operation_status(not_app_db, run_processing_json, genpipes_json):
    """operation_status when given, otherwise COMPLETED only when every job is; re-ingesting updates the same operation."""
    project_name = run_processing_json[vb.PROJECT_NAME]
    db_actions.create_project(project_name, session=not_app_db)
    project_id = db_actions.name_to_id("Project", project_name, session=not_app_db)[0]
    db_actions.ingest_run_processing(project_id, run_processing_json, not_app_db)

    def operation():
        operations = not_app_db.execute(
            select(model.Operation).where(model.Operation.name == genpipes_json[vb.OPERATION_NAME])
        ).scalars().all()
        assert len(operations) == 1
        return operations[0]

    data = copy.deepcopy(genpipes_json)
    job = data[vb.SAMPLE][0][vb.READSET][0][vb.JOB][0]
    job[vb.JOB_STATUS] = "FAILED"
    db_actions.ingest_analysis(project_id, data, not_app_db)
    assert operation().status == model.StatusEnum.RUNNING

    job[vb.JOB_STATUS] = None
    db_actions.ingest_analysis(project_id, data, not_app_db)
    assert operation().status == model.StatusEnum.RUNNING

    out = db_actions.ingest_analysis(project_id, genpipes_json, not_app_db)
    assert operation().status == model.StatusEnum.COMPLETED
    assert any("Its status changes from RUNNING to COMPLETED." in w for w in out["DB_ACTION_WARNING"])

    data[vb.OPERATION_STATUS] = "FAILED"
    db_actions.ingest_analysis(project_id, data, not_app_db)
    assert operation().status == model.StatusEnum.FAILED


def test_analysis_rejects_invalid_operation_status(not_app_db, run_processing_json, genpipes_json):
    project_name = run_processing_json[vb.PROJECT_NAME]
    db_actions.create_project(project_name, session=not_app_db)
    project_id = db_actions.name_to_id("Project", project_name, session=not_app_db)[0]
    db_actions.ingest_run_processing(project_id, run_processing_json, not_app_db)

    data = copy.deepcopy(genpipes_json)
    data[vb.OPERATION_STATUS] = "DONE"
    with pytest.raises(RequestError) as error:
        db_actions.ingest_analysis(project_id, data, not_app_db)
    assert any("invalid 'operation_status' 'DONE'" in message for message in error.value.message)


def test_edit_enum_column(client, run_processing_json):
    """edit takes an exact enum value or member name; anything else is a clear error and nothing changes."""
    project_name = run_processing_json[vb.PROJECT_NAME]
    client.post(f'admin/create_project/{project_name}')
    client.post(f'project/{project_name}/ingest_run_processing', data=json.dumps(run_processing_json))

    def edit(column, new):
        payload = {"modification": [{"table": "readset", "column": column, "id": [1], "new": new}]}
        return json.loads(client.post(f'project/{project_name}/modification/edit', data=json.dumps(payload)).data)

    def readset():
        return json.loads(client.get(f'project/{project_name}/readsets/1').data)["DB_ACTION_OUTPUT"][0]

    assert "DB_ACTION_ERROR" not in edit("state", "ON_HOLD")
    assert readset()["state"] == "ON_HOLD"
    assert "already has 'state'" in edit("state", "ON_HOLD")["DB_ACTION_WARNING"][0]

    body = edit("state", "on_hold")
    assert "'on_hold' is not a valid value for 'StateEnum'" in body["DB_ACTION_ERROR"]
    assert readset()["state"] == "ON_HOLD"

    edit("lane", "4")
    assert readset()["lane"] == "4"
    edit("lane", "FIVE")
    assert readset()["lane"] == "5"


def test_files_rejects_invalid_state_filter(client, run_processing_json):
    project_name = run_processing_json[vb.PROJECT_NAME]
    client.post(f'admin/create_project/{project_name}')
    client.post(f'project/{project_name}/ingest_run_processing', data=json.dumps(run_processing_json))
    response = client.get(f'project/{project_name}/files?json={json.dumps({"state": "valid"})}')
    assert "'valid' is not a valid value for 'StateEnum'" in json.loads(response.data)["DB_ACTION_ERROR"]


def _analysis_project(session, run_processing_json):
    project_name = run_processing_json[vb.PROJECT_NAME]
    db_actions.create_project(project_name, session=session)
    project_id = db_actions.name_to_id("Project", project_name, session=session)[0]
    db_actions.ingest_run_processing(project_id, run_processing_json, session)
    return project_id


def _analyses(session):
    return session.execute(select(model.Operation).where(model.Operation.analysis.is_(True))).scalars().all()


def test_analysis_operation_name_pipeline_protocol(not_app_db, run_processing_json, genpipes_json):
    """The operation keeps the payload name, pipeline and protocol, and is flagged as an analysis."""
    project_id = _analysis_project(not_app_db, run_processing_json)
    db_actions.ingest_analysis(project_id, genpipes_json, not_app_db)

    [operation] = _analyses(not_app_db)
    assert (operation.name, operation.pipeline, operation.protocol) == ("genpipes", "RnaSeqLight", "default")
    run_processing = not_app_db.execute(select(model.Operation).where(model.Operation.name == "run_processing")).scalar_one()
    assert run_processing.analysis is False


def test_analysis_rejects_operation_outside_vocabulary(not_app_db, run_processing_json, genpipes_json):
    """Operation name, pipeline and protocol must come from the vocabulary; problems are reported with enum ones."""
    project_id = _analysis_project(not_app_db, run_processing_json)
    cases = [
        ({vb.OPERATION_NAME: "GenPipes_RnaSeq.cancer"}, "'operation_name' 'GenPipes_RnaSeq.cancer' is not an allowed analysis operation: expected one of 'genpipes'"),
        ({vb.OPERATION_NAME: "run_processing"}, "'operation_name' 'run_processing' is reserved"),
        ({vb.OPERATION_PIPELINE: "RnaSeqlight"}, "'operation_pipeline' 'RnaSeqlight' is not allowed for 'genpipes': expected one of 'AmpliconSeq'"),
        ({vb.OPERATION_PIPELINE: None}, "'operation_pipeline' None is not allowed for 'genpipes'"),
        ({vb.OPERATION_PROTOCOL: "cancer"}, "'operation_protocol' 'cancer' is not allowed for 'genpipes' 'RnaSeqLight': expected one of 'default'"),
    ]
    for change, expected in cases:
        data = copy.deepcopy(genpipes_json)
        data.update(change)
        data[vb.SAMPLE][0][vb.READSET][0][vb.JOB][0][vb.JOB_STATUS] = "DONE"
        with pytest.raises(RequestError) as error:
            db_actions.ingest_analysis(project_id, data, not_app_db)
        assert expected in error.value.message[0], (change, error.value.message)
        assert "invalid 'job_status' 'DONE'" in error.value.message[1]
    assert _analyses(not_app_db) == []


def test_analysis_reingest_fills_pipeline_and_protocol(not_app_db, run_processing_json, genpipes_json):
    """Re-ingesting reuses the operation: empty pipeline/protocol are filled, a different value needs force."""
    project_id = _analysis_project(not_app_db, run_processing_json)
    db_actions.ingest_analysis(project_id, genpipes_json, not_app_db)
    [operation] = _analyses(not_app_db)
    # As an analysis ingested before pipeline and protocol existed
    operation.pipeline = operation.protocol = None
    not_app_db.flush()

    db_actions.ingest_analysis(project_id, genpipes_json, not_app_db)
    assert _analyses(not_app_db) == [operation]
    assert (operation.pipeline, operation.protocol) == ("RnaSeqLight", "default")

    changed = copy.deepcopy(genpipes_json)
    changed[vb.OPERATION_PIPELINE], changed[vb.OPERATION_PROTOCOL] = "RnaSeq", "cancer"
    result = db_actions.ingest_analysis(project_id, changed, not_app_db)
    assert any("already has 'pipeline' 'RnaSeqLight', keeping it instead of 'RnaSeq'" in w for w in result["DB_ACTION_WARNING"])
    assert (operation.pipeline, operation.protocol) == ("RnaSeqLight", "default")

    changed[vb.FORCE] = True
    result = db_actions.ingest_analysis(project_id, changed, not_app_db)
    assert any("'pipeline' updated from 'RnaSeqLight' to 'RnaSeq' (force=True)" in w for w in result["DB_ACTION_WARNING"])
    assert _analyses(not_app_db) == [operation]
    assert (operation.pipeline, operation.protocol) == ("RnaSeq", "cancer")


def test_analysis_vocabulary_from_config(app, client, run_processing_json, genpipes_json):
    """ANALYSIS_OPERATIONS from the config replaces the default vocabulary; a tool without pipelines takes none."""
    app.config["ANALYSIS_OPERATIONS"] = {"custom_tool": {}}
    project_name = run_processing_json[vb.PROJECT_NAME]
    client.post(f'admin/create_project/{project_name}')
    client.post(f'project/{project_name}/ingest_run_processing', data=json.dumps(run_processing_json))

    data = copy.deepcopy(genpipes_json)
    response = json.loads(client.post(f'project/{project_name}/ingest_analysis', data=json.dumps(data)).data)
    assert "'operation_name' 'genpipes' is not an allowed analysis operation: expected one of 'custom_tool'." in response["DB_ACTION_ERROR"]

    data[vb.OPERATION_NAME] = "custom_tool"
    response = json.loads(client.post(f'project/{project_name}/ingest_analysis', data=json.dumps(data)).data)
    assert "'operation_name' 'custom_tool' takes no 'operation_pipeline', got 'RnaSeqLight'." in response["DB_ACTION_ERROR"]

    del data[vb.OPERATION_PIPELINE], data[vb.OPERATION_PROTOCOL]
    response = json.loads(client.post(f'project/{project_name}/ingest_analysis', data=json.dumps(data)).data)
    assert "DB_ACTION_ERROR" not in response
    operation = response["DB_ACTION_OUTPUT"][0]
    assert (operation["name"], operation["analysis"], operation["pipeline"]) == ("custom_tool", True, None)


def test_invalid_analysis_vocabulary_fails_at_startup(monkeypatch):
    monkeypatch.setenv("C3G_ANALYSIS_OPERATIONS", json.dumps({"genpipes": {"Rna Seq": ["default"]}, "delivery": {}}))
    with pytest.raises(ValueError) as error:
        create_app({"TESTING": True})
    assert "invalid pipeline name 'Rna Seq'" in str(error.value)
    assert "operation name 'delivery' is reserved" in str(error.value)


def test_analysis_oncoanalyser_mode_as_pipeline(not_app_db, run_processing_json, genpipes_json):
    """oncoanalyser takes its mode (wgts or targeted) as pipeline, and no protocol."""
    project_id = _analysis_project(not_app_db, run_processing_json)
    data = copy.deepcopy(genpipes_json)
    data[vb.OPERATION_NAME], data[vb.OPERATION_PIPELINE] = "oncoanalyser", "wgts"

    with_protocol = copy.deepcopy(data)
    with pytest.raises(RequestError) as error:
        db_actions.ingest_analysis(project_id, with_protocol, not_app_db)
    assert "'operation_pipeline' 'wgts' takes no 'operation_protocol', got 'default'." in error.value.message

    without_mode = copy.deepcopy(data)
    del without_mode[vb.OPERATION_PIPELINE], without_mode[vb.OPERATION_PROTOCOL]
    with pytest.raises(RequestError) as error:
        db_actions.ingest_analysis(project_id, without_mode, not_app_db)
    assert "'operation_pipeline' None is not allowed for 'oncoanalyser': expected one of 'wgts', 'targeted'." in error.value.message

    del data[vb.OPERATION_PROTOCOL]
    db_actions.ingest_analysis(project_id, data, not_app_db)
    [operation] = _analyses(not_app_db)
    assert (operation.name, operation.pipeline, operation.protocol) == ("oncoanalyser", "wgts", None)


def test_analysis_dragen_somatic_needs_protocol(not_app_db, run_processing_json, genpipes_json):
    """dragen somatic takes tumor_only or tumor_normal as protocol, the other dragen pipelines take none."""
    project_id = _analysis_project(not_app_db, run_processing_json)
    data = copy.deepcopy(genpipes_json)
    data[vb.OPERATION_NAME], data[vb.OPERATION_PIPELINE] = "dragen", "somatic"

    without_protocol = copy.deepcopy(data)
    del without_protocol[vb.OPERATION_PROTOCOL]
    with pytest.raises(RequestError) as error:
        db_actions.ingest_analysis(project_id, without_protocol, not_app_db)
    assert "'operation_protocol' None is not allowed for 'dragen' 'somatic': expected one of 'tumor_only', 'tumor_normal'." in error.value.message

    germline_with_protocol = copy.deepcopy(data)
    germline_with_protocol[vb.OPERATION_PIPELINE], germline_with_protocol[vb.OPERATION_PROTOCOL] = "germline", "tumor_only"
    with pytest.raises(RequestError) as error:
        db_actions.ingest_analysis(project_id, germline_with_protocol, not_app_db)
    assert "'operation_pipeline' 'germline' takes no 'operation_protocol', got 'tumor_only'." in error.value.message

    data[vb.OPERATION_PROTOCOL] = "tumor_normal"
    db_actions.ingest_analysis(project_id, data, not_app_db)
    [operation] = _analyses(not_app_db)
    assert (operation.name, operation.pipeline, operation.protocol) == ("dragen", "somatic", "tumor_normal")


def _ingest_run(not_app_db, run_processing_json):
    project_name = run_processing_json[vb.PROJECT_NAME]
    db_actions.create_project(project_name, session=not_app_db)
    project_id = db_actions.name_to_id("Project", project_name, session=not_app_db)[0]
    db_actions.ingest_run_processing(project_id, run_processing_json, not_app_db)
    return project_id


def test_transfer_readset_state(not_app_db, run_processing_json, transfer_json):
    """An optional readset_state changes the readset's state, with a warning; readsets without it keep theirs."""
    project_id = _ingest_run(not_app_db, run_processing_json)
    data = copy.deepcopy(transfer_json)
    changed, kept = data[vb.READSET][0], data[vb.READSET][1]
    changed[vb.READSET_STATE] = "ON_HOLD"
    kept.pop(vb.READSET_STATE, None)

    result = db_actions.ingest_transfer(project_id, data, not_app_db)

    assert _readset_state(not_app_db, changed[vb.READSET_NAME]) == model.StateEnum.ON_HOLD
    assert _readset_state(not_app_db, kept[vb.READSET_NAME]) == model.StateEnum.VALID
    assert result["DB_ACTION_WARNING"] == [
        f"'Readset' '{changed[vb.READSET_NAME]}' state changed from 'VALID' to 'ON_HOLD'."
    ]
    # Sending the transfer again works: the same state changes nothing, another one changes it again
    again = db_actions.ingest_transfer(project_id, data, not_app_db)
    assert not [warning for warning in again["DB_ACTION_WARNING"] if "state changed" in warning]
    changed[vb.READSET_STATE] = "VALID"
    db_actions.ingest_transfer(project_id, data, not_app_db)
    assert _readset_state(not_app_db, changed[vb.READSET_NAME]) == model.StateEnum.VALID


def test_transfer_rejects_invalid_readset_state(not_app_db, run_processing_json, transfer_json):
    """Invalid states are all listed, and nothing is written."""
    project_id = _ingest_run(not_app_db, run_processing_json)
    data = copy.deepcopy(transfer_json)
    data[vb.READSET][0][vb.READSET_STATE] = "on_hold"
    data[vb.READSET][1][vb.READSET_STATE] = "DONE"

    with pytest.raises(RequestError) as error:
        db_actions.ingest_transfer(project_id, data, not_app_db)

    assert len(error.value.message) == 2
    assert f"'{data[vb.READSET][0][vb.READSET_NAME]}' has an invalid 'readset_state' 'on_hold'" in error.value.message[0]
    assert "invalid 'readset_state' 'DONE'" in error.value.message[1]
    assert not not_app_db.execute(select(model.Operation).where(model.Operation.name == "transfer")).first()
    assert _readset_state(not_app_db, data[vb.READSET][0][vb.READSET_NAME]) == model.StateEnum.VALID


def test_transfer_unknown_readset(client, run_processing_json, transfer_json):
    project_name = run_processing_json[vb.PROJECT_NAME]
    client.post(f'admin/create_project/{project_name}')
    client.post(f'project/{project_name}/ingest_run_processing', data=json.dumps(run_processing_json))
    data = copy.deepcopy(transfer_json)
    data[vb.READSET][0][vb.READSET_NAME] = "UNKNOWN-READSET"

    response = client.post(f'project/{project_name}/ingest_transfer', data=json.dumps(data))

    assert response.get_json()["DB_ACTION_ERROR"] == "No 'Readset' with 'name' 'UNKNOWN-READSET'"
