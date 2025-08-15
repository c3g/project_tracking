""" PyTest for the digestion API endpoints in the project_tracking module. """
import logging
import json

from project_tracking import database
from project_tracking import vocabulary as vb

logger = logging.getLogger(__name__)

READSET_FILE = {"DB_ACTION_OUTPUT": [
    {
        'Sample': 'MoHQ-JG-9-23-15000863775-19933DT',
        'Readset': 'MoHQ-JG-9-23-15000863775-19933DT.A01433_0157_1',
        'LibraryType': None,
        'RunType': 'PAIRED_END',
        'Run': '220420_A01433_0157_BHM3NHDSX2_MoHRun08-novaseq',
        'Lane': '1',
        'Adapter1': 'AGATCGGAAGAGCACACGTCTGAACTCCAGTCAC',
        'Adapter2': 'AGATCGGAAGAGCGTCGTGTAGGGAAAGAGTGT',
        'QualityOffset': '33',
        'BED': None,
        'FASTQ1': None,
        'FASTQ2': None,
        'BAM': '/lb/robot/research/processing/novaseq/2022/220420_A01433_0157_BHM3NHDSX2_MoHRun08-novaseq/Aligned.1/alignment/MoHQ-JG-9-23-15000863775-19933DT/runA01433_0157_1/MoHQ-JG-9-23-15000863775-19933DT_2-2224210.sorted.bam'
    },
    {
        'Sample': 'MoHQ-CM-1-3-15000936286-19866DN',
        'Readset': 'MoHQ-CM-1-3-15000936286-19866DN.A01433_0157_2',
        'LibraryType': None,
        'RunType': 'PAIRED_END',
        'Run': '220420_A01433_0157_BHM3NHDSX2_MoHRun08-novaseq',
        'Lane': '2',
        'Adapter1': 'AGATCGGAAGAGCACACGTCTGAACTCCAGTCAC',
        'Adapter2': 'AGATCGGAAGAGCGTCGTGTAGGGAAAGAGTGT',
        'QualityOffset': '33',
        'BED': None,
        'FASTQ1': None,
        'FASTQ2': None,
        'BAM': '/lb/robot/research/processing/novaseq/2022/220420_A01433_0157_BHM3NHDSX2_MoHRun08-novaseq/Aligned.2/alignment/MoHQ-CM-1-3-15000936286-19866DN/runA01433_0157_2/MoHQ-CM-1-3-15000936286-19866DN_2-2224220.sorted.bam'
    },
    {
        'Sample': 'MoHQ-CM-1-3-15000863775-19933DT',
        'Readset': 'MoHQ-CM-1-3-15000863775-19933DT.A01433_0157_1',
        'LibraryType': None,
        'RunType': 'PAIRED_END',
        'Run': '220420_A01433_0157_BHM3NHDSX2_MoHRun08-novaseq',
        'Lane': '1',
        'Adapter1': 'AGATCGGAAGAGCACACGTCTGAACTCCAGTCAC',
        'Adapter2': 'AGATCGGAAGAGCGTCGTGTAGGGAAAGAGTGT',
        'QualityOffset': '33',
        'BED': None,
        'FASTQ1': None,
        'FASTQ2': None,
        'BAM': '/lb/robot/research/processing/novaseq/2022/220420_A01433_0157_BHM3NHDSX2_MoHRun08-novaseq/Aligned.1/alignment/MoHQ-CM-1-3-15000863775-19933DT/runA01433_0157_1/MoHQ-CM-1-3-15000863775-19933DT_2-2224210.sorted.bam'
    },
    {
        'Sample': 'MoHQ-JG-9-23-15000936286-19866DN',
        'Readset': 'MoHQ-JG-9-23-15000936286-19866DN.A01433_0157_2',
        'LibraryType': None,
        'RunType': 'PAIRED_END',
        'Run': '220420_A01433_0157_BHM3NHDSX2_MoHRun08-novaseq',
        'Lane': '2',
        'Adapter1': 'AGATCGGAAGAGCACACGTCTGAACTCCAGTCAC',
        'Adapter2': 'AGATCGGAAGAGCGTCGTGTAGGGAAAGAGTGT',
        'QualityOffset': '33',
        'BED': None,
        'FASTQ1': None,
        'FASTQ2': None,
        'BAM': '/lb/robot/research/processing/novaseq/2022/220420_A01433_0157_BHM3NHDSX2_MoHRun08-novaseq/Aligned.2/alignment/MoHQ-JG-9-23-15000936286-19866DN/runA01433_0157_2/MoHQ-JG-9-23-15000936286-19866DN_2-2224220.sorted.bam'
    }
]}

PAIR_FILE = {"DB_ACTION_OUTPUT": [
    {
        "Specimen": "MoHQ-JG-9-23",
        "Sample_N": "MoHQ-JG-9-23-15000936286-19866DN",
        "Sample_T": "MoHQ-JG-9-23-15000863775-19933DT"
    },
    {   "Specimen": "MoHQ-CM-1-3",
        "Sample_N": "MoHQ-CM-1-3-15000936286-19866DN",
        "Sample_T": "MoHQ-CM-1-3-15000863775-19933DT"
    }
]}

UNALYZED = {"DB_ACTION_OUTPUT": [
    {
        "location_endpoint": "abacus",
        "experiment_nucleic_acid_type": "DNA",
        "sample_name": [
            "MoHQ-JG-9-23-15000863775-19933DT",
            "MoHQ-JG-9-23-15000936286-19866DN",
            "MoHQ-CM-1-3-15000863775-19933DT",
            "MoHQ-CM-1-3-15000936286-19866DN"
        ]
    }
]}

DELIVERY = {"DB_ACTION_OUTPUT": [
    {
        "experiment_nucleic_acid_type": "DNA",
        "location_endpoint": "abacus",
        "operation":[],
        "specimen": [
            {
                "cohort":"MoHQ-JG-9",
                "institution":"JG",
                "name":"MoHQ-JG-9-23",
                "sample": [
                    {
                        "name":"MoHQ-JG-9-23-15000863775-19933DT",
                        "readset": [
                            {
                                "file": [
                                    {
                                    "location":"/lb/robot/research/processing/novaseq/2022/220420_A01433_0157_BHM3NHDSX2_MoHRun08-novaseq/Aligned.1/alignment/MoHQ-JG-9-23-15000863775-19933DT/runA01433_0157_1/MoHQ-JG-9-23-15000863775-19933DT_2-2224210.sorted.bam",
                                    "name":"MoHQ-JG-9-23-15000863775-19933DT_2-2224210.sorted.bam"
                                    }
                                ],
                                "metric": [
                                    {
                                    "aggregate":None,
                                    "flag":"PASS",
                                    "name":"raw_reads_count",
                                    "value":"822429243"
                                    },
                                    {
                                    "aggregate":None,
                                    "flag":"PASS",
                                    "name":"raw_duplication_rate",
                                    "value":"9.0"
                                    }
                                ],
                                "name":"MoHQ-JG-9-23-15000863775-19933DT.A01433_0157_1"
                            }],
                        "tumour":True
                    },
                    {
                        "name":"MoHQ-JG-9-23-15000936286-19866DN",
                        "readset": [
                            {
                                "file":[
                                    {
                                    "location":"/lb/robot/research/processing/novaseq/2022/220420_A01433_0157_BHM3NHDSX2_MoHRun08-novaseq/Aligned.2/alignment/MoHQ-JG-9-23-15000936286-19866DN/runA01433_0157_2/MoHQ-JG-9-23-15000936286-19866DN_2-2224220.sorted.bam",
                                    "name":"MoHQ-JG-9-23-15000936286-19866DN_2-2224220.sorted.bam"
                                    }
                                ],
                                "metric":[
                                    {
                                    "aggregate":None,
                                    "flag":"PASS",
                                    "name":"raw_reads_count",
                                    "value":"940464718"
                                    },
                                    {
                                    "aggregate":None,
                                    "flag":"WARNING",
                                    "name":"raw_duplication_rate",
                                    "value":"9.8"
                                    }
                                ],
                                "name":"MoHQ-JG-9-23-15000936286-19866DN.A01433_0157_2"
                            }],
                        "tumour":False
                    }
                ]
            }
        ]
    }
]}

def test_digest_api(client, run_processing_json, readset_file_json, app):
    """Test the /digest_readset_file and /digest_pair_file routes."""
    project_name = run_processing_json[vb.PROJECT_NAME]
    project_id = "1"
    response = client.post(f'admin/create_project/{project_name}')
    response = client.post(f'project/{project_id}/ingest_run_processing', data=json.dumps(run_processing_json))
    # Digest readset_file
    response = client.get(f'project/{project_id}/digest_readset_file?json={json.dumps(readset_file_json)}')
    assert response.status_code == 200
    assert sorted(json.loads(response.text)["DB_ACTION_OUTPUT"], key=lambda x: x["Sample"]) == sorted(READSET_FILE["DB_ACTION_OUTPUT"], key=lambda x: x["Sample"])
    # Digest pair_file
    response = client.get(f'project/{project_id}/digest_pair_file?json={json.dumps(readset_file_json)}')
    assert response.status_code == 200
    assert sorted(json.loads(response.text)["DB_ACTION_OUTPUT"], key=lambda x: x["Specimen"]) == sorted(PAIR_FILE["DB_ACTION_OUTPUT"], key=lambda x: x["Specimen"])
    # Digest unanalyzed
    unanalyzed_json = {
        'sample_name': True,
        'sample_id': False,
        'readset_name': False,
        'readset_id': False,
        'run_name': False,
        'run_id': False,
        'experiment_nucleic_acid_type': 'DNA',
        'location_endpoint': 'abacus'
    }
    response = client.get(f'project/{project_id}/digest_unanalyzed?json={json.dumps(unanalyzed_json)}')
    assert response.status_code == 200
    assert json.loads(response.text)["DB_ACTION_OUTPUT"] == UNALYZED["DB_ACTION_OUTPUT"]

    # Digest delivery
    delivery_json = {
        'location_endpoint': 'abacus',
        'experiment_nucleic_acid_type': 'DNA',
        'readset_id': [1, 2]
    }
    response = client.get(f'project/{project_id}/digest_delivery?json={json.dumps(delivery_json)}')
    assert response.status_code == 200
    assert sorted(json.loads(response.text)["DB_ACTION_OUTPUT"], key=lambda x: x["specimen"]) == sorted(DELIVERY["DB_ACTION_OUTPUT"], key=lambda x: x["specimen"])

    with app.app_context():
        s = database.get_session()
