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
    response = client.post(f'admin/create_project/{project_name}')
    response = client.post(f'project/{project_name}/ingest_run_processing', data=json.dumps(run_processing_json))
    # Digest readset_file
    response = client.get(f'project/{project_name}/digest_readset_file?json={json.dumps(readset_file_json)}')
    assert response.status_code == 200
    assert sorted(json.loads(response.text)["DB_ACTION_OUTPUT"], key=lambda x: x["Sample"]) == sorted(READSET_FILE["DB_ACTION_OUTPUT"], key=lambda x: x["Sample"])
    # Digest pair_file
    response = client.get(f'project/{project_name}/digest_pair_file?json={json.dumps(readset_file_json)}')
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
    response = client.get(f'project/{project_name}/digest_unanalyzed?json={json.dumps(unanalyzed_json)}')
    assert response.status_code == 200
    assert json.loads(response.text)["DB_ACTION_OUTPUT"] == UNALYZED["DB_ACTION_OUTPUT"]

    # Digest delivery
    delivery_json = {
        'location_endpoint': 'abacus',
        'experiment_nucleic_acid_type': 'DNA',
        'readset_id': [1, 2]
    }
    response = client.get(f'project/{project_name}/digest_delivery?json={json.dumps(delivery_json)}')
    assert response.status_code == 200
    assert sorted(json.loads(response.text)["DB_ACTION_OUTPUT"], key=lambda x: x["specimen"]) == sorted(DELIVERY["DB_ACTION_OUTPUT"], key=lambda x: x["specimen"])

    with app.app_context():
        s = database.get_session()


def _clone_sample(sample_json, new_name):
    """Copy a run_processing sample under a new name (readsets, files and uris renamed too)."""
    return json.loads(json.dumps(sample_json).replace(sample_json[vb.SAMPLE_NAME], new_name))


def test_digest_pair_file_matched_samples(client, run_processing_json):
    """Pairs follow sample_matched_sample_name when declared, specimen otherwise."""
    project_name = run_processing_json[vb.PROJECT_NAME]
    cm_specimen = next(s for s in run_processing_json[vb.SPECIMEN] if s[vb.SPECIMEN_NAME] == "MoHQ-CM-1-3")
    samples = {s[vb.SAMPLE_NAME]: s for s in cm_specimen[vb.SAMPLE]}
    tumour = samples["MoHQ-CM-1-3-15000863775-19933DT"]
    normal = samples["MoHQ-CM-1-3-15000936286-19866DN"]
    normal_2 = _clone_sample(normal, "MoHQ-CM-1-3-15000936286-2DN")
    tumour_1 = _clone_sample(tumour, "MoHQ-CM-1-3-F10-1DT1")

    # Tumour declared before the normals in the payload
    tumour[vb.SAMPLE_MATCHED_SAMPLE_NAME] = [normal[vb.SAMPLE_NAME], normal_2[vb.SAMPLE_NAME]]
    tumour_1[vb.SAMPLE_MATCHED_SAMPLE_NAME] = [normal[vb.SAMPLE_NAME], "MoHQ-CM-1-3-NOT-INGESTED"]
    # Declared on the normal side only
    normal_2[vb.SAMPLE_MATCHED_SAMPLE_NAME] = [tumour_1[vb.SAMPLE_NAME]]
    cm_specimen[vb.SAMPLE].extend([normal_2, tumour_1])

    client.post(f'admin/create_project/{project_name}')
    response = client.post(f'project/{project_name}/ingest_run_processing', data=json.dumps(run_processing_json))
    assert response.status_code == 200
    assert any("MoHQ-CM-1-3-NOT-INGESTED" in warning for warning in json.loads(response.text)["DB_ACTION_WARNING"])

    digest_json = {
        vb.SPECIMEN_NAME: ["MoHQ-CM-1-3", "MoHQ-JG-9-23"],
        vb.EXPERIMENT_NUCLEIC_ACID_TYPE: "DNA"
    }
    response = client.get(f'project/{project_name}/digest_pair_file?json={json.dumps(digest_json)}')
    assert response.status_code == 200
    body = json.loads(response.text)

    def key(row):
        return (row["Specimen"], row["Sample_N"] or "", row["Sample_T"] or "")

    expected = [
        {"Specimen": "MoHQ-CM-1-3", "Sample_N": "MoHQ-CM-1-3-15000936286-19866DN", "Sample_T": "MoHQ-CM-1-3-15000863775-19933DT"},
        {"Specimen": "MoHQ-CM-1-3", "Sample_N": "MoHQ-CM-1-3-15000936286-2DN", "Sample_T": "MoHQ-CM-1-3-15000863775-19933DT"},
        {"Specimen": "MoHQ-CM-1-3", "Sample_N": "MoHQ-CM-1-3-15000936286-19866DN", "Sample_T": "MoHQ-CM-1-3-F10-1DT1"},
        {"Specimen": "MoHQ-CM-1-3", "Sample_N": "MoHQ-CM-1-3-15000936286-2DN", "Sample_T": "MoHQ-CM-1-3-F10-1DT1"},
        # No metadata: fallback on specimen
        {"Specimen": "MoHQ-JG-9-23", "Sample_N": "MoHQ-JG-9-23-15000936286-19866DN", "Sample_T": "MoHQ-JG-9-23-15000863775-19933DT"},
    ]
    assert sorted(body["DB_ACTION_OUTPUT"], key=key) == sorted(expected, key=key)


def test_sample_match_across_ingestions(client, run_processing_json):
    """A match declared before the other sample exists is created when that sample is ingested declaring it back."""
    project_name = run_processing_json[vb.PROJECT_NAME]
    jg_specimen = next(s for s in run_processing_json[vb.SPECIMEN] if s[vb.SPECIMEN_NAME] == "MoHQ-JG-9-23")
    tumour = next(s for s in jg_specimen[vb.SAMPLE] if s[vb.SAMPLE_TUMOUR])
    late_normal = _clone_sample(
        next(s for s in jg_specimen[vb.SAMPLE] if not s[vb.SAMPLE_TUMOUR]),
        "MoHQ-JG-9-23-B1-2DN"
    )
    tumour[vb.SAMPLE_MATCHED_SAMPLE_NAME] = [late_normal[vb.SAMPLE_NAME]]

    client.post(f'admin/create_project/{project_name}')
    response = client.post(f'project/{project_name}/ingest_run_processing', data=json.dumps(run_processing_json))
    assert any(late_normal[vb.SAMPLE_NAME] in warning for warning in json.loads(response.text)["DB_ACTION_WARNING"])

    late_normal[vb.SAMPLE_MATCHED_SAMPLE_NAME] = [tumour[vb.SAMPLE_NAME]]
    second_run = dict(run_processing_json, run_name="second_run", run_ext_id="2")
    second_run[vb.SPECIMEN] = [dict(jg_specimen, sample=[late_normal])]
    response = client.post(f'project/{project_name}/ingest_run_processing', data=json.dumps(second_run))
    assert response.status_code == 200

    digest_json = {vb.SAMPLE_NAME: [tumour[vb.SAMPLE_NAME], late_normal[vb.SAMPLE_NAME]], vb.EXPERIMENT_NUCLEIC_ACID_TYPE: "DNA"}
    response = client.get(f'project/{project_name}/digest_pair_file?json={json.dumps(digest_json)}')
    assert json.loads(response.text)["DB_ACTION_OUTPUT"] == [
        {"Specimen": "MoHQ-JG-9-23", "Sample_N": late_normal[vb.SAMPLE_NAME], "Sample_T": tumour[vb.SAMPLE_NAME]}
    ]


def test_digest_readset_file_skips_on_hold_and_invalid(client, run_processing_json, readset_file_json):
    """ON_HOLD and INVALID readsets are left out of the readset file, with a warning."""
    project_name = run_processing_json[vb.PROJECT_NAME]
    states = {
        "MoHQ-JG-9-23-15000863775-19933DT.A01433_0157_1": "ON_HOLD",
        "MoHQ-CM-1-3-15000936286-19866DN.A01433_0157_2": "INVALID",
    }
    for specimen in run_processing_json[vb.SPECIMEN]:
        for sample in specimen[vb.SAMPLE]:
            for readset in sample[vb.READSET]:
                if readset[vb.READSET_NAME] in states:
                    readset[vb.READSET_STATE] = states[readset[vb.READSET_NAME]]

    client.post(f'admin/create_project/{project_name}')
    client.post(f'project/{project_name}/ingest_run_processing', data=json.dumps(run_processing_json))
    response = client.get(f'project/{project_name}/digest_readset_file?json={json.dumps(readset_file_json)}')
    assert response.status_code == 200
    body = json.loads(response.text)

    expected = [row for row in READSET_FILE["DB_ACTION_OUTPUT"] if row["Readset"] not in states]
    assert sorted(body["DB_ACTION_OUTPUT"], key=lambda x: x["Readset"]) == sorted(expected, key=lambda x: x["Readset"])
    for readset_name, state in states.items():
        assert any(f"'{readset_name}'" in warning and f"'{state}'" in warning for warning in body["DB_ACTION_WARNING"])


def test_digest_unanalyzed_leaves_out_analysed_readsets(client, run_processing_json, genpipes_json):
    """Readsets linked to a live analysis operation are not unanalyzed; deleting the analysis brings them back."""
    project_name = run_processing_json[vb.PROJECT_NAME]
    client.post(f'admin/create_project/{project_name}')
    client.post(f'project/{project_name}/ingest_run_processing', data=json.dumps(run_processing_json))
    query = {"readset_name": True, "experiment_nucleic_acid_type": "RNA", "location_endpoint": "abacus"}

    def unanalyzed():
        response = client.get(f'project/{project_name}/digest_unanalyzed?json={json.dumps(query)}')
        return sorted(json.loads(response.text)["DB_ACTION_OUTPUT"][0]["readset_name"])

    analysed = sorted({readset[vb.READSET_NAME] for sample in genpipes_json[vb.SAMPLE] for readset in sample[vb.READSET]})
    before = unanalyzed()
    assert set(analysed) <= set(before)

    operation = json.loads(client.post(f'project/{project_name}/ingest_analysis', data=json.dumps(genpipes_json)).text)["DB_ACTION_OUTPUT"][0]
    assert unanalyzed() == sorted(set(before) - set(analysed))

    client.post('modification/delete', data=json.dumps({"modification": [{"table": "operation", "id": [operation["id"]]}]}))
    assert unanalyzed() == before
