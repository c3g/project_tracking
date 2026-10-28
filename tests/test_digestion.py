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


def test_digest_pair_file_sample_groups(client, run_processing_json):
    """Pairs are the tumour/normal combinations of each sample group, by specimen for samples in no group."""
    project_name = run_processing_json[vb.PROJECT_NAME]
    cm_specimen = next(s for s in run_processing_json[vb.SPECIMEN] if s[vb.SPECIMEN_NAME] == "MoHQ-CM-1-3")
    samples = {s[vb.SAMPLE_NAME]: s for s in cm_specimen[vb.SAMPLE]}
    tumour = samples["MoHQ-CM-1-3-15000863775-19933DT"]
    normal = samples["MoHQ-CM-1-3-15000936286-19866DN"]
    normal_2 = _clone_sample(normal, "MoHQ-CM-1-3-15000936286-2DN")
    tumour_1 = _clone_sample(tumour, "MoHQ-CM-1-3-F10-1DT1")

    # A sample can be in several groups, e.g. the normals here
    tumour[vb.SAMPLE_GROUP_NAME] = ["MoHQ-CM-1-3_G1", "MoHQ-CM-1-3_G2"]
    normal[vb.SAMPLE_GROUP_NAME] = ["MoHQ-CM-1-3_G1", "MoHQ-CM-1-3_G3"]
    normal_2[vb.SAMPLE_GROUP_NAME] = ["MoHQ-CM-1-3_G2", "MoHQ-CM-1-3_G3"]
    tumour_1[vb.SAMPLE_GROUP_NAME] = "MoHQ-CM-1-3_G3"
    cm_specimen[vb.SAMPLE].extend([normal_2, tumour_1])
    # No group: fallback on specimen
    for specimen in run_processing_json[vb.SPECIMEN]:
        if specimen[vb.SPECIMEN_NAME] == "MoHQ-JG-9-23":
            for sample in specimen[vb.SAMPLE]:
                sample.pop(vb.SAMPLE_GROUP_NAME, None)

    client.post(f'admin/create_project/{project_name}')
    response = client.post(f'project/{project_name}/ingest_run_processing', data=json.dumps(run_processing_json))
    assert response.status_code == 200
    created = [w for w in json.loads(response.text)["DB_ACTION_WARNING"] if w.startswith("Created 'SampleGroup'")]
    assert len(created) == 3

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
        {"Specimen": "MoHQ-JG-9-23", "Sample_N": "MoHQ-JG-9-23-15000936286-19866DN", "Sample_T": "MoHQ-JG-9-23-15000863775-19933DT"},
    ]
    assert sorted(body["DB_ACTION_OUTPUT"], key=key) == sorted(expected, key=key)


def test_sample_group_across_ingestions(client, run_processing_json):
    """A sample ingested later joins the group by naming it: the duo grows into a trio, under the same name."""
    project_name = run_processing_json[vb.PROJECT_NAME]
    jg_specimen = next(s for s in run_processing_json[vb.SPECIMEN] if s[vb.SPECIMEN_NAME] == "MoHQ-JG-9-23")
    tumour = next(s for s in jg_specimen[vb.SAMPLE] if s[vb.SAMPLE_TUMOUR])
    late_normal = _clone_sample(
        next(s for s in jg_specimen[vb.SAMPLE] if not s[vb.SAMPLE_TUMOUR]),
        "MoHQ-JG-9-23-B1-2DN"
    )
    tumour[vb.SAMPLE_GROUP_NAME] = ["MoHQ-JG-9-23_G2"]

    client.post(f'admin/create_project/{project_name}')
    client.post(f'project/{project_name}/ingest_run_processing', data=json.dumps(run_processing_json))

    late_normal[vb.SAMPLE_GROUP_NAME] = ["MoHQ-JG-9-23_G2"]
    second_run = dict(run_processing_json, run_name="second_run", run_ext_id="2")
    second_run[vb.SPECIMEN] = [dict(jg_specimen, sample=[late_normal])]
    response = client.post(f'project/{project_name}/ingest_run_processing', data=json.dumps(second_run))
    assert response.status_code == 200
    assert "DB_ACTION_WARNING" not in json.loads(response.text) or not any(
        w.startswith("Created 'SampleGroup'") for w in json.loads(response.text)["DB_ACTION_WARNING"])

    digest_json = {vb.SAMPLE_NAME: [tumour[vb.SAMPLE_NAME], late_normal[vb.SAMPLE_NAME]], vb.EXPERIMENT_NUCLEIC_ACID_TYPE: "DNA"}
    response = client.get(f'project/{project_name}/digest_pair_file?json={json.dumps(digest_json)}')
    assert json.loads(response.text)["DB_ACTION_OUTPUT"] == [
        {"Specimen": "MoHQ-JG-9-23", "Sample_N": late_normal[vb.SAMPLE_NAME], "Sample_T": tumour[vb.SAMPLE_NAME]}
    ]
    groups = json.loads(client.get(f'project/{project_name}/sample_groups?json={json.dumps({"sample_group_name": ["MoHQ-JG-9-23_G2"]})}').text)
    assert groups["DB_ACTION_OUTPUT"][0]["sample_name"] == sorted([tumour[vb.SAMPLE_NAME], late_normal[vb.SAMPLE_NAME]])


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

    client.post(f'project/{project_name}/modification/delete', data=json.dumps({"modification": [{"table": "operation", "id": [operation["id"]]}]}))
    assert unanalyzed() == before


ONCOANALYSER_SPECIMEN = "MoHQ-CM-1-3"
ONCOANALYSER_GROUP = "MoHQ-CM-1-3_G1"
ONCOANALYSER_DIR = "/lb/robot/research/processing/novaseq/2022/220420_A01433_0157_BHM3NHDSX2_MoHRun08-novaseq/Unaligned"


def _fastq_files(readset_name, lane):
    """R1 and R2 fastq files of a readset on abacus."""
    return [
        {
            "location_uri": f"abacus://{ONCOANALYSER_DIR}.{lane}/{readset_name}_L00{lane}_{read_type}_001.fastq.gz",
            "file_name": f"{readset_name}_L00{lane}_{read_type}_001.fastq.gz",
            "file_md5sum": f"{read_type}{readset_name}",
            "file_extra_metadata": {"read_type": read_type},
            "file_deliverable": True,
        }
        for read_type in ("R1", "R2")
    ]


def _oncoanalyser_json(run_processing_json):
    """MoHQ-CM-1-3 DNA readsets get fastq files: the specimen then has a tumour, a normal and an RNA tumour."""
    for specimen in run_processing_json[vb.SPECIMEN]:
        for sample in specimen[vb.SAMPLE]:
            for readset in sample[vb.READSET]:
                if specimen[vb.SPECIMEN_NAME] == ONCOANALYSER_SPECIMEN and readset[vb.EXPERIMENT_NUCLEIC_ACID_TYPE] == "DNA":
                    readset[vb.FILE] = _fastq_files(readset[vb.READSET_NAME], readset[vb.READSET_LANE])
    return run_processing_json


def _samplesheet(client, project_name, query):
    response = client.get(f'project/{project_name}/digest_oncoanalyser_samplesheet?json={json.dumps(query)}')
    return response.status_code, json.loads(response.text)


def test_digest_oncoanalyser_samplesheet(client, run_processing_json):
    """One fastq row per readset of the group, DNA and RNA together: DNA tumour, DNA normal, then RNA by lane."""
    project_name = run_processing_json[vb.PROJECT_NAME]
    client.post(f'admin/create_project/{project_name}')
    client.post(f'project/{project_name}/ingest_run_processing', data=json.dumps(_oncoanalyser_json(run_processing_json)))

    status, body = _samplesheet(client, project_name, {"specimen_name": [ONCOANALYSER_SPECIMEN], "location_endpoint": "abacus"})
    assert status == 200
    assert "DB_ACTION_WARNING" not in body
    rows = body["DB_ACTION_OUTPUT"]
    rna_dir = "/lb/robot/research/processing/novaseq/2022/220420_A01433_0157_BHM3NHDSX2_MoHRun08-novaseq"
    assert [(row["sample_id"], row["sample_type"], row["sequence_type"], row["info"]) for row in rows] == [
        ("MoHQ-CM-1-3-15000863775-19933DT", "tumor", "dna", "library_id:MoHQ-CM-1-3-15000863775-19933DT.A01433_0157_1;lane:001;flowcell:HM3NHDSX2"),
        ("MoHQ-CM-1-3-15000936286-19866DN", "normal", "dna", "library_id:MoHQ-CM-1-3-15000936286-19866DN.A01433_0157_2;lane:002;flowcell:HM3NHDSX2"),
        ("MoHQ-CM-1-3-6929-1RT", "tumor", "rna", "library_id:MoHQ-CM-1-3-6929-1RT.A01433_0157_1;lane:001;flowcell:HM3NHDSX2"),
        ("MoHQ-CM-1-3-6929-1RT", "tumor", "rna", "library_id:MoHQ-CM-1-3-6929-1RT.A01433_0157_2;lane:002;flowcell:HM3NHDSX2"),
        ("MoHQ-CM-1-3-6929-1RT", "tumor", "rna", "library_id:MoHQ-CM-1-3-6929-1RT.A01433_0157_3;lane:003;flowcell:HM3NHDSX2"),
    ]
    assert all(row["group_id"] == ONCOANALYSER_GROUP and row["subject_id"] == ONCOANALYSER_SPECIMEN and row["filetype"] == "fastq" for row in rows)
    assert rows[0]["filepath"] == (
        f"{ONCOANALYSER_DIR}.1/MoHQ-CM-1-3-15000863775-19933DT.A01433_0157_1_L001_R1_001.fastq.gz;"
        f"{ONCOANALYSER_DIR}.1/MoHQ-CM-1-3-15000863775-19933DT.A01433_0157_1_L001_R2_001.fastq.gz"
    )
    assert rows[4]["filepath"] == (
        f"{rna_dir}/Unaligned.3/Project_MES1431/Sample_MoHQ-CM-1-3-6929-1RT_2-2053929/MoHQ-CM-1-3-6929-1RT_2-2053929_S2_L003_R1_001.fastq.gz;"
        f"{rna_dir}/Unaligned.3/Project_MES1431/Sample_MoHQ-CM-1-3-6929-1RT_2-2053929/MoHQ-CM-1-3-6929-1RT_2-2053929_S2_L003_R2_001.fastq.gz"
    )

    _, body = _samplesheet(client, project_name, {"specimen_name": [ONCOANALYSER_SPECIMEN], "location_endpoint": "abacus", "experiment_nucleic_acid_type": "RNA"})
    assert {row["sequence_type"] for row in body["DB_ACTION_OUTPUT"]} == {"rna"}

    _, body = _samplesheet(client, project_name, {"sample_group_name": [ONCOANALYSER_GROUP, "MoHQ-CM-1-3_G404"], "location_endpoint": "abacus"})
    assert body["DB_ACTION_OUTPUT"] == rows
    assert body["DB_ACTION_WARNING"] == ["'SampleGroup' 'MoHQ-CM-1-3_G404' doesn't exist in this project."]

    _, body = _samplesheet(client, project_name, {"sample_name": ["MoHQ-CM-1-3-6929-1RT"], "location_endpoint": "abacus"})
    assert "DB_ACTION_OUTPUT" not in body
    assert "'specimen_name', 'specimen_id' or 'sample_group_name' is required" in body["DB_ACTION_ERROR"]


def test_digest_oncoanalyser_samplesheet_cancer_type(client, run_processing_json):
    """The tumour samples' cancer type goes in info when oncoanalyser can take it, otherwise it's left out with a warning."""
    project_name = run_processing_json[vb.PROJECT_NAME]
    data = _oncoanalyser_json(run_processing_json)
    cancer_types = {"MoHQ-CM-1-3-15000863775-19933DT": "Lung", "MoHQ-CM-1-3-6929-1RT": "Lung;Adenocarcinoma",
                    "MoHQ-CM-1-3-15000936286-19866DN": "Lung"}
    for specimen in data[vb.SPECIMEN]:
        for sample in specimen[vb.SAMPLE]:
            if sample[vb.SAMPLE_NAME] in cancer_types:
                sample[vb.SAMPLE_CANCER_TYPE] = cancer_types[sample[vb.SAMPLE_NAME]]
    client.post(f'admin/create_project/{project_name}')
    client.post(f'project/{project_name}/ingest_run_processing', data=json.dumps(data))

    _, body = _samplesheet(client, project_name, {"specimen_name": [ONCOANALYSER_SPECIMEN], "location_endpoint": "abacus"})
    info = {row["sample_id"]: row["info"] for row in body["DB_ACTION_OUTPUT"]}
    assert info["MoHQ-CM-1-3-15000863775-19933DT"].endswith(";cancer_type:Lung")
    assert "cancer_type" not in info["MoHQ-CM-1-3-15000936286-19866DN"]
    assert "cancer_type" not in info["MoHQ-CM-1-3-6929-1RT"]
    assert len([w for w in body["DB_ACTION_WARNING"] if "'Lung;Adenocarcinoma' left out" in w]) == 3


def test_digest_oncoanalyser_samplesheet_skips(client, run_processing_json):
    """Readsets without fastq files at the endpoint, ON_HOLD and INVALID readsets are left out with a warning."""
    project_name = run_processing_json[vb.PROJECT_NAME]
    data = _oncoanalyser_json(run_processing_json)
    for specimen in data[vb.SPECIMEN]:
        for sample in specimen[vb.SAMPLE]:
            for readset in sample[vb.READSET]:
                if readset[vb.READSET_NAME] == "MoHQ-CM-1-3-6929-1RT.A01433_0157_3":
                    readset[vb.READSET_STATE] = "ON_HOLD"
    client.post(f'admin/create_project/{project_name}')
    client.post(f'project/{project_name}/ingest_run_processing', data=json.dumps(data))

    status, body = _samplesheet(client, project_name, {"specimen_name": ["MoHQ-JG-9-23", ONCOANALYSER_SPECIMEN], "location_endpoint": "abacus"})
    assert status == 200
    assert {row["group_id"] for row in body["DB_ACTION_OUTPUT"]} == {ONCOANALYSER_GROUP}
    assert len(body["DB_ACTION_OUTPUT"]) == 4
    warnings = body["DB_ACTION_WARNING"]
    assert any("'MoHQ-CM-1-3-6929-1RT.A01433_0157_3'" in w and "'ON_HOLD'" in w for w in warnings)
    for readset_name in ("MoHQ-JG-9-23-15000863775-19933DT.A01433_0157_1", "MoHQ-JG-9-23-15000936286-19866DN.A01433_0157_2"):
        assert any(f"'{readset_name}'" in w and "no R1 and R2 fastq 'File' in 'abacus'" in w for w in warnings)


def test_digest_oncoanalyser_samplesheet_one_sample_per_type(client, run_processing_json):
    """Two DNA tumours in one group can't go in one oncoanalyser samplesheet: rejected until one leaves the group."""
    project_name = run_processing_json[vb.PROJECT_NAME]
    data = _oncoanalyser_json(run_processing_json)
    for specimen in data[vb.SPECIMEN]:
        if specimen[vb.SPECIMEN_NAME] == ONCOANALYSER_SPECIMEN:
            tumour = next(s for s in specimen[vb.SAMPLE] if s[vb.SAMPLE_NAME] == "MoHQ-CM-1-3-15000863775-19933DT")
            second = _clone_sample(tumour, "MoHQ-CM-1-3-15000863775-2DT")
            for readset in second[vb.READSET]:
                readset[vb.FILE] = _fastq_files(readset[vb.READSET_NAME], readset[vb.READSET_LANE])
            specimen[vb.SAMPLE].append(second)
    client.post(f'admin/create_project/{project_name}')
    client.post(f'project/{project_name}/ingest_run_processing', data=json.dumps(data))
    query = {"specimen_name": [ONCOANALYSER_SPECIMEN], "location_endpoint": "abacus"}

    _, body = _samplesheet(client, project_name, query)
    assert "DB_ACTION_OUTPUT" not in body
    assert any(f"'SampleGroup' '{ONCOANALYSER_GROUP}'" in e and "'MoHQ-CM-1-3-15000863775-19933DT', 'MoHQ-CM-1-3-15000863775-2DT'" in e
               for e in body["DB_ACTION_ERROR"])

    edit = {"sample_group_name": ONCOANALYSER_GROUP, "remove": ["MoHQ-CM-1-3-15000863775-19933DT"]}
    assert "DB_ACTION_ERROR" not in json.loads(client.post(f'project/{project_name}/sample_group', data=json.dumps(edit)).text)
    _, body = _samplesheet(client, project_name, query)
    sample_ids = {row["sample_id"] for row in body["DB_ACTION_OUTPUT"]}
    assert "MoHQ-CM-1-3-15000863775-2DT" in sample_ids and "MoHQ-CM-1-3-15000863775-19933DT" not in sample_ids
    assert "'Sample' 'MoHQ-CM-1-3-15000863775-19933DT' of 'Specimen' 'MoHQ-CM-1-3' is in no 'SampleGroup': left out." in body["DB_ACTION_WARNING"]


def test_digest_oncoanalyser_samplesheet_two_groups(client, run_processing_json):
    """A specimen with a duo and a trio sharing the normal gives one samplesheet per group."""
    project_name = run_processing_json[vb.PROJECT_NAME]
    data = _oncoanalyser_json(run_processing_json)
    for specimen in data[vb.SPECIMEN]:
        if specimen[vb.SPECIMEN_NAME] == ONCOANALYSER_SPECIMEN:
            samples = {s[vb.SAMPLE_NAME]: s for s in specimen[vb.SAMPLE]}
            relapse = _clone_sample(samples["MoHQ-CM-1-3-15000863775-19933DT"], "MoHQ-CM-1-3-15000863775-2DT")
            for readset in relapse[vb.READSET]:
                readset[vb.FILE] = _fastq_files(readset[vb.READSET_NAME], readset[vb.READSET_LANE])
            relapse[vb.SAMPLE_GROUP_NAME] = ["MoHQ-CM-1-3_G2"]
            samples["MoHQ-CM-1-3-15000936286-19866DN"][vb.SAMPLE_GROUP_NAME] = [ONCOANALYSER_GROUP, "MoHQ-CM-1-3_G2"]
            specimen[vb.SAMPLE].append(relapse)
    client.post(f'admin/create_project/{project_name}')
    client.post(f'project/{project_name}/ingest_run_processing', data=json.dumps(data))

    _, body = _samplesheet(client, project_name, {"specimen_name": [ONCOANALYSER_SPECIMEN], "location_endpoint": "abacus"})
    by_group = {}
    for row in body["DB_ACTION_OUTPUT"]:
        by_group.setdefault(row["group_id"], []).append((row["sample_id"], row["sequence_type"]))
    assert by_group == {
        ONCOANALYSER_GROUP: [
            ("MoHQ-CM-1-3-15000863775-19933DT", "dna"), ("MoHQ-CM-1-3-15000936286-19866DN", "dna"),
            ("MoHQ-CM-1-3-6929-1RT", "rna"), ("MoHQ-CM-1-3-6929-1RT", "rna"), ("MoHQ-CM-1-3-6929-1RT", "rna"),
        ],
        "MoHQ-CM-1-3_G2": [("MoHQ-CM-1-3-15000863775-2DT", "dna"), ("MoHQ-CM-1-3-15000936286-19866DN", "dna")],
    }
    assert {row["subject_id"] for row in body["DB_ACTION_OUTPUT"]} == {ONCOANALYSER_SPECIMEN}


def test_digest_oncoanalyser_samplesheet_same_lane_twice(client, run_processing_json):
    """A lane sequenced on two runs: each readset is its own library_id, with its run's flowcell."""
    project_name = run_processing_json[vb.PROJECT_NAME]
    data = _oncoanalyser_json(run_processing_json)
    client.post(f'admin/create_project/{project_name}')
    client.post(f'project/{project_name}/ingest_run_processing', data=json.dumps(data))
    second_run = json.loads(json.dumps(data).replace("A01433_0157_BHM3NHDSX2", "A01433_0999_BHM9ZZDSX2").replace("A01433_0157", "A01433_0999"))
    client.post(f'project/{project_name}/ingest_run_processing', data=json.dumps(second_run))

    _, body = _samplesheet(client, project_name, {"specimen_name": [ONCOANALYSER_SPECIMEN], "location_endpoint": "abacus", "experiment_nucleic_acid_type": "DNA"})
    assert "DB_ACTION_ERROR" not in body
    assert [row["info"] for row in body["DB_ACTION_OUTPUT"] if row["sample_id"] == "MoHQ-CM-1-3-15000863775-19933DT"] == [
        "library_id:MoHQ-CM-1-3-15000863775-19933DT.A01433_0157_1;lane:001;flowcell:HM3NHDSX2",
        "library_id:MoHQ-CM-1-3-15000863775-19933DT.A01433_0999_1;lane:001;flowcell:HM9ZZDSX2",
    ]
