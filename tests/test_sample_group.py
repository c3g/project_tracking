"""Groups of samples analysed together: declared at ingestion with sample_group_name, edited with /sample_group."""
import copy
import json

from project_tracking import vocabulary as vb

CM_GROUP = "MoHQ-CM-1-3_G1"
CM_TUMOUR = "MoHQ-CM-1-3-15000863775-19933DT"
CM_NORMAL = "MoHQ-CM-1-3-15000936286-19866DN"
CM_RNA = "MoHQ-CM-1-3-6929-1RT"
JG_TUMOUR = "MoHQ-JG-9-23-15000863775-19933DT"


def _sample(data, name):
    return next(s for specimen in data[vb.SPECIMEN] for s in specimen[vb.SAMPLE] if s[vb.SAMPLE_NAME] == name)


def _ingest(client, data):
    project_name = data[vb.PROJECT_NAME]
    client.post(f'admin/create_project/{project_name}')
    return json.loads(client.post(f'project/{project_name}/ingest_run_processing', data=json.dumps(data)).text)


def _groups(client, project_name, query=None):
    url = f'project/{project_name}/sample_groups'
    if query:
        url += f'?json={json.dumps(query)}'
    return json.loads(client.get(url).text)["DB_ACTION_OUTPUT"]


def _edit(client, project_name, **payload):
    return json.loads(client.post(f'project/{project_name}/sample_group', data=json.dumps(payload)).text)


def test_ingest_creates_and_fills_groups(client, run_processing_json):
    """A group is created by its first sample, the others join it; each creation is reported once."""
    body = _ingest(client, run_processing_json)
    created = sorted(w for w in body["DB_ACTION_WARNING"] if w.startswith("Created 'SampleGroup'"))
    assert created == [
        f"Created 'SampleGroup' '{CM_GROUP}' for 'Specimen' 'MoHQ-CM-1-3' with 'Sample' '{CM_RNA}', '{CM_TUMOUR}', '{CM_NORMAL}'.",
        "Created 'SampleGroup' 'MoHQ-JG-9-23_G1' for 'Specimen' 'MoHQ-JG-9-23' with 'Sample' "
        f"'{JG_TUMOUR}', 'MoHQ-JG-9-23-15000936286-19866DN'.",
    ]
    groups = {g["name"]: g for g in _groups(client, run_processing_json[vb.PROJECT_NAME])}
    assert groups[CM_GROUP]["specimen_name"] == "MoHQ-CM-1-3"
    assert groups[CM_GROUP]["sample_name"] == sorted([CM_TUMOUR, CM_NORMAL, CM_RNA])


def test_reingest_only_adds(client, run_processing_json):
    """Re-ingesting without the key, or with another group, never removes a sample from its groups."""
    project_name = run_processing_json[vb.PROJECT_NAME]
    _ingest(client, run_processing_json)

    second_run = json.loads(json.dumps(run_processing_json).replace("A01433_0157", "A01433_0999"))
    _sample(second_run, CM_TUMOUR)[vb.SAMPLE_GROUP_NAME] = ["MoHQ-CM-1-3_G2"]
    del _sample(second_run, CM_NORMAL)[vb.SAMPLE_GROUP_NAME]
    body = _ingest(client, second_run)
    assert [w for w in body["DB_ACTION_WARNING"] if w.startswith("Created 'SampleGroup'")] == [
        f"Created 'SampleGroup' 'MoHQ-CM-1-3_G2' for 'Specimen' 'MoHQ-CM-1-3' with 'Sample' '{CM_TUMOUR}'."
    ]
    groups = {g["name"]: g["sample_name"] for g in _groups(client, project_name, {"specimen_name": ["MoHQ-CM-1-3"]})}
    assert groups == {CM_GROUP: sorted([CM_TUMOUR, CM_NORMAL, CM_RNA]), "MoHQ-CM-1-3_G2": [CM_TUMOUR]}


def test_sample_groups_of_a_sample(client, run_processing_json):
    """sample_name gives every group holding the sample, each with all its samples."""
    project_name = run_processing_json[vb.PROJECT_NAME]
    _ingest(client, run_processing_json)
    _edit(client, project_name, sample_group_name="MoHQ-CM-1-3_G2", add=[CM_TUMOUR, CM_NORMAL])

    groups = {g["name"]: g["sample_name"] for g in _groups(client, project_name, {"sample_name": [CM_TUMOUR]})}
    assert groups == {CM_GROUP: sorted([CM_TUMOUR, CM_NORMAL, CM_RNA]), "MoHQ-CM-1-3_G2": sorted([CM_TUMOUR, CM_NORMAL])}
    assert {g["name"] for g in _groups(client, project_name, {"sample_name": [CM_RNA, JG_TUMOUR]})} == {"MoHQ-JG-9-23_G1", CM_GROUP}
    assert _groups(client, project_name, {"sample_name": ["NOT-A-SAMPLE"]}) == []


def test_ingest_rejects_invalid_groups(client, run_processing_json):
    """Invalid names, non list values and groups across specimens reject the payload before anything is written."""
    project_name = run_processing_json[vb.PROJECT_NAME]
    data = copy.deepcopy(run_processing_json)
    _sample(data, CM_TUMOUR)[vb.SAMPLE_GROUP_NAME] = ["MoHQ CM 1-3"]
    _sample(data, CM_NORMAL)[vb.SAMPLE_GROUP_NAME] = {"name": CM_GROUP}
    _sample(data, JG_TUMOUR)[vb.SAMPLE_GROUP_NAME] = [CM_GROUP]
    body = _ingest(client, data)
    errors = body["DB_ACTION_ERROR"]
    assert any(f"'Sample' '{CM_TUMOUR}': 'sample_group_name' 'MoHQ CM 1-3' is invalid" in e for e in errors)
    assert any(f"'Sample' '{CM_NORMAL}' has an invalid 'sample_group_name'" in e for e in errors)
    assert any(f"'SampleGroup' '{CM_GROUP}' would hold samples of several specimens" in e and JG_TUMOUR in e for e in errors)
    assert json.loads(client.get(f'project/{project_name}/samples').text).get("DB_ACTION_OUTPUT", []) == []


def test_ingest_rejects_sample_of_another_specimen(client, run_processing_json):
    """An existing group only takes samples of its specimen."""
    _ingest(client, run_processing_json)
    second_run = json.loads(json.dumps(run_processing_json).replace("A01433_0157", "A01433_0999"))
    _sample(second_run, JG_TUMOUR)[vb.SAMPLE_GROUP_NAME] = [CM_GROUP]
    body = _ingest(client, second_run)
    assert any(f"'SampleGroup' '{CM_GROUP}' belongs to 'Specimen' 'MoHQ-CM-1-3'" in e and JG_TUMOUR in e for e in body["DB_ACTION_ERROR"])


def test_edit_sample_group(client, run_processing_json):
    """Add creates the group, remove takes samples out, a group left empty is deleted."""
    project_name = run_processing_json[vb.PROJECT_NAME]
    _ingest(client, run_processing_json)

    body = _edit(client, project_name, sample_group_name="MoHQ-CM-1-3_G2", add=[CM_TUMOUR, CM_NORMAL])
    assert body["DB_ACTION_OUTPUT"][0]["sample_name"] == sorted([CM_TUMOUR, CM_NORMAL])
    assert body["DB_ACTION_WARNING"] == ["Created 'SampleGroup' 'MoHQ-CM-1-3_G2' for 'Specimen' 'MoHQ-CM-1-3'."]

    body = _edit(client, project_name, sample_group_name="MoHQ-CM-1-3_G2", remove=[CM_NORMAL])
    assert body["DB_ACTION_OUTPUT"][0]["sample_name"] == [CM_TUMOUR]

    body = _edit(client, project_name, sample_group_name="MoHQ-CM-1-3_G2", remove=[CM_TUMOUR])
    assert body["DB_ACTION_WARNING"] == ["'SampleGroup' 'MoHQ-CM-1-3_G2' has no sample left: deleted."]
    assert {g["name"] for g in _groups(client, project_name)} == {CM_GROUP, "MoHQ-JG-9-23_G1"}


def test_edit_sample_group_errors(client, run_processing_json):
    """Every problem is listed and nothing changes."""
    project_name = run_processing_json[vb.PROJECT_NAME]
    _ingest(client, run_processing_json)

    body = _edit(client, project_name, sample_group_name=CM_GROUP, add=[JG_TUMOUR, "NOT-A-SAMPLE"],
                 remove=["MoHQ-JG-9-23-15000936286-19866DN"])
    errors = body["DB_ACTION_ERROR"]
    assert "'Sample' 'NOT-A-SAMPLE' doesn't exist in this project." in errors
    assert f"'Sample' '{JG_TUMOUR}' of 'Specimen' 'MoHQ-JG-9-23' can't join 'SampleGroup' '{CM_GROUP}' of 'Specimen' 'MoHQ-CM-1-3'." in errors
    assert f"'Sample' 'MoHQ-JG-9-23-15000936286-19866DN' isn't in 'SampleGroup' '{CM_GROUP}'." in errors
    assert {g["name"]: g["sample_name"] for g in _groups(client, project_name, {"sample_group_name": [CM_GROUP]})} == {
        CM_GROUP: sorted([CM_TUMOUR, CM_NORMAL, CM_RNA])
    }

    assert "doesn't exist" in _edit(client, project_name, sample_group_name="NEW", remove=[CM_TUMOUR])["DB_ACTION_ERROR"]
    assert "Nothing to do" in _edit(client, project_name, sample_group_name=CM_GROUP)["DB_ACTION_ERROR"]
    assert "is invalid" in _edit(client, project_name, sample_group_name="bad name", add=[CM_TUMOUR])["DB_ACTION_ERROR"]


def _group_id(client, project_name, name):
    return _groups(client, project_name, {"sample_group_name": [name]})[0]["id"]


def _modify(client, action, table, ids, **edit):
    payload = {"modification": [dict({"table": table, "id": ids}, **edit)]}
    return json.loads(client.post(f'modification/{action}', data=json.dumps(payload)).text)


def test_rename_sample_group_with_edit(client, run_processing_json):
    """Groups are renamed with the edit route, which keeps the name format and uniqueness."""
    project_name = run_processing_json[vb.PROJECT_NAME]
    _ingest(client, run_processing_json)
    group_id = _group_id(client, project_name, CM_GROUP)

    body = _modify(client, "edit", "sample_group", [group_id], column="name", new="MoHQ-CM-1-3_G2")
    assert "DB_ACTION_ERROR" not in body
    assert {g["name"] for g in _groups(client, project_name)} == {"MoHQ-CM-1-3_G2", "MoHQ-JG-9-23_G1"}

    assert "is invalid" in _modify(client, "edit", "sample_group", [group_id], column="name", new="bad name")["DB_ACTION_ERROR"]
    assert _modify(client, "edit", "SampleGroup", [group_id], column="name", new="MoHQ-JG-9-23_G1")["DB_ACTION_ERROR"] == \
        "'SampleGroup' 'MoHQ-JG-9-23_G1' already exists."
    assert "can't be edited" in _modify(client, "edit", "sample_group", [group_id], column="specimen_id", new=1)["DB_ACTION_ERROR"]
    assert {g["name"] for g in _groups(client, project_name)} == {"MoHQ-CM-1-3_G2", "MoHQ-JG-9-23_G1"}


def test_deprecate_and_curate_sample_group(client, run_processing_json):
    """A deprecated group can't be joined at ingestion; curating it removes the group, not its samples."""
    project_name = run_processing_json[vb.PROJECT_NAME]
    _ingest(client, run_processing_json)
    group_id = _group_id(client, project_name, CM_GROUP)

    assert "DB_ACTION_ERROR" not in _modify(client, "deprecate", "sample_group", [group_id])
    assert _groups(client, project_name, {"sample_group_name": [CM_GROUP]})[0]["deprecated"] is True
    second_run = json.loads(json.dumps(run_processing_json).replace("A01433_0157", "A01433_0999"))
    assert f"'SampleGroup' '{CM_GROUP}' is deprecated: undo it or use another name." in _ingest(client, second_run)["DB_ACTION_ERROR"]

    assert "DB_ACTION_ERROR" not in _modify(client, "curate", "sample_group", [group_id])
    assert {g["name"] for g in _groups(client, project_name)} == {"MoHQ-JG-9-23_G1"}
    samples = json.loads(client.get(f'project/{project_name}/samples?json={json.dumps({"sample_name": CM_TUMOUR})}').text)
    assert samples["DB_ACTION_OUTPUT"][0]["sample_group_ids"] == []


def test_curate_sample_drops_its_memberships(client, run_processing_json):
    project_name = run_processing_json[vb.PROJECT_NAME]
    _ingest(client, run_processing_json)
    samples = json.loads(client.get(f'project/{project_name}/samples?json={json.dumps({"sample_name": CM_RNA})}').text)["DB_ACTION_OUTPUT"]
    response = client.post('/modification/curate', json={"modification": [{"table": "Sample", "id": [samples[0]["id"]]}]})
    assert response.status_code == 200
    assert _groups(client, project_name, {"sample_group_name": [CM_GROUP]})[0]["sample_name"] == sorted([CM_TUMOUR, CM_NORMAL])
