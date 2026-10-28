"""
Cascades of the modification routes: down a link, a row is reached when all its parents through that link are gone;
up (readset > sample > specimen, location > file), a parent is reached when all its children are gone. Curate never
removes a row the request doesn't reach.
"""
import json

import pytest

from project_tracking import database, model
from project_tracking import vocabulary as vb


@pytest.fixture()
def project(client, run_processing_json):
    """MOH-Q with run_processing.json: specimen MoHQ-JG-9-23 has 2 samples of 1 readset each, in one group."""
    name = run_processing_json[vb.PROJECT_NAME]
    client.post(f'admin/create_project/{name}')
    client.post(f'project/{name}/ingest_run_processing', data=json.dumps(run_processing_json))
    return name


def _post(client, project, action, table, ids, mode=None, **flags):
    body = {"modification": [{"table": table, "id": ids}], **flags}
    if mode:
        body[mode] = True
    return client.post(f'/project/{project}/modification/{action}', data=json.dumps(body)).get_json()


def _session(app):
    session = database.get_session()
    session.expire_all()
    return session


def _jg_specimen(session):
    return session.query(model.Specimen).filter_by(name="MoHQ-JG-9-23").one()


def _counts(app):
    with app.app_context():
        session = _session(app)
        return {cls.__name__: session.query(cls).count()
                for cls in (model.Specimen, model.Sample, model.SampleGroup, model.Readset, model.File, model.Location,
                            model.Metric, model.Job, model.Operation)}


def _own_files(readsets):
    """Files linked only to these readsets, with their locations."""
    ids = {readset.id for readset in readsets}
    files = {file for readset in readsets for file in readset.files if {r.id for r in file.readsets} <= ids}
    return files, {location for file in files for location in file.locations}


def test_delete_specimen_cascade_down_reaches_its_rows(app, client, project):
    with app.app_context():
        specimen = _jg_specimen(_session(app))
        specimen_id, sample_ids = specimen.id, [s.id for s in specimen.samples]

    result = _post(client, project, "delete", "specimen", [specimen_id], "cascade_down")

    assert "DB_ACTION_ERROR" not in result, result
    with app.app_context():
        session = _session(app)
        specimen = _jg_specimen(session)
        readsets = [r for s in specimen.samples for r in s.readsets]
        files, locations = _own_files(readsets)
        assert specimen.deleted
        assert all(s.deleted for s in specimen.samples) and all(g.deleted for g in specimen.sample_groups)
        assert readsets and all(r.deleted for r in readsets)
        assert files and all(f.deleted for f in files) and all(loc.deleted for loc in locations)
        assert all(m.deleted for r in readsets for m in r.metrics)
        # The other specimen, and the run processing job shared with its readsets, stay
        assert session.query(model.Sample).filter_by(deleted=True).count() == len(sample_ids)
        assert not any(j.deleted for r in readsets for j in r.jobs)
    cascaded = f"'Sample' with id '{sample_ids[0]}' deleted (cascade from 'Specimen' with id '{specimen_id}')."
    assert cascaded in result["DB_ACTION_OUTPUT"]


def test_delete_one_sample_cascade_up_keeps_specimen(app, client, project):
    with app.app_context():
        specimen = _jg_specimen(_session(app))
        specimen_id, sample_ids = specimen.id, [s.id for s in specimen.samples]

    result = _post(client, project, "delete", "sample", sample_ids[:1], "cascade_up")

    with app.app_context():
        assert not _jg_specimen(_session(app)).deleted
    stopped = f"'Specimen' with id '{specimen_id}' not deleted: 'Sample' with id '{sample_ids[1]}' isn't."
    assert stopped in result["DB_ACTION_WARNING"]


def test_delete_all_samples_cascade_up_deletes_specimen_in_any_order(app, client, project):
    with app.app_context():
        sample_ids = [s.id for s in _jg_specimen(_session(app)).samples]

    result = _post(client, project, "delete", "sample", list(reversed(sample_ids)), "cascade_up")

    assert "DB_ACTION_WARNING" not in result, result
    with app.app_context():
        assert _jg_specimen(_session(app)).deleted


def test_delete_readset_cascade_down_keeps_shared_job(app, client, project):
    with app.app_context():
        readset = _jg_specimen(_session(app)).samples[0].readsets[0]
        readset_id, job_ids = readset.id, [j.id for j in readset.jobs]
        file_ids = [f.id for f in _own_files([readset])[0]]
        assert job_ids and all(len(j.readsets) > 1 for j in readset.jobs)

    _post(client, project, "delete", "readset", [readset_id], "cascade_down")

    with app.app_context():
        session = _session(app)
        assert all(session.get(model.File, i).deleted for i in file_ids)
        assert not any(session.get(model.Job, i).deleted for i in job_ids)


def test_delete_job_cascade_down_takes_its_metrics_and_files(app, client, project):
    with app.app_context():
        job = _session(app).query(model.Job).first()
        job_id, metric_ids = job.id, [m.id for m in job.metrics]
        file_ids = [f.id for f in job.files if [j.id for j in f.jobs] == [job_id]]

    _post(client, project, "delete", "job", [job_id], "cascade_down")

    with app.app_context():
        session = _session(app)
        assert all(session.get(model.Metric, i).deleted for i in metric_ids)
        assert file_ids and all(session.get(model.File, i).deleted for i in file_ids)
        assert all(loc.deleted for i in file_ids for loc in session.get(model.File, i).locations)
        assert session.query(model.Readset).filter_by(deleted=True).count() == 0


def test_sample_group_deleted_once_all_its_samples_are(app, client, project):
    with app.app_context():
        specimen = _jg_specimen(_session(app))
        sample_ids, group_id = [s.id for s in specimen.samples], specimen.sample_groups[0].id

    _post(client, project, "delete", "sample", sample_ids[:1], "cascade_down")
    with app.app_context():
        assert not _session(app).get(model.SampleGroup, group_id).deleted
    _post(client, project, "delete", "sample", sample_ids[1:], "cascade_down")
    with app.app_context():
        assert _session(app).get(model.SampleGroup, group_id).deleted


def test_undelete_sample_cascade_up_restores_specimen_not_siblings(app, client, project):
    with app.app_context():
        specimen = _jg_specimen(_session(app))
        specimen_id, sample_ids = specimen.id, [s.id for s in specimen.samples]
    _post(client, project, "delete", "sample", sample_ids, "cascade_up")

    _post(client, project, "undelete", "sample", sample_ids[:1], "cascade_up")

    with app.app_context():
        session = _session(app)
        assert not session.get(model.Specimen, specimen_id).deleted
        assert not session.get(model.Sample, sample_ids[0]).deleted
        assert session.get(model.Sample, sample_ids[1]).deleted


def test_undelete_specimen_cascade_down_restores_its_rows(app, client, project):
    with app.app_context():
        specimen_id = _jg_specimen(_session(app)).id
    _post(client, project, "delete", "specimen", [specimen_id], "cascade_down")

    _post(client, project, "undelete", "specimen", [specimen_id], "cascade_down")

    with app.app_context():
        session = _session(app)
        for cls in (model.Specimen, model.Sample, model.SampleGroup, model.Readset, model.File, model.Location, model.Metric):
            assert session.query(cls).filter_by(deleted=True).count() == 0, cls.__name__


def test_deprecate_follows_the_same_rules(app, client, project):
    with app.app_context():
        specimen = _jg_specimen(_session(app))
        specimen_id, sample_ids = specimen.id, [s.id for s in specimen.samples]

    _post(client, project, "deprecate", "sample", sample_ids[:1], "cascade_up")
    with app.app_context():
        assert not _session(app).get(model.Specimen, specimen_id).deprecated
    _post(client, project, "deprecate", "specimen", [specimen_id], "cascade_down")
    with app.app_context():
        assert all(r.deprecated for s in _jg_specimen(_session(app)).samples for r in s.readsets)


def test_cascade_dry_run_reports_without_saving(app, client, project):
    with app.app_context():
        specimen_id = _jg_specimen(_session(app)).id

    result = _post(client, project, "delete", "specimen", [specimen_id], "cascade_down", dry_run=True)

    assert len(result["DB_ACTION_OUTPUT"]) > 5
    assert "Dry run: nothing was saved." in result["DB_ACTION_WARNING"]
    with app.app_context():
        assert _session(app).query(model.Sample).filter_by(deleted=True).count() == 0


def test_curate_one_sample_cascade_up_is_refused(app, client, project):
    """The sample's readset isn't reached going up, and curate never removes a row the request doesn't reach."""
    with app.app_context():
        sample = _jg_specimen(_session(app)).samples[0]
        sample_id, readset_id = sample.id, sample.readsets[0].id
    before = _counts(app)

    result = _post(client, project, "curate", "sample", [sample_id], "cascade_up")

    assert f"'Readset' with id '{readset_id}'" in result["DB_ACTION_ERROR"]
    assert _counts(app) == before


def test_curate_one_sample_cascade_removes_exactly_what_it_reports(app, client, project):
    with app.app_context():
        specimen = _jg_specimen(_session(app))
        specimen_id, sample_id = specimen.id, specimen.samples[0].id
    before = _counts(app)

    result = _post(client, project, "curate", "sample", [sample_id], "cascade")

    assert "DB_ACTION_ERROR" not in result, result
    removed = {}
    for line in result["DB_ACTION_OUTPUT"]:
        table = line.split("'")[1]
        removed[table] = removed.get(table, 0) + 1
    after = _counts(app)
    assert {table: before[table] - after[table] for table in before if before[table] != after[table]} == removed
    assert removed["Sample"] == 1 and removed["Readset"] == 1 and "Specimen" not in removed
    with app.app_context():
        assert _session(app).get(model.Specimen, specimen_id) is not None


def test_plain_curate_of_a_used_row_is_refused(app, client, project):
    with app.app_context():
        run_id = _session(app).query(model.Run).first().id
    before = _counts(app)

    result = _post(client, project, "curate", "run", [run_id])

    assert "Nothing was curated" in result["DB_ACTION_ERROR"]
    assert _counts(app) == before


def test_plain_curate_of_a_leaf_row(app, client, project):
    with app.app_context():
        location_id = _session(app).query(model.Location).first().id

    result = _post(client, project, "curate", "location", [location_id])

    assert result["DB_ACTION_OUTPUT"] == [f"'Location' with id '{location_id}' permanently deleted."]
    with app.app_context():
        assert _session(app).get(model.Location, location_id) is None
