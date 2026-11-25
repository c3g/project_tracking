"""Metrics of operations in the metric vocabulary: one live value per readset, sample or set of samples."""
import copy

import pytest
from sqlalchemy import select

from project_tracking import db_actions, model
from project_tracking import vocabulary as vb
from project_tracking.db_actions.errors import RequestError
from project_tracking.metrics import DEFAULT_METRIC_VOCABULARY, metric_level, validate_metric_vocabulary

# Samples of tests/data/run_processing.json: a DNA tumour, a DNA normal and an RNA tumour with 3 readsets
DT, DN, RT = "MoHQ-CM-1-3-15000863775-19933DT", "MoHQ-CM-1-3-15000936286-19866DN", "MoHQ-CM-1-3-6929-1RT"
READSETS = {
    DT: [f"{DT}.A01433_0157_1"],
    DN: [f"{DN}.A01433_0157_2"],
    RT: [f"{RT}.A01433_0157_1", f"{RT}.A01433_0157_2", f"{RT}.A01433_0157_3"],
}


def _payload(operation, jobs, stop="2026-11-20 10:00:00"):
    """
    An analysis payload, jobs: [(job name, {sample: [(metric name, value)]})]: each job is sent under every readset
    of its samples, with the metrics of the sample.
    """
    samples = {}
    for job_name, metrics in jobs:
        for sample, sample_metrics in metrics.items():
            for readset in READSETS[sample]:
                readsets = samples.setdefault(sample, {})
                readsets.setdefault(readset, []).append({
                    vb.JOB_NAME: job_name,
                    vb.JOB_STATUS: "COMPLETED",
                    vb.JOB_START: "2026-11-20 08:00:00",
                    vb.JOB_STOP: stop,
                    vb.METRIC: [{vb.METRIC_NAME: n, vb.METRIC_VALUE: v, vb.METRIC_FLAG: "PASS"} for n, v in sample_metrics],
                })
    return {
        vb.OPERATION_PLATFORM: "cardinal",
        vb.OPERATION_CMD_LINE: f"{operation} run",
        vb.OPERATION_NAME: operation,
        **({vb.OPERATION_PIPELINE: "wgts"} if operation == "oncoanalyser" else {}),
        vb.OPERATION_CONFIG_NAME: f"{operation}_ini",
        vb.OPERATION_CONFIG_VERSION: "1",
        vb.OPERATION_CONFIG_MD5SUM: "0" * 32,
        vb.OPERATION_CONFIG_DATA: "params {}",
        vb.SAMPLE: [
            {vb.SAMPLE_NAME: sample, vb.READSET: [{vb.READSET_NAME: r, vb.JOB: j} for r, j in readsets.items()]}
            for sample, readsets in samples.items()
        ],
    }


@pytest.fixture
def project_id(not_app_db, run_processing_json):
    name = run_processing_json[vb.PROJECT_NAME]
    db_actions.create_project(name, session=not_app_db)
    project_id = db_actions.name_to_id("Project", name, session=not_app_db)[0]
    db_actions.ingest_run_processing(project_id, run_processing_json, not_app_db)
    return project_id


def _metrics(session, name):
    """(value, level, deprecated, sorted readset names) of the metrics named name, oldest first."""
    rows = session.execute(select(model.Metric).where(model.Metric.name == name).order_by(model.Metric.id)).scalars()
    return [(m.value, metric_level(m.job.operation.name, m.name), m.deprecated, sorted(r.name for r in m.readsets))
            for m in rows]


def test_default_vocabulary_is_valid():
    validate_metric_vocabulary(DEFAULT_METRIC_VOCABULARY)


def test_sample_metric_is_one_row_for_all_readsets(not_app_db, project_id):
    payload = _payload("oncoanalyser", [("isofox.RT", {RT: [("total_fragments", "55")]})])
    db_actions.ingest_analysis(project_id, payload, not_app_db)
    assert _metrics(not_app_db, "total_fragments") == [("55", "SAMPLE", False, sorted(READSETS[RT]))]


def test_newer_job_deprecates_and_older_job_is_stored_deprecated(not_app_db, project_id):
    db_actions.ingest_analysis(project_id, _payload("oncoanalyser", [("qsee.DT", {DT: [("purity", "0.26")]})]), not_app_db)
    # A top-up rerun: a new job (later stop), value replaced
    rerun = _payload("oncoanalyser", [("qsee.DT", {DT: [("purity", "0.31")]})], stop="2026-11-22 10:00:00")
    ret = db_actions.ingest_analysis(project_id, rerun, not_app_db)
    replaced = ("'Metric' 'purity' of 'Sample' 'MoHQ-CM-1-3-15000863775-19933DT': deprecated '0.26' (job 'qsee.DT' "
                "stopped 2026-11-20 10:00:00), replaced by '0.31' (job 'qsee.DT' stopped 2026-11-22 10:00:00).")
    assert replaced in ret["DB_ACTION_WARNING"]
    # An older run sent late: kept deprecated, the live value doesn't change
    late = _payload("oncoanalyser", [("qsee.DT", {DT: [("purity", "0.20")]})], stop="2026-11-01 10:00:00")
    ret = db_actions.ingest_analysis(project_id, late, not_app_db)
    assert any("kept '0.31'" in w and "is older and is stored deprecated" in w for w in ret["DB_ACTION_WARNING"])
    assert [(v, d) for v, _, d, _ in _metrics(not_app_db, "purity")] == [("0.26", True), ("0.31", False), ("0.20", True)]
    # The same payload again changes nothing
    db_actions.ingest_analysis(project_id, rerun, not_app_db)
    assert len(_metrics(not_app_db, "purity")) == 3


def test_tumour_and_normal_values_of_one_job_stay_apart(not_app_db, project_id):
    """The bug of GenPipes conpair contamination: two samples, one job, two values."""
    payload = _payload("mhedal", [
        ("conpair", {DT: [("conpair_contamination", "0.1")], DN: [("conpair_contamination", "0.2")]}),
    ])
    db_actions.ingest_analysis(project_id, payload, not_app_db)
    assert [(v, d) for v, _, d, _ in _metrics(not_app_db, "conpair_contamination")] == [("0.1", False), ("0.2", False)]


def test_group_metric_one_value_per_set_of_samples(not_app_db, project_id):
    payload = _payload("mhedal", [
        ("ngscheckmate.DT_DN", {DT: [("sample_identity", "MATCH")], DN: [("sample_identity", "MATCH")]}),
        ("ngscheckmate.DT_RT", {DT: [("sample_identity", "NO_MATCH")], RT: [("sample_identity", "NO_MATCH")]}),
    ])
    db_actions.ingest_analysis(project_id, payload, not_app_db)
    assert _metrics(not_app_db, "sample_identity") == [
        ("MATCH", "SAMPLE_GROUP", False, sorted(READSETS[DT] + READSETS[DN])),
        ("NO_MATCH", "SAMPLE_GROUP", False, sorted(READSETS[DT] + READSETS[RT])),
    ]
    # Rerun of one pair: only its value is replaced
    rerun = _payload("mhedal", [
        ("ngscheckmate.DT_RT", {DT: [("sample_identity", "MATCH")], RT: [("sample_identity", "MATCH")]}),
    ], stop="2026-11-22 10:00:00")
    db_actions.ingest_analysis(project_id, rerun, not_app_db)
    assert [(v, d) for v, _, d, _ in _metrics(not_app_db, "sample_identity")] == [
        ("MATCH", False), ("NO_MATCH", True), ("MATCH", False)]


@pytest.mark.parametrize("payload, message", [
    (_payload("oncoanalyser", [("qsee.DT", {DT: [("purity", "0.2"), ("spliced_fragment_perc", "0.6")]})]),
     "'Operation' 'oncoanalyser' has no metric ['spliced_fragment_perc'] in its metric vocabulary (GET /vocabulary): "
     "rename them or ask for them to be added."),
    (_payload("oncoanalyser", [("qsee.DN", {DN: [("purity", "0.2")]})]),
     "'Metric' 'purity' of 'Job' 'qsee.DN' applies to ['DNA_TUMOUR'], not to 'Readset' "
     "'MoHQ-CM-1-3-15000936286-19866DN.A01433_0157_2' (DNA_NORMAL)."),
    (_payload("oncoanalyser", [("isofox.DT", {DT: [("total_fragments", "1")]})]),
     "'Metric' 'total_fragments' of 'Job' 'isofox.DT' applies to ['RNA'], not to 'Readset' "
     "'MoHQ-CM-1-3-15000863775-19933DT.A01433_0157_1' (DNA_TUMOUR)."),
])
def test_refused_metrics(not_app_db, project_id, payload, message):
    with pytest.raises(RequestError) as error:
        db_actions.ingest_analysis(project_id, payload, not_app_db)
    assert message in error.value.message


def test_one_sample_two_values_in_one_payload_is_refused(not_app_db, project_id):
    payload = _payload("oncoanalyser", [("isofox.RT", {RT: [("total_fragments", "55")]})])
    payload[vb.SAMPLE][0][vb.READSET][1][vb.JOB][0][vb.METRIC][0][vb.METRIC_VALUE] = "56"
    with pytest.raises(RequestError) as error:
        db_actions.ingest_analysis(project_id, payload, not_app_db)
    assert ("'Metric' 'total_fragments' of 'Sample' 'MoHQ-CM-1-3-6929-1RT' has different values in 'Job' "
            "['isofox.RT']: ['55', '56'].") in error.value.message
    assert _metrics(not_app_db, "total_fragments") == []


def test_one_sample_two_jobs_of_one_payload(not_app_db, project_id):
    """Values of one sample from two jobs of a payload are compared too; the same value is kept once."""
    payload = _payload("oncoanalyser", [("isofox.RT", {RT: [("total_fragments", "55")]}),
                                        ("isofox_bis.RT", {RT: [("total_fragments", "56")]})])
    with pytest.raises(RequestError) as error:
        db_actions.ingest_analysis(project_id, payload, not_app_db)
    assert ("'Metric' 'total_fragments' of 'Sample' 'MoHQ-CM-1-3-6929-1RT' has different values in 'Job' "
            "['isofox.RT', 'isofox_bis.RT']: ['55', '56'].") in error.value.message
    payload = _payload("oncoanalyser", [("isofox.RT", {RT: [("total_fragments", "55")]}),
                                        ("isofox_bis.RT", {RT: [("total_fragments", "55")]})])
    db_actions.ingest_analysis(project_id, payload, not_app_db)
    assert _metrics(not_app_db, "total_fragments") == [("55", "SAMPLE", False, sorted(READSETS[RT]))]


def test_numeric_value_sent_again_changes_nothing(not_app_db, project_id):
    payload = _payload("oncoanalyser", [("qsee.DT", {DT: [("purity", 0.26)]})])
    db_actions.ingest_analysis(project_id, payload, not_app_db)
    not_app_db.expire_all()
    ret = db_actions.ingest_analysis(project_id, payload, not_app_db)
    assert not any("purity" in w for w in ret["DB_ACTION_WARNING"])
    assert _metrics(not_app_db, "purity") == [("0.26", "SAMPLE", False, READSETS[DT])]


def test_same_number_written_differently_is_the_same_value(not_app_db, project_id):
    """55000472 and 55000472.0 (or 0.6 and 0.60) are one value: kept as first sent, nothing replaced or refused."""
    payload = _payload("oncoanalyser", [("isofox.RT", {RT: [("total_fragments", "55000472"), ("spliced_fragment", "0.6")]})])
    db_actions.ingest_analysis(project_id, payload, not_app_db)
    again = _payload("oncoanalyser", [("isofox.RT", {RT: [("total_fragments", 55000472.0), ("spliced_fragment", "0.60")]})])
    again[vb.SAMPLE][0][vb.READSET][1][vb.JOB][0][vb.METRIC][1][vb.METRIC_VALUE] = "6E-1"
    ret = db_actions.ingest_analysis(project_id, again, not_app_db)
    assert not any("total_fragments" in w or "spliced_fragment" in w for w in ret["DB_ACTION_WARNING"])
    assert _metrics(not_app_db, "total_fragments") == [("55000472", "SAMPLE", False, sorted(READSETS[RT]))]
    assert _metrics(not_app_db, "spliced_fragment") == [("0.6", "SAMPLE", False, sorted(READSETS[RT]))]


def _flag(payload, flag):
    for sample_json in payload[vb.SAMPLE]:
        for readset_json in sample_json[vb.READSET]:
            for job_json in readset_json[vb.JOB]:
                for metric_json in job_json[vb.METRIC]:
                    metric_json[vb.METRIC_FLAG] = flag
    return payload


@pytest.mark.parametrize("value", ["", "-nan", "NaN"])
def test_metric_without_value_needs_a_flag_explaining_it(not_app_db, project_id, value):
    """No value is a result with MISSING, FAILED or NOT_APPLICABLE; with PASS, WARNING or no flag it isn't stored."""
    payload = _flag(_payload("oncoanalyser", [("qsee.DT", {DT: [("purity", value)]})]), "PASS")
    ret = db_actions.ingest_analysis(project_id, payload, not_app_db)
    assert any("'Metric' 'purity' of 'Job' 'qsee.DT' has no value" in w and "not stored" in w
               for w in ret["DB_ACTION_WARNING"])
    assert _metrics(not_app_db, "purity") == []
    payload = _flag(_payload("oncoanalyser", [("qsee.DT", {DT: [("purity", value)]})]), "FAILED")
    db_actions.ingest_analysis(project_id, payload, not_app_db)
    assert _metrics(not_app_db, "purity") == [(value, "SAMPLE", False, READSETS[DT])]


def test_no_value_replacing_a_value_needs_force(not_app_db, project_id):
    db_actions.ingest_analysis(project_id, _payload("oncoanalyser", [("qsee.DT", {DT: [("purity", "0.31")]})]), not_app_db)
    rerun = _flag(_payload("oncoanalyser", [("qsee.DT", {DT: [("purity", "")]})], stop="2026-11-22 10:00:00"), "FAILED")
    with pytest.raises(RequestError) as error:
        db_actions.ingest_analysis(project_id, copy.deepcopy(rerun), not_app_db)
    assert ("'Metric' 'purity' of 'Sample' 'MoHQ-CM-1-3-15000863775-19933DT': no value ('', flag 'FAILED', job 'qsee.DT' "
            "stopped 2026-11-22 10:00:00) replaces '0.31' (job 'qsee.DT' stopped 2026-11-20 10:00:00): send it with 'force' "
            "if intended.") in error.value.message
    assert [(v, d) for v, _, d, _ in _metrics(not_app_db, "purity")] == [("0.31", False)]
    rerun[vb.FORCE] = True
    ret = db_actions.ingest_analysis(project_id, rerun, not_app_db)
    assert any(w.endswith("(force).") for w in ret["DB_ACTION_WARNING"])
    assert [(v, d) for v, _, d, _ in _metrics(not_app_db, "purity")] == [("0.31", True), ("", False)]


def test_run_processing_metric_without_value_needs_a_flag_explaining_it(not_app_db, run_processing_json):
    data = copy.deepcopy(run_processing_json)
    metrics = data[vb.SPECIMEN][0][vb.SAMPLE][0][vb.READSET][0][vb.METRIC]
    metrics[0].update({vb.METRIC_VALUE: "", vb.METRIC_FLAG: "PASS"})
    metrics[1].update({vb.METRIC_VALUE: "", vb.METRIC_FLAG: "MISSING"})
    db_actions.create_project(data[vb.PROJECT_NAME], session=not_app_db)
    project_id = db_actions.name_to_id("Project", data[vb.PROJECT_NAME], session=not_app_db)[0]
    ret = db_actions.ingest_run_processing(project_id, data, not_app_db)
    assert any(f"'Metric' '{metrics[0][vb.METRIC_NAME]}' of 'Readset'" in w and "not stored" in w
               for w in ret["DB_ACTION_WARNING"])
    readset = data[vb.SPECIMEN][0][vb.SAMPLE][0][vb.READSET][0][vb.READSET_NAME]
    assert not any(readset in readsets for *_, readsets in _metrics(not_app_db, metrics[0][vb.METRIC_NAME]))
    assert ("", "READSET", False, [readset]) in _metrics(not_app_db, metrics[1][vb.METRIC_NAME])


def test_metric_stored_per_readset_before_is_deprecated(not_app_db, project_id):
    """A metric stored per readset before its operation was in the vocabulary is replaced like any other."""
    db_actions.ingest_analysis(project_id, _payload("oncoanalyser", [("isofox.RT", {RT: [("spliced_gene_count", "1")]})]),
                               not_app_db)
    job = not_app_db.execute(select(model.Job).where(model.Job.name == "isofox.RT")).scalar_one()
    readset = not_app_db.execute(select(model.Readset).where(model.Readset.name == READSETS[RT][0])).scalar_one()
    not_app_db.add(model.Metric(name="total_fragments", value="50", job=job, readsets=[readset]))
    not_app_db.flush()
    rerun = _payload("oncoanalyser", [("isofox.RT", {RT: [("total_fragments", "55")]})], stop="2026-11-22 10:00:00")
    db_actions.ingest_analysis(project_id, rerun, not_app_db)
    assert [(v, d) for v, _, d, _ in _metrics(not_app_db, "total_fragments")] == [("50", True), ("55", False)]


def test_metric_level_must_match_the_vocabulary(not_app_db, project_id):
    payload = _payload("oncoanalyser", [("qsee.DT", {DT: [("purity", "0.2")]})])
    payload[vb.SAMPLE][0][vb.READSET][0][vb.JOB][0][vb.METRIC][0][vb.METRIC_LEVEL] = "READSET"
    with pytest.raises(RequestError) as error:
        db_actions.ingest_analysis(project_id, payload, not_app_db)
    assert "'Metric' 'purity' has 'metric_level' 'READSET', the metric vocabulary says 'SAMPLE'." in error.value.message


def test_run_processing_metrics_are_per_readset_with_aggregate(not_app_db, project_id):
    rows = not_app_db.execute(select(model.Metric).where(model.Metric.name == "raw_reads_count")).scalars().all()
    assert rows and {m.aggregate for m in rows} == {model.AggregateEnum.SUM}
    assert metric_level("run_processing", "raw_reads_count") == "READSET"


def test_vocabulary_route_lists_metrics(client):
    output = client.get("/vocabulary/").get_json()["DB_ACTION_OUTPUT"][0]
    assert output["metric_vocabulary"]["oncoanalyser"]["purity"] == {
        "level": "SAMPLE", "unit": "fraction", "applies_to": ["DNA_TUMOUR"]}


def test_invalid_vocabulary_is_refused():
    with pytest.raises(ValueError, match="run_processing metrics are per readset"):
        validate_metric_vocabulary({"run_processing": {"x": {"level": "SAMPLE"}}})
    with pytest.raises(ValueError, match="applies_to"):
        validate_metric_vocabulary({"mhedal": {"x": {"level": "SAMPLE", "applies_to": ["DNA"]}}})


def test_digest_delivery_lists_sample_metrics_once_on_the_sample(not_app_db, project_id):
    payload = _payload("oncoanalyser", [("isofox.RT", {RT: [("total_fragments", "55")]})])
    for readset_json in payload[vb.SAMPLE][0][vb.READSET]:
        readset_json[vb.JOB][0][vb.METRIC][0][vb.METRIC_DELIVERABLE] = True
    db_actions.ingest_analysis(project_id, payload, not_app_db)
    stmt = select(model.Readset.id).where(model.Readset.name.in_(READSETS[RT]))
    readset_ids = not_app_db.execute(stmt).scalars().all()
    output = db_actions.digest_delivery(project_id, {
        vb.LOCATION_ENDPOINT: "abacus", vb.EXPERIMENT_NUCLEIC_ACID_TYPE: "RNA", "readset_id": readset_ids,
    }, not_app_db)["DB_ACTION_OUTPUT"][0]
    [sample] = [s for specimen in output["specimen"] for s in specimen["sample"]]
    assert sample["metric"] == [{"name": "total_fragments", "value": "55", "aggregate": None, "flag": "PASS"}]
    assert all(m["name"] != "total_fragments" for readset in sample["readset"] for m in readset["metric"])
