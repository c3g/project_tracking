"""
Metrics of operations listed in the metric vocabulary (metrics.py): one live value per readset, sample or set of
samples (the level, from the vocabulary), whatever the job; a newer job's value deprecates the older one.
"""
from decimal import Decimal, InvalidOperation

from sqlalchemy import select

from .errors import RequestError
from .. import vocabulary as vb
from ..metrics import SAMPLE_KINDS, sample_kind
from ..model import AggregateEnum, FlagEnum, Job, Metric, Operation, Readset

READSET, SAMPLE, SAMPLE_GROUP = "READSET", "SAMPLE", "SAMPLE_GROUP"


def check_metric_names(invalid, ingest_data, vocabulary):
    """
    Add to invalid the metrics of an analysis payload its operation's metric vocabulary doesn't list, and the
    metric_level values that contradict it. Without vocabulary for the operation, metric_level is refused.
    """
    operation_name = ingest_data.get(vb.OPERATION_NAME)
    metrics = vocabulary.get(operation_name)
    unknown, wrong_level = set(), set()
    for sample_json in ingest_data[vb.SAMPLE]:
        for readset_json in sample_json[vb.READSET]:
            for job_json in readset_json[vb.JOB]:
                for metric_json in job_json.get(vb.METRIC) or []:
                    name, level = metric_json[vb.METRIC_NAME], metric_json.get(vb.METRIC_LEVEL)
                    if metrics is None:
                        if level:
                            wrong_level.add(f"'{vb.METRIC_LEVEL}' of 'Metric' '{name}' needs 'Operation' "
                                            f"'{operation_name}' in the metric vocabulary (GET /vocabulary).")
                    elif name not in metrics:
                        unknown.add(name)
                    elif level and level != metrics[name]["level"]:
                        wrong_level.add(f"'Metric' '{name}' has '{vb.METRIC_LEVEL}' '{level}', the metric "
                                        f"vocabulary says '{metrics[name]['level']}'.")
    if unknown:
        invalid.append(f"'Operation' '{operation_name}' has no metric {sorted(unknown)} in its metric vocabulary "
                       f"(GET /vocabulary): rename them or ask for them to be added.")
    invalid.extend(sorted(wrong_level))


def store_metrics(pending, metrics, operation_name, session, force=False):
    """
    Store the metrics of a payload, pending: {(job, metric name): [(readset, metric_json)]}, metrics: the operation's
    metric vocabulary. Checks everything first (sample kinds, one value per readset, sample or set of samples across
    all the payload's jobs, no value replacing a value unless force) and raises RequestError listing every problem;
    returns the warnings.
    """
    problems, warnings = [], []
    # (name, subject) -> {job: [(readset, metric_json)]}; a subject is a readset id, a sample id or a set of sample ids
    subjects = {}
    for (job, name), entries in pending.items():
        skipped = [m for _, m in entries if not has_value(m) and not explains_no_value(m)]
        if skipped:
            warnings.append(f"'Metric' '{name}' of 'Job' '{job.name}' has no value ('{skipped[0][vb.METRIC_VALUE]}') "
                            f"and flag '{skipped[0].get(vb.METRIC_FLAG)}': not stored (only {sorted(NO_VALUE_FLAGS)} "
                            f"explain a missing value).")
            entries = [(r, m) for r, m in entries if has_value(m) or explains_no_value(m)]
            if not entries:
                continue
        level = metrics[name]["level"]
        applies_to = metrics[name].get("applies_to", list(SAMPLE_KINDS))
        if level == SAMPLE_GROUP:
            # A group is the samples of the job's readsets: no declared sample group needed (e.g. between cases)
            subjects.setdefault((name, frozenset(r.sample_id for r, _ in entries)), {})[job] = entries
            if not any(sample_kind(readset) in applies_to for readset, _ in entries):
                problems.append(f"'Metric' '{name}' of 'Job' '{job.name}' applies to {applies_to}, not to its "
                                f"samples {sorted({readset.sample.name for readset, _ in entries})}.")
            continue
        for readset, metric_json in entries:
            if sample_kind(readset) not in applies_to:
                problems.append(f"'Metric' '{name}' of 'Job' '{job.name}' applies to {applies_to}, not to "
                                f"'Readset' '{readset.name}' ({sample_kind(readset)}).")
            key = readset.id if level == READSET else readset.sample_id
            subjects.setdefault((name, key), {}).setdefault(job, []).append((readset, metric_json))

    work = []
    for (name, _), by_job in subjects.items():
        level = metrics[name]["level"]
        entries = [entry for job_entries in by_job.values() for entry in job_entries]
        values = {_comparable(metric_json[vb.METRIC_VALUE]): str(metric_json[vb.METRIC_VALUE]) for _, metric_json in entries}
        if len(values) > 1:
            values = sorted(values.values())
            problems.append(f"'Metric' '{name}' of {_subject(level, [r for r, _ in entries])} has different values "
                            f"in 'Job' {sorted(job.name for job in by_job)}: {values}.")
            continue
        # The same value from several jobs of one payload: kept once, on the newest job
        job = max(by_job, key=lambda j: j.stop)
        readsets = list(dict.fromkeys(readset for readset, _ in entries))
        live = _live(session, operation_name, name, level, readsets)
        metric_json = entries[0][1]
        replaced = [m for m in live if has_value({vb.METRIC_VALUE: m.value}) and _newer(job, m.job)]
        if not has_value(metric_json) and replaced:
            old = replaced[0]
            message = (f"'Metric' '{name}' of {_subject(level, readsets)}: no value "
                       f"('{metric_json[vb.METRIC_VALUE]}', flag '{metric_json.get(vb.METRIC_FLAG)}', {_job(job)}) "
                       f"replaces '{old.value}' ({_job(old.job)})")
            if not force:
                problems.append(f"{message}: send it with 'force' if intended.")
                continue
            warnings.append(f"{message} (force).")
        work.append((job, name, level, metrics[name], entries, readsets, live))
    if problems:
        raise RequestError(message=sorted(set(problems)))
    for job, name, level, entry, entries, readsets, live in work:
        warning = _store(session, job, name, level, entry, entries, readsets, live)
        if warning:
            warnings.append(warning)
    return warnings


NO_VALUE_FLAGS = {"MISSING", "FAILED", "NOT_APPLICABLE"}


def has_value(metric_json):
    """False for a missing, empty or NaN ('-nan', 'NaN') value."""
    value = metric_json.get(vb.METRIC_VALUE)
    text = "" if value is None else str(value).strip()
    return text != "" and text.lower().lstrip("+-") != "nan"


def explains_no_value(metric_json):
    """A metric without value is a result only when its flag says why: MISSING, FAILED or NOT_APPLICABLE."""
    return metric_json.get(vb.METRIC_FLAG) in NO_VALUE_FLAGS


def _subject(level, readsets):
    if level == READSET:
        return f"'Readset' '{readsets[0].name}'"
    names = sorted({readset.sample.name for readset in readsets})
    return f"'Sample' '{names[0]}'" if level == SAMPLE else f"'Samples' {names}"


def _comparable(value):
    """A number as a Decimal, so 55000472 and 55000472.0 (or 0.1 and 0.10) are the same value; other values as text."""
    try:
        number = Decimal(str(value).strip())
    except InvalidOperation:
        return str(value)
    return number if number.is_finite() else str(value)


def _newer(job, other):
    """
    job (with metrics, so COMPLETED with a stop time: check_analysis_payload) is newer than other; a stored job
    without stop time, from before that check, counts as older.
    """
    return other.stop is None or job.stop >= other.stop


def _job(job):
    return f"job '{job.name}' stopped {job.stop or 'at an unknown time'}"


def _live(session, operation_name, name, level, readsets):
    """
    The live metrics named name of operation_name for these readsets' readset, sample or exact set of samples,
    including ones stored before the vocabulary listed the operation.
    """
    stmt = (
        select(Metric).distinct()
        .join(Metric.job).join(Job.operation).join(Metric.readsets)
        .where(
            Metric.name == name,
            Metric.deprecated.is_(False),
            Metric.deleted.is_(False),
            Operation.name == operation_name,
        )
    )
    sample_ids = {readset.sample_id for readset in readsets}
    if level == READSET:
        stmt = stmt.where(Readset.id == readsets[0].id)
    else:
        stmt = stmt.where(Readset.sample_id.in_(sample_ids))
    live = session.execute(stmt).scalars().all()
    if level == SAMPLE_GROUP:
        live = [m for m in live if {r.sample_id for r in m.readsets} == sample_ids]
    return live


def _store(session, job, name, level, entry, entries, readsets, live):
    metric_json = entries[0][1]
    # Metric.value is text: compare as text, or a number sent again would look like a new value
    value = str(metric_json[vb.METRIC_VALUE])
    subject = _subject(level, readsets)

    for metric in live:
        if metric.job_id == job.id and _comparable(metric.value) == _comparable(value):
            metric.readsets.extend(r for r in readsets if r not in metric.readsets)
            return None

    metric = Metric(
        name=name,
        value=value,
        flag=FlagEnum(metric_json[vb.METRIC_FLAG]) if metric_json.get(vb.METRIC_FLAG) else None,
        deliverable=metric_json.get(vb.METRIC_DELIVERABLE, False),
        aggregate=AggregateEnum(entry["aggregate"]) if entry.get("aggregate") else None,
        job=job,
        readsets=readsets,
    )
    session.add(metric)
    newer_live = [m for m in live if not _newer(job, m.job)]
    if newer_live:
        metric.deprecated = True
        kept = newer_live[0]
        return (f"'Metric' '{name}' of {subject}: kept '{kept.value}' ({_job(kept.job)}); '{value}' ({_job(job)}) "
                f"is older and is stored deprecated.")
    for old in live:
        old.deprecated = True
    if live:
        return (f"'Metric' '{name}' of {subject}: deprecated '{live[0].value}' ({_job(live[0].job)}), replaced by "
                f"'{value}' ({_job(job)}).")
    return None
