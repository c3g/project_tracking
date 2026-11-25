"""Controlled vocabulary of the metrics: what each metric describes (level), its unit and on which samples it applies."""
import json
from importlib import resources

import flask

# operation_name -> metric_name -> {level, unit, applies_to, aggregate}, see metric_vocabulary.json.
# Overridden with the METRIC_VOCABULARY config (C3G_METRIC_VOCABULARY or C3G_METRIC_VOCABULARY_FILE, as JSON).
# An analysis operation listed here accepts only its listed metrics; run_processing metrics are always per readset and
# an unlisted one is only warned about; any other operation stores its metrics without a level.
DEFAULT_METRIC_VOCABULARY = json.loads(
    resources.files(__package__).joinpath("metric_vocabulary.json").read_text(encoding="utf-8")
)

LEVELS = ("READSET", "SAMPLE", "SAMPLE_GROUP")
AGGREGATES = ("SUM", "AVERAGE", "N")
# RNA isn't split into tumour and normal: unlike a DNA pair, the difference isn't biologically relevant here
SAMPLE_KINDS = ("DNA_TUMOUR", "DNA_NORMAL", "RNA")
ENTRY_KEYS = {"level", "unit", "applies_to", "aggregate"}


def validate_metric_vocabulary(vocabulary):
    """
    Check the shape and the values of a metric vocabulary; raises ValueError listing every problem.
    """
    if not isinstance(vocabulary, dict):
        raise ValueError(f"METRIC_VOCABULARY must be a JSON object, got {type(vocabulary).__name__}.")
    problems = []
    for operation, metrics in vocabulary.items():
        if not isinstance(metrics, dict):
            problems.append(f"{operation!r} must map metric names to their description")
            continue
        for name, entry in metrics.items():
            where = f"{operation!r} {name!r}"
            if not isinstance(entry, dict):
                problems.append(f"{where} must be an object")
                continue
            if set(entry) - ENTRY_KEYS:
                problems.append(f"{where} has unknown keys {sorted(set(entry) - ENTRY_KEYS)}")
            if entry.get("level") not in LEVELS:
                problems.append(f"{where} needs a level among {list(LEVELS)}")
            elif operation == "run_processing" and entry["level"] != "READSET":
                problems.append(f"{where}: run_processing metrics are per readset")
            if entry.get("aggregate") not in (None, *AGGREGATES):
                problems.append(f"{where} has an aggregate not among {list(AGGREGATES)}")
            if entry.get("unit") is not None and not isinstance(entry["unit"], str):
                problems.append(f"{where} has a unit that isn't text")
            applies_to = entry.get("applies_to", list(SAMPLE_KINDS))
            if not isinstance(applies_to, list) or not applies_to or set(applies_to) - set(SAMPLE_KINDS):
                problems.append(f"{where} has applies_to not a list among {list(SAMPLE_KINDS)}")
    if problems:
        raise ValueError("Invalid METRIC_VOCABULARY: " + "; ".join(problems) + ".")


def metric_vocabulary():
    """
    The vocabulary in use: the app config when called within the app, the default otherwise (e.g. scripts, tests).
    """
    if flask.has_app_context():
        return flask.current_app.config.get("METRIC_VOCABULARY", DEFAULT_METRIC_VOCABULARY)
    return DEFAULT_METRIC_VOCABULARY


def sample_kind(readset):
    """DNA_TUMOUR, DNA_NORMAL or RNA: the kind of sample a readset was sequenced from."""
    nucleic_acid_type = readset.experiment.nucleic_acid_type.value
    if nucleic_acid_type != "DNA":
        return nucleic_acid_type
    return f"DNA_{'TUMOUR' if readset.sample.tumour else 'NORMAL'}"


def metric_level(operation_name, metric_name):
    """READSET, SAMPLE or SAMPLE_GROUP from the metric vocabulary; None when it doesn't list the metric."""
    return metric_vocabulary().get(operation_name, {}).get(metric_name, {}).get("level")
