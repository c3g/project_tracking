"""
Vocabulary API: every controlled value accepted by the ingest and edit routes
"""
import enum
import logging

from flask import Blueprint, jsonify

from .. import model
from ..analysis import RESERVED_OPERATION_NAMES, analysis_operations

logger = logging.getLogger(__name__)

bp = Blueprint('vocabulary', __name__, url_prefix='/vocabulary')


def model_enums():
    """
    Values of every enum defined in the model, by enum name.
    """
    return {
        name: [member.value for member in value]
        for name, value in vars(model).items()
        if isinstance(value, type) and issubclass(value, enum.Enum) and value.__module__ == model.__name__
    }


@bp.route('/', methods=['GET'])
def vocabulary():
    """
    Get the controlled vocabularies:
        enums: values of the enum fields (e.g. readset_state, readset_lane), fixed in the code
        analysis_operations: operation_name -> operation_pipeline -> allowed operation_protocol values accepted by
            ingest_analysis, from the configuration in use
        reserved_operation_names: operation names analyses can't use
    """
    return jsonify({"DB_ACTION_OUTPUT": [{
        "enums": model_enums(),
        "analysis_operations": analysis_operations(),
        "reserved_operation_names": list(RESERVED_OPERATION_NAMES),
    }]})
