"""
Version API
"""
import logging

from flask import Blueprint, jsonify

from .. import __version__

logger = logging.getLogger(__name__)

bp = Blueprint('version', __name__, url_prefix='/version')

@bp.route('/', methods=['GET'])
def get_version():
    """
    Get the current version of the application.
    """
    return jsonify({"version": __version__.__version__})
