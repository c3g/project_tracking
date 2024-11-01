import logging
import functools

from flask import Blueprint, jsonify, request, flash, redirect, json, abort

from .. import db_action
from .. import vocabulary as vc
from .. import __version__

logger = logging.getLogger(__name__)

bp = Blueprint('version', __name__, url_prefix='/version')

@bp.route('/', methods=['GET'])
def get_version():
    return jsonify({"version": __version__.__version__})
