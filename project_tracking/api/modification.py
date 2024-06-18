import logging
import functools

from flask import Blueprint, jsonify, request, flash, redirect, json, abort

from .. import db_action
from .. import vocabulary as vc

logger = logging.getLogger(__name__)

bp = Blueprint('modification', __name__, url_prefix='/modification')

@bp.route('/edit', methods=['POST'])
def edit():
    """
    POST: json describing the edit to be made
    return:
    """
    if request.method == 'POST':
        try:
            ingest_data = request.get_json(force=True)
        except:
            flash('Data does not seems to be json')
            return redirect(request.url)

        return db_action.edit(ingest_data)

@bp.route('/delete', methods=['POST'])
def delete():
    """
    POST: json describing the delete to be made
    return:
    """
    if request.method == 'POST':
        try:
            ingest_data = request.get_json(force=True)
        except:
            flash('Data does not seems to be json')
            return redirect(request.url)

        return db_action.delete(ingest_data)

@bp.route('/undelete', methods=['POST'])
def undelete():
    """
    POST: json describing the undelete to be made
    return:
    """
    if request.method == 'POST':
        try:
            ingest_data = request.get_json(force=True)
        except:
            flash('Data does not seems to be json')
            return redirect(request.url)

        return db_action.undelete(ingest_data)

@bp.route('/deprecate', methods=['POST'])
def deprecate():
    """
    POST: json describing the deprecate to be made
    return:
    """
    if request.method == 'POST':
        try:
            ingest_data = request.get_json(force=True)
        except:
            flash('Data does not seems to be json')
            return redirect(request.url)

        return db_action.deprecate(ingest_data)

@bp.route('/undeprecate', methods=['POST'])
def undeprecate():
    """
    POST: json describing the undeprecate to be made
    return:
    """
    if request.method == 'POST':
        try:
            ingest_data = request.get_json(force=True)
        except:
            flash('Data does not seems to be json')
            return redirect(request.url)

        return db_action.undeprecate(ingest_data)

@bp.route('/curate', methods=['POST'])
def curate():
    """
    POST: json describing the curate to be made
    return:
    """
    if request.method == 'POST':
        try:
            ingest_data = request.get_json(force=True)
        except:
            flash('Data does not seems to be json')
            return redirect(request.url)

        return db_action.curate(ingest_data)
