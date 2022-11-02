from . import database
from sqlalchemy import select


def projects():
    engine = database.get_engine()
    return engine.execute((select([database.Project]))).fetchall()
