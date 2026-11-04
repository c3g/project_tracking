"""
The write lock (database.write_lock) on PostgreSQL: SQLite has no advisory lock. Runs only when PT_TEST_POSTGRES is a
server URI whose user may create databases, e.g.
PT_TEST_POSTGRES=postgresql+psycopg2://pt:pt-local-only@127.0.0.1:55432/postgres (see Testing in the system notes).
"""
import json
import os
import threading
import time
import uuid

import pytest
import sqlalchemy
from sqlalchemy import text

from project_tracking import create_app
from project_tracking.database import WRITE_LOCK_KEY, Engine, init_db

SERVER = os.getenv("PT_TEST_POSTGRES")
pytestmark = pytest.mark.skipif(not SERVER, reason="PT_TEST_POSTGRES not set: the write lock needs PostgreSQL")


@pytest.fixture()
def databases():
    """Two new databases, URIs (a, b), dropped afterwards."""
    admin = sqlalchemy.create_engine(SERVER, isolation_level="AUTOCOMMIT")
    names = [f"pt_lock_{uuid.uuid4().hex[:8]}" for _ in range(2)]
    with admin.connect() as connection:
        for name in names:
            connection.execute(text(f'CREATE DATABASE "{name}"'))
    uris = [SERVER.rsplit("/", 1)[0] + f"/{name}" for name in names]
    yield uris
    sqlalchemy.orm.close_all_sessions()
    with admin.connect() as connection:
        for name in names:
            connection.execute(text(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)'))
    admin.dispose()


@pytest.fixture()
def client(databases):
    """PROJ-A in database a (also the default), PROJ-B in database b; a write waits 1 s for the lock."""
    uri_a, uri_b = databases
    app = create_app({
        "TESTING": True,
        "SECRET_KEY": "test",
        "SQLALCHEMY_DATABASE_URI": uri_a,
        "PROJECT_DATABASES": {"PROJ-A": uri_a, "PROJ-B": uri_b},
        "WRITE_LOCK_TIMEOUT": 1,
        "WRITE_LOCK_RETRY_AFTER": 5,
    })
    with app.app_context():
        init_db(uri_a)
        init_db(uri_b)
    client = app.test_client()
    for project in ("PROJ-A", "PROJ-B"):
        assert client.post(f"/admin/create_project/{project}").status_code == 200
    yield client
    for engine in [*Engine.ENGINES.values(), *Engine.LOCK_ENGINES.values()]:
        engine.dispose()


def _held(uri):
    """A connection holding uri's write lock, like psql during a manual change."""
    connection = sqlalchemy.create_engine(uri, poolclass=sqlalchemy.pool.NullPool).connect()
    connection.execute(text("SELECT pg_advisory_lock(:key)"), {"key": WRITE_LOCK_KEY})
    connection.commit()
    return connection


def _ingest(client, project, run_processing_json):
    data = dict(run_processing_json, project_name=project)
    return client.post(f"/project/{project}/ingest_run_processing", data=json.dumps(data))


def test_a_write_waits_then_gets_503_only_on_its_database(client, databases, run_processing_json):
    """While database a is locked, its writes get 503 + Retry-After and save nothing; its reads and b's writes run."""
    holder = _held(databases[0])
    try:
        response = _ingest(client, "PROJ-A", run_processing_json)
        assert response.status_code == 503
        assert response.headers["Retry-After"] == "5"
        assert "Another write to database" in response.get_json()["DB_ACTION_ERROR"]
        assert client.get("/project/PROJ-A/readsets").status_code == 200
        assert not client.get("/project/PROJ-A/readsets").get_json().get("DB_ACTION_OUTPUT")
        assert _ingest(client, "PROJ-B", run_processing_json).status_code == 200
    finally:
        holder.close()

    assert _ingest(client, "PROJ-A", run_processing_json).status_code == 200


def test_writes_to_one_database_run_one_at_a_time(client, databases, run_processing_json):
    """Two ingests started together: the second waits for the first, so neither fails or duplicates rows."""
    client.application.config["WRITE_LOCK_TIMEOUT"] = 60
    holder = _held(databases[0])
    results = []
    first = dict(run_processing_json, project_name="PROJ-A")
    second = json.loads(json.dumps(first).replace("A01433_0157", "A01433_0998"))

    def ingest(data):
        results.append(client.post("/project/PROJ-A/ingest_run_processing", data=json.dumps(data)).status_code)

    threads = [threading.Thread(target=ingest, args=(data,)) for data in (first, second)]
    for thread in threads:
        thread.start()
    holder.close()
    for thread in threads:
        thread.join()

    assert results == [200, 200]
    engine = sqlalchemy.create_engine(databases[0], poolclass=sqlalchemy.pool.NullPool)
    with engine.connect() as connection:
        samples = connection.execute(text("SELECT name, count(*) FROM sample GROUP BY name")).all()
    assert samples and all(count == 1 for _, count in samples)


def test_pool_follows_the_config_and_waiting_writes_leave_it_to_reads(client, databases, run_processing_json):
    """A one-connection pool (DB_POOL_SIZE 1, no overflow): three writes waiting for the lock don't hold it, a read gets
    it at once."""
    client.application.config.update(DB_POOL_SIZE=1, DB_MAX_OVERFLOW=0, WRITE_LOCK_TIMEOUT=3)
    for engine in [*Engine.ENGINES.values(), *Engine.LOCK_ENGINES.values()]:
        engine.dispose()
    Engine.ENGINES.clear()
    holder = _held(databases[0])
    results = []
    writers = [threading.Thread(target=lambda: results.append(_ingest(client, "PROJ-A", run_processing_json).status_code))
               for _ in range(3)]
    try:
        for writer in writers:
            writer.start()
        time.sleep(0.5)
        started = time.monotonic()
        assert client.get("/project/PROJ-A/readsets").status_code == 200
        assert time.monotonic() - started < 2
        pool = Engine.ENGINES[databases[0]].pool
        assert (pool.size(), pool._max_overflow) == (1, 0)
    finally:
        for writer in writers:
            writer.join()
        holder.close()
    assert results == [503, 503, 503]
