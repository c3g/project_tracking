"""Tests for app configuration loading."""

from project_tracking import create_app


def test_loads_c3g_file_backed_env_vars(monkeypatch, tmp_path):
    """C3G_*_FILE values should populate regular C3G_* config keys."""
    uri_value = "postgresql+psycopg2://user:pw@host/default_db?client_encoding=utf8"
    mapping_value = '{"PROJ-A":"postgresql+psycopg2://user:pw@host/PRJA_DB?client_encoding=utf8","GRP":"postgresql+psycopg2://user:pw@host/GRP_DB?client_encoding=utf8"}'

    uri_file = tmp_path / "c3g_sqlalchemy_database_uri.secret"
    map_file = tmp_path / "c3g_project_databases.secret"
    uri_file.write_text(uri_value + "\n", encoding="utf-8")
    map_file.write_text(mapping_value + "\n", encoding="utf-8")

    monkeypatch.delenv("C3G_SQLALCHEMY_DATABASE_URI", raising=False)
    monkeypatch.delenv("C3G_PROJECT_DATABASES", raising=False)
    monkeypatch.setenv("C3G_SQLALCHEMY_DATABASE_URI_FILE", str(uri_file))
    monkeypatch.setenv("C3G_PROJECT_DATABASES_FILE", str(map_file))

    app = create_app(test_config={})

    assert app.config["SQLALCHEMY_DATABASE_URI"] == uri_value
    assert app.config["PROJECT_DATABASES"] == {
        "PROJ-A": "postgresql+psycopg2://user:pw@host/PRJA_DB?client_encoding=utf8",
        "GRP": "postgresql+psycopg2://user:pw@host/GRP_DB?client_encoding=utf8",
    }


def test_file_backed_env_beats_plain_env(monkeypatch, tmp_path):
    """C3G_*_FILE overrides C3G_*, e.g. the SQLite default set by the Containerfile."""
    file_uri = "postgresql+psycopg2://user:pw@host/from_file"
    image_default_uri = "sqlite:////sqlite/tracking_db.sql"

    uri_file = tmp_path / "c3g_sqlalchemy_database_uri.secret"
    uri_file.write_text(file_uri + "\n", encoding="utf-8")

    monkeypatch.setenv("C3G_SQLALCHEMY_DATABASE_URI", image_default_uri)
    monkeypatch.setenv("C3G_SQLALCHEMY_DATABASE_URI_FILE", str(uri_file))

    app = create_app(test_config={})

    assert app.config["SQLALCHEMY_DATABASE_URI"] == file_uri
