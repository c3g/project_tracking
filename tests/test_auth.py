"""
AUTH_MODE 'oidc': the app checks a COmanage token on every request but the public ones, then asks OPA, with the input
the project_tracking policy of c3g-opa-policies expects. The issuer (signing keys) and OPA are real HTTP servers on
local ports; OPA's answers come from the decide function of each test.
"""
import os
import tempfile
import threading
import time

import pytest
from flask import Flask, jsonify, request
from werkzeug.serving import make_server

pytest.importorskip("c3g_oidc.server", reason="the 'auth' extra (c3g-oidc, c3g-flask-opa) isn't installed")
from joserfc import jwt as joserfc_jwt  # noqa: E402
from joserfc.jwk import KeySet, RSAKey  # noqa: E402

from project_tracking import create_app  # noqa: E402
from project_tracking.database import init_db  # noqa: E402

CLIENT_ID = "cilogon:oa4mp,2012:/client_id/project-tracking"
ROBOT_ID = "cilogon:oa4mp,2012:/client_id/robot"


def _serve(app):
    server = make_server("127.0.0.1", 0, app)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, thread, f"http://127.0.0.1:{server.server_port}"


@pytest.fixture(scope="module")
def signing_key():
    return RSAKey.generate_key(2048, private=True, auto_kid=True)


@pytest.fixture(scope="module")
def issuer(signing_key):
    app = Flask("issuer")

    @app.route("/.well-known/openid-configuration")
    def discovery():
        return jsonify({"jwks_uri": request.host_url + "jwks", "issuer": request.host_url.rstrip("/")})

    @app.route("/jwks")
    def jwks():
        return jsonify(KeySet([signing_key]).as_dict(private=False))

    server, thread, url = _serve(app)
    yield url
    server.shutdown()
    thread.join(timeout=5)


@pytest.fixture
def opa():
    """A mock OPA: state['decide'](input) answers allow, state['readable'] is readable_databases, state['inputs']
    records the inputs."""
    app = Flask("opa")
    state = {"inputs": [], "decide": lambda input_: True, "readable": []}

    @app.post("/v1/data/project_tracking/authz/allow")
    def allow():
        input_ = request.get_json()["input"]
        state["inputs"].append(input_)
        return jsonify({"result": state["decide"](input_)})

    @app.post("/v1/data/project_tracking/authz/readable_databases")
    def readable():
        return jsonify({"result": state["readable"]})

    @app.get("/health")
    def health():
        return jsonify({})

    server, thread, url = _serve(app)
    state["url"] = url
    yield state
    server.shutdown()
    thread.join(timeout=5)


@pytest.fixture
def auth_app(issuer, opa):
    default_fd, default_path = tempfile.mkstemp()
    project_fd, project_path = tempfile.mkstemp()
    default_uri, project_uri = f"sqlite:///{default_path}", f"sqlite:///{project_path}"
    app = create_app({
        "TESTING": True,
        "SECRET_KEY": "test",
        "SQLALCHEMY_DATABASE_URI": default_uri,
        "PROJECT_DATABASES": {"MY-PROJECT": project_uri},
        "AUTH_MODE": "oidc",
        "OIDC_ISSUER": issuer,
        "OIDC_CLIENT_ID": CLIENT_ID,
        "OIDC_CLIENT_SECRET": "secret",
        "OIDC_AUDIENCES": [ROBOT_ID],
        "OPA_URL": opa["url"],
    })
    with app.app_context():
        init_db(default_uri)
        init_db(project_uri)
    yield app
    for fd, path in ((default_fd, default_path), (project_fd, project_path)):
        os.close(fd)
        os.unlink(path)


def _token(key, issuer, aud=CLIENT_ID, groups=None, exp_delta=300, **claims):
    now = int(time.time())
    payload = {"iss": issuer, "aud": aud, "sub": "user-1", "iat": now, "exp": now + exp_delta, **claims}
    if groups is not None:
        payload["groups"] = groups
    return joserfc_jwt.encode({"alg": "RS256", "kid": key.kid}, payload, key)


def _bearer(token):
    return {"Authorization": f"Bearer {token}"}


def test_a_route_needs_a_valid_token(auth_app, issuer, signing_key, opa):
    client = auth_app.test_client()
    other_key = RSAKey.generate_key(2048, private=True, auto_kid=True)

    for headers in ({}, {"Authorization": "Basic abc"}, _bearer("not-a-jwt"),
                    _bearer(_token(signing_key, issuer, exp_delta=-3600)),
                    _bearer(_token(signing_key, issuer, aud="another-app")),
                    _bearer(_token(other_key, issuer))):
        response = client.get("/vocabulary/", headers=headers)
        assert response.status_code == 401, headers
        assert response.get_json()["DB_ACTION_ERROR"] and response.headers["WWW-Authenticate"].startswith("Bearer")
    assert opa["inputs"] == []


def test_opa_decides_with_the_policy_input(auth_app, issuer, signing_key, opa):
    client = auth_app.test_client()
    token = _token(signing_key, issuer, groups=["pt-admins", "pt-my-project-read"])
    assert client.post("/admin/create_project/MY-PROJECT", headers=_bearer(token)).status_code == 200
    opa["inputs"].clear()

    response = client.get("/project/my-project/samples", headers=_bearer(token))

    assert response.status_code == 200
    assert opa["inputs"] == [{
        "method": "GET", "path": "/project/my-project/samples", "endpoint": "project.samples",
        "database": os.path.basename(auth_app.config["PROJECT_DATABASES"]["MY-PROJECT"].removeprefix("sqlite:///")),
        "project": "my-project", "groups_header": "pt-admins,pt-my-project-read", "client_id": None,
    }]


def test_opa_refusal_is_a_403_in_the_envelope(auth_app, issuer, signing_key, opa):
    opa["decide"] = lambda input_: input_["endpoint"] != "admin_api.create_project"
    client = auth_app.test_client()
    token = _bearer(_token(signing_key, issuer, groups=["pt-my-project-read"]))

    response = client.post("/admin/create_project/MY-PROJECT", headers=token)

    assert response.status_code == 403
    assert "'admin_api.create_project'" in response.get_json()["DB_ACTION_ERROR"][0]
    assert client.get("/vocabulary/", headers=token).status_code == 200


def test_opa_unreachable_refuses(auth_app, issuer, signing_key, opa):
    client = auth_app.test_client()
    token = _bearer(_token(signing_key, issuer, groups=["pt-admins"]))
    opa["decide"] = lambda input_: (_ for _ in ()).throw(RuntimeError("OPA down"))

    assert client.get("/vocabulary/", headers=token).status_code == 403


def test_robot_token_reaches_opa_with_its_client(auth_app, issuer, signing_key, opa):
    client = auth_app.test_client()
    token = _token(signing_key, issuer, aud=ROBOT_ID, sub=ROBOT_ID, client_id=ROBOT_ID)

    assert client.get("/vocabulary/", headers=_bearer(token)).status_code == 200
    assert opa["inputs"][-1]["client_id"] == ROBOT_ID and opa["inputs"][-1]["groups_header"] == ""


def test_public_routes_need_no_token_and_no_opa(auth_app, opa):
    client = auth_app.test_client()
    public = {
        rule.rule for rule in auth_app.url_map.iter_rules()
        if client.open(rule.rule.replace("<path:filename>", "swagger-ui.css"),
                       method="POST" if "POST" in rule.methods else "GET").status_code != 401
    }

    assert public == {"/docs", "/docs/static/<path:filename>", "/openapi.json", "/version/", "/health",
                      "/auth/device_authorization", "/auth/token"}
    assert opa["inputs"] == []


def test_login_routes_only_forward_device_code_and_refresh(auth_app):
    response = auth_app.test_client().post("/auth/token", data={"grant_type": "client_credentials"})

    assert response.status_code == 400 and response.get_json()["error"] == "unsupported_grant_type"


def test_project_list_only_for_readers_of_the_default_database(auth_app, issuer, signing_key, opa):
    client = auth_app.test_client()
    client_headers = _bearer(_token(signing_key, issuer, groups=["pt-my-project-read"]))
    with auth_app.app_context():
        from project_tracking import db_actions
        from project_tracking.database import session_scope
        with session_scope() as session:
            db_actions.create_project("BASE-PROJECT", session=session)
    default_label = os.path.basename(auth_app.config["SQLALCHEMY_DATABASE_URI"].removeprefix("sqlite:///"))

    opa["readable"] = []
    assert "DB_ACTION_OUTPUT" not in client.get("/project/", headers=client_headers).get_json()
    opa["readable"] = [default_label]
    assert client.get("/project/", headers=client_headers).get_json()["DB_ACTION_OUTPUT"][0]["name"] == "BASE-PROJECT"


def test_health_reports_opa(auth_app, opa):
    client = auth_app.test_client()
    assert client.get("/health").get_json() == {"status": "ok"}
    auth_app.config["OPA_URL"] = "http://127.0.0.1:1"
    response = client.get("/health")
    assert response.status_code == 503 and response.get_json() == {"status": "unavailable"}


def test_oidc_mode_needs_its_settings():
    with pytest.raises(RuntimeError, match="OIDC_CLIENT_SECRET"):
        create_app({"TESTING": True, "SECRET_KEY": "x", "SQLALCHEMY_DATABASE_URI": "sqlite://",
                    "AUTH_MODE": "oidc", "OIDC_ISSUER": "https://issuer.example", "OIDC_CLIENT_ID": "c"})
    with pytest.raises(RuntimeError, match="AUTH_MODE must be"):
        create_app({"TESTING": True, "SECRET_KEY": "x", "SQLALCHEMY_DATABASE_URI": "sqlite://", "AUTH_MODE": "off"})
