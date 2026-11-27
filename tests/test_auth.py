"""
AUTH_MODE 'oidc': the app checks a COmanage token on every request but the public ones, then asks OPA, with the input
the project_tracking policy of c3g-opa-policies expects. The issuer (signing keys) and OPA are real HTTP servers on
local ports; OPA's answers come from the decide function of each test.
"""
import os
import tempfile
import logging
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
    # A short poll interval: shutdown() waits up to one interval
    thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.05}, daemon=True)
    thread.start()
    return server, thread, f"http://127.0.0.1:{server.server_port}"


@pytest.fixture(scope="module")
def signing_key():
    return RSAKey.generate_key(2048, private=True, auto_kid=True)


# What the issuer's userinfo answers, (status, body), and the tokens it was asked about. Reset for each test.
USERINFO = {}


@pytest.fixture(autouse=True)
def userinfo_state():
    USERINFO.clear()
    USERINFO.update({"answer": (200, {"sub": "user-1", "groups": []}), "calls": []})
    return USERINFO


@pytest.fixture(scope="module")
def issuer(signing_key):
    app = Flask("issuer")

    @app.route("/.well-known/openid-configuration")
    def discovery():
        return jsonify({"jwks_uri": request.host_url + "jwks", "issuer": request.host_url.rstrip("/"),
                        "userinfo_endpoint": request.host_url + "userinfo"})

    @app.route("/jwks")
    def jwks():
        return jsonify(KeySet([signing_key]).as_dict(private=False))

    @app.route("/userinfo")
    def userinfo():
        USERINFO["calls"].append(request.headers.get("Authorization"))
        status, body = USERINFO["answer"]
        return jsonify(body), status

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
        "PROJECT_DATABASES": [project_uri],
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


def _project_label(auth_app):
    return os.path.basename(auth_app.config["PROJECT_DATABASES"][0].removeprefix("sqlite:///"))


def test_opa_decides_with_the_policy_input(auth_app, issuer, signing_key, opa):
    client = auth_app.test_client()
    token = _token(signing_key, issuer, groups=["pt-admins", "pt-my-project-read"])
    response = client.post("/admin/create_project/MY-PROJECT", headers=_bearer(token),
                           json={"database": _project_label(auth_app)})
    assert response.status_code == 200
    # Not created yet: OPA gets the database the payload names
    assert opa["inputs"][-1]["database"] == _project_label(auth_app)
    opa["inputs"].clear()

    response = client.get("/project/my-project/samples", headers=_bearer(token))

    assert response.status_code == 200
    assert opa["inputs"] == [{
        "method": "GET", "path": "/project/my-project/samples", "endpoint": "project.samples",
        "database": _project_label(auth_app),
        "project": "my-project", "groups_header": "pt-admins,pt-my-project-read", "client_id": None,
    }]


def test_opa_gets_the_database_holding_the_project_now(auth_app, issuer, signing_key, opa):
    """No routing cache: a project moved to another database is authorized against that database at once."""
    from project_tracking.database import session_scope
    from project_tracking import model
    client = auth_app.test_client()
    token = _bearer(_token(signing_key, issuer, groups=["pt-admins"]))
    default_uri, project_uri = auth_app.config["SQLALCHEMY_DATABASE_URI"], auth_app.config["PROJECT_DATABASES"][0]
    with session_scope(no_app=True, db_uri=project_uri) as session:
        session.add(model.Project(name="MOVING"))
    assert client.get("/project/MOVING/samples", headers=token).status_code == 200
    assert opa["inputs"][-1]["database"] == _project_label(auth_app)

    with session_scope(no_app=True, db_uri=project_uri) as session:
        session.query(model.Project).filter_by(name="MOVING").delete()
    with session_scope(no_app=True, db_uri=default_uri) as session:
        session.add(model.Project(name="MOVING"))
    assert client.get("/project/MOVING/samples", headers=token).status_code == 200
    assert opa["inputs"][-1]["database"] == os.path.basename(default_uri.removeprefix("sqlite:///"))


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
    assert USERINFO["calls"] == []


def test_groups_from_userinfo_when_the_token_has_none(auth_app, issuer, signing_key, opa, userinfo_state):
    """CILogon's access tokens carry no groups: they come from userinfo, once per token."""
    client = auth_app.test_client()
    token = _token(signing_key, issuer)
    userinfo_state["answer"] = (200, {"sub": "user-1", "groups": ["CO:members:all", "pt-my-project-read"]})

    assert client.get("/vocabulary/", headers=_bearer(token)).status_code == 200
    assert client.get("/vocabulary/", headers=_bearer(token)).status_code == 200

    assert opa["inputs"][-1]["groups_header"] == "CO:members:all,pt-my-project-read"
    assert userinfo_state["calls"] == [f"Bearer {token}"]


def test_access_log_says_who(auth_app, issuer, signing_key, opa, userinfo_state, caplog):
    """The eppn (else email) from userinfo, else the token's sub; '-' without an accepted token."""
    client = auth_app.test_client()
    userinfo_state["answer"] = (200, {"sub": "user-1", "groups": ["pt-admins"], "eppn": "jdoe@mcgill.ca"})

    with caplog.at_level(logging.INFO, logger="app.access"):
        client.get("/vocabulary/", headers=_bearer(_token(signing_key, issuer)))
        client.get("/vocabulary/")
        client.get("/vocabulary/", headers=_bearer(_token(signing_key, issuer, groups=["pt-admins"], jti="2")))
    # The access lines, not the line telling why the request without token was refused
    lines = [r.getMessage() for r in caplog.records if r.name == "app.access" and "[source_file=" in r.getMessage()]

    assert "user=jdoe@mcgill.ca client=" in lines[0]
    assert "user=- client=-" in lines[1]
    assert "user=user-1 client=" in lines[2]


def test_userinfo_refusal_is_401_and_outage_503(auth_app, issuer, signing_key, opa, userinfo_state):
    client = auth_app.test_client()

    userinfo_state["answer"] = (401, {"error": "invalid_token"})
    assert client.get("/vocabulary/", headers=_bearer(_token(signing_key, issuer))).status_code == 401
    userinfo_state["answer"] = (200, {"sub": "someone-else", "groups": ["pt-admins"]})
    assert client.get("/vocabulary/", headers=_bearer(_token(signing_key, issuer, jti="2"))).status_code == 401
    userinfo_state["answer"] = (502, {"error": "bad_gateway"})
    response = client.get("/vocabulary/", headers=_bearer(_token(signing_key, issuer, jti="3")))
    assert response.status_code == 503 and response.get_json()["DB_ACTION_ERROR"]
    assert opa["inputs"] == []


def test_group_names_with_a_comma_are_ignored(auth_app, issuer, signing_key, opa):
    token = _token(signing_key, issuer, groups=["x,pt-admins", "pt-my-project-read"])

    auth_app.test_client().get("/vocabulary/", headers=_bearer(token))

    assert opa["inputs"][-1]["groups_header"] == "pt-my-project-read"


def test_public_routes_need_no_token_and_no_opa(auth_app, opa):
    client = auth_app.test_client()
    public = {
        rule.rule for rule in auth_app.url_map.iter_rules()
        if client.open(rule.rule.replace("<path:filename>", "swagger-ui.css").replace("<int:project_id>", "1"),
                       method="POST" if "POST" in rule.methods else "GET").status_code != 401
    }

    assert public == {"/docs", "/docs/static/<path:filename>", "/openapi.json", "/version/", "/health",
                      "/auth/device_authorization", "/auth/token"}
    assert opa["inputs"] == []


def test_browsers_without_token_go_from_the_welcome_page_to_the_docs(auth_app, opa):
    client = auth_app.test_client()

    response = client.get("/", headers={"Accept": "text/html,application/xhtml+xml,*/*;q=0.8"})
    assert response.status_code == 302 and response.headers["Location"].endswith("/docs")
    assert client.get("/").status_code == 401
    assert client.get("/vocabulary/", headers={"Accept": "text/html"}).status_code == 401
    assert opa["inputs"] == []


def test_unknown_route_or_method_is_404_or_405_with_or_without_token(auth_app, issuer, signing_key, opa):
    client = auth_app.test_client()
    token = _bearer(_token(signing_key, issuer, groups=["pt-admins"]))

    for headers in ({}, token):
        # A wrong --url-root, e.g. https://host/h: not a login problem
        assert client.get("/h/project/MY-PROJECT/samples", headers=headers).status_code == 404, headers
        assert client.post("/h/auth/device_authorization", headers=headers).status_code == 404, headers
        assert client.delete("/vocabulary/", headers=headers).status_code == 405, headers
    assert opa["inputs"] == []


def test_login_routes_only_forward_device_code_and_refresh(auth_app):
    response = auth_app.test_client().post("/auth/token", data={"grant_type": "client_credentials"})

    assert response.status_code == 400 and response.get_json()["error"] == "unsupported_grant_type"


def test_project_list_only_from_the_databases_the_caller_reads(auth_app, issuer, signing_key, opa):
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
    rows = client.get("/project/", headers=client_headers).get_json()["DB_ACTION_OUTPUT"]
    assert [(row["name"], row["_database"]) for row in rows] == [("BASE-PROJECT", default_label)]


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
