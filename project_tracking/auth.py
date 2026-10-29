"""
Authentication and authorization in the app (Tier 3b of web-app-template): every request carries a Bearer token
from COmanage (CILogon), checked here with c3g-oidc, then an OPA sidecar decides with the project_tracking policy of
c3g-opa-policies. pt-cli logs in through the device-code routes registered here (/auth/device_authorization and
/auth/token), which add the confidential client's secret: the secret never leaves the API.

AUTH_MODE turns it on:
- 'oidc': token and OPA checks. Every OIDC_* setting must be there, or the app doesn't start.
- 'none' (the default): no check in the app, e.g. behind oauth2-proxy, for local development and the tests.

Settings (C3G_ environment variables, *_FILE for secrets):
- OIDC_ISSUER: e.g. https://cilogon.org
- OIDC_CLIENT_ID, OIDC_CLIENT_SECRET: the confidential client pt-cli logs in through
- OIDC_SCOPE: requested when pt-cli asks for none (default: openid profile email org.cilogon.userinfo)
- OIDC_AUDIENCES: other accepted token audiences, e.g. each robot's client id (default: none)
- OIDC_GROUPS_CLAIM: the token claim with the groups (default: groups)
- OPA_URL: the OPA sidecar (default: http://127.0.0.1:8181)
- OPA_POLICY: the package of the policy (default: project_tracking/authz)
"""
import logging

import requests
from flask import current_app, g, jsonify, request

from . import database

logger = logging.getLogger(__name__)

DEFAULT_SCOPE = "openid profile email org.cilogon.userinfo"
DEFAULT_OPA_URL = "http://127.0.0.1:8181"
DEFAULT_OPA_POLICY = "project_tracking/authz"
REQUIRED_SETTINGS = ("OIDC_ISSUER", "OIDC_CLIENT_ID", "OIDC_CLIENT_SECRET")

# Reachable without a token: the login itself, the documentation (no data), the version and the health check.
# The policy lists the same endpoints; tests/test_auth.py checks the routes reachable without a token.
PUBLIC_ENDPOINTS = frozenset({
    "oidc_broker.device_authorization",
    "oidc_broker.token",
    "docs.openapi_json",
    "docs.static",
    "docs.swagger_ui",
    "version.get_version",
    "health",
})


def auth_mode(app):
    mode = str(app.config.get("AUTH_MODE", "none")).lower()
    if mode not in ("oidc", "none"):
        raise RuntimeError(f"AUTH_MODE must be 'oidc' or 'none', not '{mode}'.")
    return mode


def refused(status, message):
    """A refused request, in the usual envelope."""
    response = jsonify({"DB_ACTION_ERROR": [message]})
    response.status_code = status
    if status == 401:
        response.headers["WWW-Authenticate"] = 'Bearer realm="project_tracking"'
    return response


def request_database():
    """
    Label of the database the project of the request routes to, and that project: (None, None) without one.
    """
    args = request.view_args or {}
    project = args.get("project") or args.get("project_name")
    if not project:
        return None, None
    return database.db_label(database.project_db_uri(current_app.config, project)), project


def opa_input(groups_header):
    """
    The input the project_tracking policy expects (see c3g-opa-policies): the rights follow the endpoint and the
    database, never the path.
    """
    database_label, project = request_database()
    return {
        "method": request.method,
        "path": request.path,
        "endpoint": request.endpoint,
        "database": database_label,
        "project": project,
        "groups_header": groups_header,
        "client_id": g.get("client_id"),
    }


def opa_query(rule, input_):
    """
    The result of a rule of the policy, or None when OPA can't answer: callers fail closed.
    """
    url = f"{current_app.config.get('OPA_URL', DEFAULT_OPA_URL).rstrip('/')}/v1/data/" \
          f"{current_app.config.get('OPA_POLICY', DEFAULT_OPA_POLICY).strip('/')}/{rule}"
    try:
        response = requests.post(url, json={"input": input_}, timeout=2)
        response.raise_for_status()
        return response.json().get("result")
    except (requests.RequestException, ValueError) as error:
        logger.error("OPA query %s failed: %s", rule, error)
        return None


def readable_databases():
    """
    Labels of the databases the caller may read, or None when every database is readable (no check in the app).
    """
    if auth_mode(current_app) == "none":
        return None
    result = opa_query("readable_databases", opa_input(",".join(g.get("groups") or [])))
    return set(result or [])


def opa_reachable(app):
    try:
        return requests.get(f"{app.config.get('OPA_URL', DEFAULT_OPA_URL).rstrip('/')}/health", timeout=2).ok
    except requests.RequestException:
        return False


def init_app(app):
    """
    Set up the checks of AUTH_MODE 'oidc'. Call it after every blueprint is registered.
    """
    if auth_mode(app) == "none":
        logger.warning("AUTH_MODE is 'none': the API checks no token and no rights; only a proxy in front protects it.")
        return

    missing = [name for name in REQUIRED_SETTINGS if not app.config.get(name)]
    if missing:
        raise RuntimeError(f"AUTH_MODE 'oidc' needs {', '.join(missing)}.")

    # Imported here: the 'auth' extra is only needed with AUTH_MODE 'oidc'
    from c3g_flask_opa import FlaskOPA, groups_header_from_g
    from c3g_oidc.server import BearerTokenValidator, TokenValidationError, device_flow_broker

    app.register_blueprint(device_flow_broker(
        app.config["OIDC_ISSUER"],
        app.config["OIDC_CLIENT_ID"],
        app.config["OIDC_CLIENT_SECRET"],
        scope=app.config.get("OIDC_SCOPE", DEFAULT_SCOPE),
    ))
    audiences = [app.config["OIDC_CLIENT_ID"], *(app.config.get("OIDC_AUDIENCES") or [])]
    validator = BearerTokenValidator(
        app.config["OIDC_ISSUER"], audiences, groups_claim=app.config.get("OIDC_GROUPS_CLAIM", "groups")
    )

    @app.before_request
    def authenticate():
        """
        Check the Bearer token of every request but the public ones, and keep its groups and client for OPA.
        """
        if request.endpoint in PUBLIC_ENDPOINTS:
            return None
        token = request.headers.get("Authorization", "")
        if not token.startswith("Bearer "):
            return refused(401, "Not logged in: log in with 'pt-cli login'.")
        try:
            claims = validator.validate_claims(token.removeprefix("Bearer ").strip())
        except TokenValidationError as error:
            logger.info("Token refused: %s", error)
            return refused(401, "Token expired or invalid: log in again with 'pt-cli login'.")
        groups = claims.get(validator.groups_claim) or []
        g.groups = [str(group) for group in groups] if isinstance(groups, list) else []
        g.client_id = claims.get("client_id") or claims.get("azp")
        g.user = claims.get("sub")
        return None

    # Registered after authenticate: before_request hooks run in order, and OPA needs the groups
    opa = FlaskOPA(
        app,
        groups_source=groups_header_from_g(),
        input_builder=opa_input,
        policy_path=f"{app.config.get('OPA_POLICY', DEFAULT_OPA_POLICY).strip('/')}/allow",
        opa_url=app.config.get("OPA_URL", DEFAULT_OPA_URL),
    )
    for endpoint in PUBLIC_ENDPOINTS:
        if endpoint in app.view_functions:
            opa.exempt(app.view_functions[endpoint])

    @app.errorhandler(403)
    def forbidden(_error):
        database_label, project = request_database()
        where = f" on project '{project}' (database '{database_label}')" if project else ""
        return refused(403, f"Not allowed: '{request.endpoint}'{where} needs a role your groups don't give.")
