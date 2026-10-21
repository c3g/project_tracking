"""
API documentation: the OpenAPI specification at /openapi.json and Swagger UI at /docs. Swagger UI is vendored in
project_tracking/static/swagger-ui (see its VERSION file) so no third party script runs on the API origin.
"""
from html import escape

from flask import Blueprint, current_app, jsonify, make_response, request, url_for

from ..openapi import build_spec

bp = Blueprint('docs', __name__, static_folder='../static', static_url_path='/docs/static')

# Only the vendored files may run: no inline script, no other origin
CONTENT_SECURITY_POLICY = (
    "default-src 'none'; script-src 'self'; style-src 'self' 'unsafe-inline'; img-src 'self' data:; "
    "connect-src 'self'; base-uri 'none'; form-action 'none'; frame-ancestors 'none'"
)

PAGE = """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <meta name="robots" content="noindex">
  <title>Project tracking API</title>
  <link rel="icon" type="image/png" href="{favicon}">
  <link rel="stylesheet" href="{css}">
</head>
<body>
  <div id="swagger-ui" data-spec-url="{spec}"></div>
  <script src="{bundle}"></script>
  <script src="{init}"></script>
</body>
</html>
"""


def _spec():
    """
    The specification, built once per app.
    """
    if "openapi_spec" not in current_app.extensions:
        current_app.extensions["openapi_spec"] = build_spec(current_app)
    return current_app.extensions["openapi_spec"]


@bp.route('/openapi.json', methods=['GET'])
def openapi_json():
    """
    OpenAPI specification of the API, generated from the routes
    """
    spec = dict(_spec())
    # Try it out sends requests to this server, under any path prefix the proxy adds
    spec["servers"] = [{"url": request.script_root or "/"}]
    return jsonify(spec)


@bp.route('/docs', methods=['GET'])
def swagger_ui():
    """
    Swagger UI showing the OpenAPI specification
    """
    page = PAGE.format(
        favicon=escape(url_for('docs.static', filename='swagger-ui/favicon-32x32.png')),
        css=escape(url_for('docs.static', filename='swagger-ui/swagger-ui.css')),
        spec=escape(url_for('docs.openapi_json')),
        bundle=escape(url_for('docs.static', filename='swagger-ui/swagger-ui-bundle.js')),
        init=escape(url_for('docs.static', filename='docs.js')),
    )
    response = make_response(page, 200)
    response.headers["Content-Type"] = "text/html; charset=utf-8"
    response.headers["Content-Security-Policy"] = CONTENT_SECURITY_POLICY
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["Referrer-Policy"] = "no-referrer"
    return response
