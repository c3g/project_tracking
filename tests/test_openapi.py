"""The OpenAPI specification follows the routes, and /docs only runs the vendored Swagger UI."""
import hashlib
import json
import re
from pathlib import Path

from openapi_spec_validator import validate

from project_tracking.api.project import INGEST_EXAMPLES
from project_tracking.openapi import build_spec, spec_json

ROOT = Path(__file__).resolve().parent.parent
COMMITTED_SPEC = ROOT / "docs" / "openapi.json"
SWAGGER_UI = ROOT / "project_tracking" / "static" / "swagger-ui"


def _operations(spec):
    return [(method, path, operation) for path, ops in spec["paths"].items() for method, operation in ops.items()]


def test_spec_is_valid_openapi(app):
    validate(build_spec(app))


def test_committed_spec_matches_routes(app):
    """docs/openapi.json is the generated spec; only the version may differ (the release workflow bumps it)."""
    committed = json.loads(COMMITTED_SPEC.read_text(encoding="utf-8"))
    generated = json.loads(spec_json(build_spec(app)))
    committed["info"].pop("version")
    generated["info"].pop("version")
    assert committed == generated, (
        "docs/openapi.json is out of date with the routes: run 'flask --app project_tracking openapi -o docs/openapi.json' "
        "and commit it."
    )


def test_every_route_is_documented(app):
    """A route needs a docstring, and a POST its payload: parse_json_input, or openapi_doc(requestBody=...)."""
    spec = build_spec(app)
    documented = {path for path in spec["paths"]}
    for rule in app.url_map.iter_rules():
        if rule.endpoint.endswith("static") or rule.endpoint.startswith("docs."):
            continue
        path = re.sub(r"<(?:[^:<>]+:)?([^<>]+)>", r"{\1}", rule.rule)
        assert (path.rstrip("/") or "/") in documented, rule.rule
    for method, path, operation in _operations(spec):
        assert operation["summary"], f"{method.upper()} {path} has no docstring"
        if method == "post":
            assert "requestBody" in operation, f"POST {path} doesn't describe its payload"


def test_spec_content(app):
    spec = build_spec(app)
    groups = spec["paths"]["/project/{project}/sample_groups"]["get"]
    json_parameter = next(p for p in groups["parameters"] if p["name"] == "json")
    assert json_parameter["in"] == "query"
    assert set(json_parameter["content"]["application/json"]["schema"]["properties"]) == {
        "specimen_name", "sample_group_name", "sample_name"
    }
    readset = spec["components"]["schemas"]["Readset"]["properties"]
    assert readset["state"]["enum"] == ["VALID", "ON_HOLD", "INVALID", "DELIVERED"]
    samples = spec["paths"]["/project/{project}/samples"]["get"]
    assert samples["responses"]["200"]["content"]["application/json"]["schema"]["properties"]["DB_ACTION_OUTPUT"] == {
        "type": "array", "items": {"$ref": "#/components/schemas/Sample"}
    }
    edit = spec["paths"]["/modification/edit"]["post"]["requestBody"]["content"]["application/json"]["schema"]
    assert edit["properties"]["modification"]["items"]["required"] == ["table", "id", "column", "new"]
    project = next(p for p in samples["parameters"] if p["name"] == "project")
    assert "ignoring case" in project["description"]


def test_ingest_examples_ingest(client):
    """The /docs examples of the ingest routes are valid payloads, ingested in order without error or skipped file."""
    assert client.post("admin/create_project/MY-PROJECT").status_code == 200
    for kind in ("run_processing", "transfer", "analysis", "delivery"):
        response = client.post(f"project/MY-PROJECT/ingest_{kind}", data=json.dumps(INGEST_EXAMPLES[kind]))
        result = response.get_json()
        assert response.status_code == 200 and not result.get("DB_ACTION_ERROR"), (kind, result)
        assert "skipping" not in json.dumps(result.get("DB_ACTION_WARNING")), (kind, result)
    groups = client.get("project/MY-PROJECT/sample_groups").get_json()["DB_ACTION_OUTPUT"]
    assert groups[0]["sample_name"] == ["SAMPLE-1-1DN", "SAMPLE-1-1DT"]


def test_ingest_routes_show_their_example(app):
    spec = build_spec(app)
    for kind in INGEST_EXAMPLES:
        body = spec["paths"][f"/project/{{project}}/ingest_{kind}"]["post"]["requestBody"]
        assert body["content"]["application/json"]["example"] == INGEST_EXAMPLES[kind]


def test_openapi_json_route(client):
    response = client.get("/openapi.json")
    assert response.status_code == 200
    spec = response.get_json()
    assert spec["servers"] == [{"url": "/"}]
    assert "/project/{project}/samples" in spec["paths"]


def test_docs_page_runs_only_vendored_scripts(client):
    response = client.get("/docs")
    assert response.status_code == 200
    csp = response.headers["Content-Security-Policy"]
    assert "script-src 'self';" in csp and "default-src 'none'" in csp
    page = response.get_data(as_text=True)
    scripts = re.findall(r"<script([^>]*)>(.*?)</script>", page, re.S)
    assert scripts and all('src="/docs/static/' in attributes and not body.strip() for attributes, body in scripts)
    assert "https://" not in page
    for url in re.findall(r'(?:src|href)="([^"]+)"', page):
        assert client.get(url).status_code == 200, url


def test_vendored_swagger_ui_matches_its_checksums():
    """The vendored files are the ones recorded in VERSION, unchanged."""
    recorded = dict(
        reversed(line.split()) for line in (SWAGGER_UI / "VERSION").read_text().splitlines()
        if re.match(r"^[0-9a-f]{64}\s", line)
    )
    assert set(recorded) == {"swagger-ui-bundle.js", "swagger-ui.css"}
    for name, digest in recorded.items():
        assert hashlib.sha256((SWAGGER_UI / name).read_bytes()).hexdigest() == digest, name
