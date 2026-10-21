"""Shared API decorators."""

import functools
import json
from typing import Any, Callable, TypeVar, cast, overload

from flask import jsonify, request

F = TypeVar("F", bound=Callable[..., Any])


# `parse_json_input` supports two call styles:
# 1) `@parse_json_input` (function passed directly)
# 2) `@parse_json_input(expected_keys={...})` (decorator factory)
#
# `@overload` provides static type signatures for each style so Pylance can
# infer that the decorated function remains a valid callable for Flask routes.
@overload
def parse_json_input(func: F, *, expected_keys: set[str] | None = None) -> F:
    ...


@overload
def parse_json_input(func: None = None, *, expected_keys: set[str] | None = None) -> Callable[[F], F]:
    ...


def parse_json_input(func=None, *, expected_keys=None):
    """
    Decorator to parse JSON input from POST/PUT/PATCH request bodies.

    The parsed object is injected as `ingest_data` into the wrapped function.
    If no body is provided, an empty dict is used.

    Args:
        expected_keys (set, optional): If provided, reject payload keys not in this set.

    Returns:
        function: Wrapped function with `ingest_data` kwarg.
    """

    def decorator(inner_func: F) -> F:
        @functools.wraps(inner_func)
        def wrapper(*args: Any, **kwargs: Any):
            raw_body = request.get_data(cache=True, as_text=True)
            if not raw_body.strip():
                ingest_data = {}
            else:
                try:
                    ingest_data = request.get_json(force=True)
                except Exception as exc:
                    return jsonify({
                        "DB_ACTION_ERROR": [
                            "Invalid JSON",
                            str(exc),
                        ]
                    }), 400

            if ingest_data is None:
                ingest_data = {}

            if expected_keys is not None and isinstance(ingest_data, dict):
                unexpected_keys = set(ingest_data.keys()) - expected_keys
                if unexpected_keys:
                    return jsonify({
                        "DB_ACTION_ERROR": [
                            f"Unexpected keys in JSON: {', '.join(sorted(unexpected_keys))}"
                        ]
                    }), 400

            kwargs["ingest_data"] = ingest_data
            return inner_func(*args, **kwargs)

        # Read by the OpenAPI spec (project_tracking/openapi.py); functools.wraps carries it to outer decorators
        wrapper.openapi_json = {"in": "body", "keys": expected_keys}
        return cast(F, wrapper)

    if func is not None and callable(func):
        return decorator(func)
    return decorator


def parse_json_get(expected_keys=None):
    """
    Decorator to parse JSON input from the request's `json` query parameter.

    Args:
        expected_keys (set, optional): A set of expected keys in the JSON input.

    Returns:
        function: Wrapped function with `digest_data` kwarg.
    """

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            allowed_params = {"json"}
            unexpected_params = set(request.args.keys()) - allowed_params
            if unexpected_params:
                return jsonify({
                    "DB_ACTION_ERROR": [
                        f"Unexpected query parameter(s): {', '.join(unexpected_params)}. Allowed: {', '.join(allowed_params)}"
                    ]
                }), 400

            raw_json = request.args.get("json", "{}")
            try:
                digest_data = json.loads(raw_json)
            except json.JSONDecodeError as exc:
                pointer_line = " " * exc.pos + "^"
                return jsonify({
                    "DB_ACTION_ERROR": [
                        "Invalid JSON",
                        f"Error: {str(exc)}",
                        raw_json,
                        pointer_line,
                    ]
                }), 400

            if expected_keys is not None:
                unexpected_keys = set(digest_data.keys()) - expected_keys
                if unexpected_keys:
                    return jsonify({
                        "DB_ACTION_ERROR": [
                            f"Unexpected keys in JSON: {', '.join(unexpected_keys)}"
                        ]
                    }), 400

            kwargs["digest_data"] = digest_data
            return func(*args, **kwargs)

        # Read by the OpenAPI spec (project_tracking/openapi.py); functools.wraps carries it to outer decorators
        wrapper.openapi_json = {"in": "query", "keys": expected_keys}
        return wrapper

    return decorator


TEXT_RESPONSE = {"200": {"description": "Plain text", "content": {"text/plain": {"schema": {"type": "string"}}}}}


def openapi_doc(**operation):
    """
    Add OpenAPI operation fields (parameters, requestBody, responses...) to a route's generated documentation, for
    what the parse_json_* decorators don't describe. Place it right under @bp.route.
    """
    def decorator(func):
        func.openapi = {**getattr(func, "openapi", {}), **operation}
        return func

    return decorator
