import asyncio
import json
import re
from dataclasses import dataclass
from typing import TYPE_CHECKING
from unittest.mock import patch

from dagster import DagsterInstance
from dagster._core.workspace.context import WorkspaceProcessContext
from dagster_webserver.app import create_app_from_workspace_process_context
from starlette.applications import Starlette
from starlette.types import Message, Scope

from statsig_dagster_integration.retries import workspace_retries
from statsig_dagster_integration.storage import daemon_pools

if TYPE_CHECKING:
    from collections.abc import Iterator


@dataclass(frozen=True)
class HttpResponse:
    status: int
    body: bytes


async def http(app: Starlette, path: str, method: str = "GET", body: bytes = b"") -> HttpResponse:
    messages: list[Message] = []
    received = False

    async def receive() -> Message:
        nonlocal received
        if received:
            await asyncio.Future()
        received = True
        return {"type": "http.request", "body": body, "more_body": False}

    async def send(message: Message) -> None:
        messages.append(message)

    scope: Scope = {
        "type": "http",
        "asgi": {"version": "3.0"},
        "http_version": "1.1",
        "method": method,
        "scheme": "http",
        "path": path,
        "raw_path": path.encode(),
        "query_string": b"",
        "root_path": "",
        "headers": [(b"content-type", b"application/json"), (b"host", b"fixture")],
        "server": ("fixture", 80),
        "client": ("synthetic", 1),
    }
    await asyncio.wait_for(app(scope, receive, send), timeout=5)
    status = next(
        message["status"] for message in messages if message["type"] == "http.response.start"
    )
    response_body = b"".join(
        message.get("body", b"") for message in messages if message["type"] == "http.response.body"
    )
    return HttpResponse(status, response_body)


async def websocket(app: Starlette) -> list[Message]:
    incoming: Iterator[Message] = iter(
        [
            {"type": "websocket.connect"},
            {
                "type": "websocket.receive",
                "text": json.dumps({"type": "connection_init", "payload": {}}),
            },
            {"type": "websocket.disconnect", "code": 1000},
        ]
    )
    messages: list[Message] = []

    async def receive() -> Message:
        return next(incoming)

    async def send(message: Message) -> None:
        messages.append(message)

    scope: Scope = {
        "type": "websocket",
        "asgi": {"version": "3.0"},
        "scheme": "ws",
        "path": "/graphql",
        "query_string": b"",
        "root_path": "",
        "headers": [(b"host", b"fixture")],
        "subprotocols": ["graphql-ws"],
        "server": ("fixture", 80),
        "client": ("synthetic", 1),
    }
    await asyncio.wait_for(app(scope, receive, send), timeout=5)
    return messages


def test_actual_webserver_http_graphql_and_websocket_without_emission() -> None:
    with (
        DagsterInstance.local_temp(overrides={"telemetry": {"enabled": False}}) as instance,
        WorkspaceProcessContext(instance, None, version="1.13.25") as workspace,
        workspace_retries(),
        daemon_pools(),
        patch("dagster_webserver.app.log_workspace_stats") as telemetry,
    ):
        app = create_app_from_workspace_process_context(workspace)
        response = asyncio.run(http(app, "/server_info"))
        assert response.status == 200
        assert json.loads(response.body)["dagster_version"] == "1.13.25"
        homepage = asyncio.run(http(app, "/"))
        assert homepage.status == 200
        asset = re.search(r'(?:src|href)="([^"]+\.(?:js|css))"', homepage.body.decode())
        assert asset is not None
        assert asyncio.run(http(app, asset.group(1))).status == 200
        assert asyncio.run(http(app, "/server_info", "POST")).status == 405
        response = asyncio.run(http(app, "/graphql", "POST", b'{"query":"{ version }"}'))
        assert response.status == 200
        assert json.loads(response.body) == {"data": {"version": "1.13.25"}}
        invalid = asyncio.run(http(app, "/graphql", "POST", b'{"query":"{ missingField }"}'))
        assert invalid.status == 400
        assert "errors" in json.loads(invalid.body)
        messages = asyncio.run(websocket(app))
        assert any(
            json.loads(message["text"])["type"] == "connection_ack"
            for message in messages
            if message["type"] == "websocket.send"
        )
        telemetry.assert_called_once()
