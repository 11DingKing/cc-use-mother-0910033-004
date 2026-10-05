"""HTTP/JSON 接口（仅用标准库）。

路由：

- ``GET  /health``：存活检查。
- ``GET  /candidates`` / ``PUT /candidates/{id}``：候选档案（资格、距离、连续班次）。
- ``POST /events``：创建带截止时间的接力事件（立即发出第一批）。
- ``GET  /events``：事件列表。
- ``GET  /events/{id}``：实时接力链（分批邀请、状态、筛选留痕、最终安排）。
- ``GET  /events/{id}/stream``：SSE 实时推送接力链变化。
- ``POST /invitations/{id}/respond``：候选 ``accept`` / ``decline``。
- ``POST /events/{id}/recover``：原讲解员复岗。
- ``POST /tick``：手动触发一次超时/批次巡检。
- ``GET  /outbox``：投递箱内容（运营/排障可核对通知）。

所有写请求在服务层串行事务内完成；响应统一 JSON。
"""
from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import urlparse

from .service import RelayError, RelayService


class _Handler(BaseHTTPRequestHandler):
    server_version = "EmergencyRelay/0.2"
    service: RelayService  # 由 build_server 注入到类上

    # ---- 基础工具 -------------------------------------------------------------

    def log_message(self, fmt: str, *args: Any) -> None:  # 静音默认访问日志
        return

    def _send_json(self, obj: Any, status: int = 200) -> None:
        body = json.dumps(obj, ensure_ascii=False, indent=2).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _read_json(self) -> dict[str, Any]:
        length = int(self.headers.get("Content-Length") or 0)
        if length == 0:
            return {}
        raw = self.rfile.read(length)
        try:
            value = json.loads(raw.decode("utf-8"))
        except json.JSONDecodeError as exc:
            raise RelayError("invalid_json", f"请求体不是合法 JSON：{exc}") from exc
        if not isinstance(value, dict):
            raise RelayError("invalid_json", "请求体必须是 JSON 对象")
        return value

    def _handle_error(self, exc: RelayError) -> None:
        self._send_json({"error": {"code": exc.code, "message": str(exc)}}, exc.status)

    # ---- 路由 -----------------------------------------------------------------

    def do_GET(self) -> None:  # noqa: N802
        path = urlparse(self.path).path.rstrip("/") or "/"
        try:
            if path == "/health":
                self._send_json({"status": "ok"})
            elif path == "/candidates":
                self._send_json({"candidates": self.service.list_candidates()})
            elif path == "/events":
                self._send_json({"events": self.service.list_events()})
            elif path == "/outbox":
                self._send_json({"outbox": self.service.list_outbox()})
            elif path.startswith("/events/") and path.endswith("/stream"):
                event_id = path.split("/")[2]
                self._serve_sse(event_id)
            elif path.startswith("/events/"):
                self._send_json(self.service.get_event(path.split("/")[2]))
            else:
                self._send_json({"error": {"code": "not_found", "message": path}}, 404)
        except RelayError as exc:
            self._handle_error(exc)

    def do_PUT(self) -> None:  # noqa: N802
        path = urlparse(self.path).path.rstrip("/") or "/"
        try:
            data = self._read_json()
            if path.startswith("/candidates/"):
                data["id"] = path.split("/")[2]
                self._send_json(self.service.upsert_candidate(data))
            else:
                self._send_json({"error": {"code": "not_found", "message": path}}, 404)
        except RelayError as exc:
            self._handle_error(exc)

    def do_POST(self) -> None:  # noqa: N802
        path = urlparse(self.path).path.rstrip("/") or "/"
        try:
            data = self._read_json()
            if path == "/events":
                self._send_json(self.service.create_event(data), 201)
            elif path == "/tick":
                changed = self.service.tick()
                self._send_json({"advanced_events": changed})
            elif path.startswith("/invitations/") and path.endswith("/respond"):
                invitation_id = path.split("/")[2]
                self._send_json(self.service.respond(invitation_id, str(data.get("action"))))
            elif path.startswith("/events/") and path.endswith("/recover"):
                event_id = path.split("/")[2]
                self._send_json(self.service.recover(event_id, data.get("note")))
            else:
                self._send_json({"error": {"code": "not_found", "message": path}}, 404)
        except RelayError as exc:
            self._handle_error(exc)

    # ---- SSE 实时接力链 --------------------------------------------------------

    def _serve_sse(self, event_id: str) -> None:
        try:
            snapshot = self.service.get_event(event_id)
        except RelayError as exc:
            self._handle_error(exc)
            return
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream; charset=utf-8")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Connection", "keep-alive")
        self.send_header("X-Accel-Buffering", "no")
        self.end_headers()

        def write(event: str, payload: dict[str, Any]) -> bool:
            try:
                self.wfile.write(f"event: {event}\n".encode("utf-8"))
                for chunk in json.dumps(payload, ensure_ascii=False).split("\n"):
                    self.wfile.write(f"data: {chunk}\n".encode("utf-8"))
                self.wfile.write(b"\n")
                self.wfile.flush()
                return True
            except (BrokenPipeError, ConnectionResetError):
                return False

        if not write("snapshot", snapshot):
            return
        last_signature = _signature(snapshot)
        while True:
            # 由提交广播唤醒；加超时兼顾心跳，代理不会掐断连接。
            self.service.wait_change(timeout=15.0)
            try:
                current = self.service.get_event(event_id)
            except RelayError:
                return
            signature = _signature(current)
            if signature != last_signature:
                last_signature = signature
                if not write("update", current):
                    return
            else:
                if not write("ping", {"ts": self.service.now_iso()}):
                    return


def _signature(view: dict[str, Any]) -> str:
    """接力链视图的稳定签名：只有真正变化才推送 update。"""
    event = view["event"]
    inv = [
        (w["wave"], [(i["id"], i["status"]) for i in w["invitations"]])
        for w in view["chain"]
    ]
    return json.dumps(
        [event["status"], event["winner_invitation_id"], event["closed_at"], inv],
        ensure_ascii=False,
        sort_keys=True,
    )


def build_server(service: RelayService, host: str, port: int) -> ThreadingHTTPServer:
    """构造 HTTP 服务器（服务实例注入到处理器）。"""
    handler = type("BoundHandler", (_Handler,), {"service": service})
    server = ThreadingHTTPServer((host, port), handler)
    server.daemon_threads = True
    return server
