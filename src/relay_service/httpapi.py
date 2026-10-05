"""HTTP 接口（仅用标准库 http.server）。

路由：
  POST   /admin/candidates           登记/更新候选
  GET    /admin/candidates           候选名册
  POST   /events                     创建带截止时间的接力事件（自动发首批）
  GET    /events                     事件列表
  GET    /events/{id}                实时接力链与最终安排（读时惰性推进超时）
  POST   /events/{id}/restore        原讲解员复岗，取消接力
  POST   /events/{id}/sweep          手动推进（测试/排障）
  GET    /events/{id}/outbox         投递箱明细
  POST   /invitations/{id}/confirm   候选确认（首确认原子获岗）
  POST   /invitations/{id}/decline   候选拒绝
  GET    /invitations/{id}           邀请状态
  GET    /healthz                    健康检查
"""
from __future__ import annotations

import json
import re
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

from .engine import DomainError, RelayEngine
from .service import RelayService
from .store import NotFound


class _Handler(BaseHTTPRequestHandler):
    server_version = "EmergencyRelay/0.2"
    service: RelayService

    # 静默标准库的噪声日志，改用结构化单行
    def log_message(self, fmt: str, *args: Any) -> None:
        return

    # ---- 基础工具 ------------------------------------------------------

    def _json(self, obj: Any, status: int = 200) -> None:
        body = json.dumps(obj, ensure_ascii=False, indent=2).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _read_body(self) -> dict[str, Any]:
        length = int(self.headers.get("Content-Length") or 0)
        if length == 0:
            return {}
        raw = self.rfile.read(length)
        try:
            data = json.loads(raw.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise _HttpError(400, f"请求体不是合法 JSON：{exc}")
        if not isinstance(data, dict):
            raise _HttpError(400, "请求体必须是 JSON 对象")
        return data

    def _run(self, fn, status: int = 200) -> None:
        try:
            result = fn()
        except _HttpError as exc:
            self._json({"error": exc.message}, exc.status)
        except NotFound as exc:
            self._json({"error": str(exc)}, 404)
        except DomainError as exc:
            self._json({"error": str(exc)}, 409)
        except (TypeError, KeyError, ValueError) as exc:
            self._json({"error": f"参数错误：{exc}"}, 400)
        else:
            self._json(result if result is not None else {"ok": True}, status)

    @property
    def engine(self) -> RelayEngine:
        return self.service.engine

    # ---- 路由 ----------------------------------------------------------

    def do_GET(self) -> None:  # noqa: N802
        path = self.path.split("?", 1)[0].rstrip("/") or "/"

        if path == "/healthz":
            return self._run(lambda: {"status": "ok"})
        if path == "/admin/candidates":
            return self._run(lambda: {"candidates": self.engine.list_candidates()})
        if path == "/events":
            return self._run(self._list_events)
        m = re.fullmatch(r"/events/([^/]+)", path)
        if m:
            return self._run(lambda: self.engine.sweep_event(m.group(1)))
        m = re.fullmatch(r"/events/([^/]+)/outbox", path)
        if m:
            return self._run(lambda: {"messages": self.engine.list_outbox(event_id=m.group(1))})
        m = re.fullmatch(r"/invitations/([^/]+)", path)
        if m:
            return self._run(lambda: self.engine.get_invitation(m.group(1)))
        self._json({"error": f"无此路径：{path}"}, 404)

    def do_POST(self) -> None:  # noqa: N802
        path = self.path.split("?", 1)[0].rstrip("/") or "/"

        if path == "/admin/candidates":
            return self._run(self._post_candidate, 201)
        if path == "/events":
            return self._run(self._post_event, 201)
        m = re.fullmatch(r"/events/([^/]+)/restore", path)
        if m:
            return self._run(lambda: self.engine.restore_original(
                m.group(1), self._read_body().get("note", "原讲解员已复岗")))
        m = re.fullmatch(r"/events/([^/]+)/sweep", path)
        if m:
            return self._run(lambda: self.engine.sweep_event(m.group(1)))
        m = re.fullmatch(r"/invitations/([^/]+)/confirm", path)
        if m:
            return self._run(lambda: self.engine.confirm(
                m.group(1), self._read_body().get("candidate_id")))
        m = re.fullmatch(r"/invitations/([^/]+)/decline", path)
        if m:
            body = self._read_body()
            return self._run(lambda: self.engine.decline(
                m.group(1), body.get("reason", ""), body.get("candidate_id")))
        self._json({"error": f"无此路径：{path}"}, 404)

    # ---- 处理函数 ------------------------------------------------------

    def _list_events(self) -> dict[str, Any]:
        # 读列表也做一次惰性推进，保证超时/轮换在无后台线程时依旧确定发生
        self.engine.sweep_all()
        return {"events": self.engine.list_events()}

    def _post_candidate(self) -> dict[str, Any]:
        body = self._read_body()
        return self.engine.register_candidate(
            candidate_id=self._require(body, "candidate_id"),
            name=self._require(body, "name"),
            qualifications=body.get("qualifications") or self._require(body, "qualifications"),
            distance_meters=float(self._require(body, "distance_meters")),
            last_service_end=body.get("last_service_end"),
            contact=body.get("contact", ""),
        )

    def _post_event(self) -> dict[str, Any]:
        body = self._read_body()
        return self.engine.create_event(
            shift_label=self._require(body, "shift_label"),
            original_guide_id=self._require(body, "original_guide_id"),
            event_starts_at=self._require(body, "event_starts_at"),
            deadline=self._require(body, "deadline"),
            required_qualifications=body.get("required_qualifications", []),
            batch_size=body.get("batch_size"),
            invite_ttl_seconds=body.get("invite_ttl_seconds"),
            event_id=body.get("event_id"),
        )

    @staticmethod
    def _require(body: dict[str, Any], key: str) -> Any:
        if key not in body or body[key] in (None, ""):
            raise _HttpError(400, f"缺少必填字段：{key}")
        return body[key]


class _HttpError(Exception):
    def __init__(self, status: int, message: str) -> None:
        self.status = status
        self.message = message


def build_server(service: RelayService, host: str = "127.0.0.1", port: int = 8080) -> ThreadingHTTPServer:
    """构造可直接 serve_forever() 的 HTTP 服务器（测试可绑定端口 0）。"""

    class Handler(_Handler):
        pass

    Handler.service = service
    server = ThreadingHTTPServer((host, port), Handler)
    return server
