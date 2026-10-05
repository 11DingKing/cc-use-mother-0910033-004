"""事务投递箱（Transactional Outbox）。

通知与业务状态在**同一个数据库事务**内提交（见 ``service`` 对
``store.outbox_add`` 的调用），因此"岗位已定"与"通知已生成"要么同时发生、
要么同时不发生，不存在状态改了却漏发通知的窗口。

投递语义（标准 outbox）：

1. ``pending``：待投递；投递器在短事务内把它认领为 ``sending``。
2. 事务提交后执行真正的通道发送（发送不持有数据库锁）。
3. 发送成功 → 短事务内置 ``sent`` 并写入稳定幂等键 ``notifications_delivered``。
4. 发送失败 → 释放认领回 ``pending``，稍后重试。
5. 进程在"发送成功/置 sent"之间崩溃：重启后 ``sending`` 超过陈旧阈值会被
   重新认领并**再投一次**——本投递箱提供"至少一次"语义，幂等键
   ``notify_key`` 随发送下发，由通道/接收方去重（日志通道重复一行无副作用）。

通道可插拔：默认 ``LogTransport`` 只落日志（无外部依赖、可验证），
生产可注入短信/IM 网关实现同一协议并用 ``notify_key`` 做接收端去重。
"""
from __future__ import annotations

import json
import logging
import threading
from datetime import timedelta
from typing import Any, Callable, Protocol

from .clock import Clock, SystemClock, now_iso
from .store import Store

logger = logging.getLogger("relay.notifications")

STALE_CLAIM = timedelta(seconds=30)


class Transport(Protocol):
    """通知通道协议。"""

    name: str

    def send(self, target: str, subject: str, body: str, notify_key: str) -> None:
        """成功返回；失败抛异常，投递箱释放认领并重投。

        ``notify_key`` 为稳定幂等键，网关侧应据此去重，实现"至少一次"下的
        接收端"恰好一次"效果。
        """
        ...


class LogTransport:
    """把通知写入日志的默认通道（演示与测试用，无外部副作用）。"""

    name = "log"

    def __init__(self, sink: Callable[[str], None] | None = None) -> None:
        self._sink = sink or (lambda line: logger.info(line))

    def send(self, target: str, subject: str, body: str, notify_key: str) -> None:
        self._sink(f"[通知→{target}] {subject} | {body}")


def notify_key(kind: str, ref_type: str, ref_id: str, target: str) -> str:
    """稳定幂等键：同一引用、同一类型、同一接收方只生效一次。"""
    return f"{kind}:{ref_type}:{ref_id}:{target}"


def _render(message_kind: str, payload: dict[str, Any]) -> tuple[str, str]:
    title_map = {
        "invitation": "紧急讲解岗邀请",
        "revoked": "邀请已失效",
        "won": "替补确认成功",
        "lost": "岗位已被其他志愿者确认",
        "recovered": "原讲解员已复岗，邀请取消",
        "expired_event": "接力未完成",
    }
    subject = title_map.get(message_kind, "接力通知")
    body = payload.get("text") or json.dumps(payload, ensure_ascii=False, sort_keys=True)
    return subject, body


class OutboxRelay:
    """把投递箱里的待发消息推送到通道，单线程循环、可随进程重启恢复。"""

    def __init__(
        self,
        store: Store,
        transport: Transport | None = None,
        clock: Clock | None = None,
        *,
        batch_size: int = 32,
        on_deliver: Callable[[dict[str, Any]], None] | None = None,
    ) -> None:
        self._store = store
        self._transport = transport or LogTransport()
        self._clock = clock or SystemClock()
        self._batch_size = batch_size
        self._on_deliver = on_deliver
        self._stop = threading.Event()
        self._wake = threading.Event()

    def pump_once(self) -> int:
        """处理一轮待发消息，返回成功下发条数。"""
        stale_before = now_iso(self._clock.now() - STALE_CLAIM)
        delivered = 0
        for row in self._store.outbox_claimable(stale_before, self._batch_size):
            if self._stop.is_set():
                break
            payload = json.loads(row["payload"])
            key = notify_key(row["kind"], row["ref_type"], row["ref_id"], row["target"])
            subject, body = _render(row["kind"], payload)

            with self._store.tx() as conn:
                if self._store.outbox_is_delivered(conn, key):
                    # 已投递且已记录（陈旧认领），直接补齐 sent，不再发送。
                    self._store.outbox_complete(conn, row["id"], key, self._clock.now_iso())
                    continue
                self._store.outbox_claim(conn, row["id"], self._clock.now_iso())

            try:
                self._transport.send(row["target"], subject, body, key)
            except Exception:
                logger.exception("通知投递失败，将重试：outbox=%s", row["id"])
                with self._store.tx() as conn:
                    self._store.outbox_release(conn, row["id"])
                continue

            with self._store.tx() as conn:
                self._store.outbox_complete(conn, row["id"], key, self._clock.now_iso())
            delivered += 1
            if self._on_deliver:
                self._on_deliver({"id": row["id"], "kind": row["kind"], "target": row["target"]})
        return delivered

    # ---- 后台循环 --------------------------------------------------------------

    def wake(self) -> None:
        self._wake.set()

    def run_forever(self, idle_seconds: float = 1.0) -> None:
        """阻塞循环，直到 ``stop()``。供后台线程调用。"""
        while not self._stop.is_set():
            try:
                self.pump_once()
            except Exception:
                logger.exception("投递箱轮询异常")
            self._wake.wait(idle_seconds)
            self._wake.clear()

    def start_thread(self, idle_seconds: float = 1.0) -> threading.Thread:
        thread = threading.Thread(
            target=self.run_forever, args=(idle_seconds,), name="outbox-relay", daemon=True
        )
        thread.start()
        return thread

    def stop(self) -> None:
        self._stop.set()
        self._wake.set()
