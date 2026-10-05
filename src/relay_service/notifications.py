"""通知通道与事务投递箱分发器。

业务代码只在数据库事务内写入 outbox（与状态变更同一事务，保证不丢消息）；
OutboxDispatcher 在事务提交后异步投递，失败按退避重试，进程重启后
启动恢复循环继续投递 PENDING / 僵死 PROCESSING 的消息。
"""
from __future__ import annotations

import json
import logging
import threading
import time
from datetime import timedelta
from typing import Callable, Protocol

from .clock import Clock, SystemClock, to_iso
from .store import OUT_PROCESSING, OUT_SENT, Store

logger = logging.getLogger("relay_service.notifications")


class Notifier(Protocol):
    """外部通知通道（短信/App 推送/IM）。异常会触发投递重试。"""

    def send(self, target: str, channel: str, msg_type: str, payload: dict) -> None: ...


class LoggingNotifier:
    """默认实现：结构化日志，绝不抛异常以外的副作用，便于单机自证。"""

    def send(self, target: str, channel: str, msg_type: str, payload: dict) -> None:
        logger.info(
            "notify target=%s channel=%s type=%s payload=%s",
            target, channel, msg_type, json.dumps(payload, ensure_ascii=False),
        )


class RecordingNotifier:
    """测试实现：记录全部已投递消息，可按类型设置失败。"""

    def __init__(self, fail_types: set[str] | None = None) -> None:
        self.sent: list[dict] = []
        self._fail_types = fail_types or set()

    def send(self, target: str, channel: str, msg_type: str, payload: dict) -> None:
        if msg_type in self._fail_types:
            raise RuntimeError(f"通道暂时不可用：{msg_type}")
        self.sent.append({"target": target, "channel": channel, "type": msg_type, "payload": payload})


class OutboxDispatcher:
    """轮询 outbox 并投递，保证至少一次（at-least-once）。

    - 消息在业务事务内入箱，事务回滚则消息一并消失；
    - 投递成功才标记 SENT；异常则回到 PENDING 退避重试；
    - PROCESSING 超过 stale_after 视为僵死（进程崩溃），重新领取；
    - 幂等键 idem_key 随 payload 下发，接收端可去重。
    """

    def __init__(
        self,
        store: Store,
        notifier: Notifier | None = None,
        clock: Clock | None = None,
        interval_seconds: float = 0.2,
        batch_size: int = 32,
        stale_after: timedelta = timedelta(seconds=60),
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self._store = store
        self._notifier = notifier or LoggingNotifier()
        self._clock = clock or SystemClock()
        self._interval = interval_seconds
        self._batch = batch_size
        self._stale = stale_after
        self._sleep = sleep
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def flush_once(self) -> int:
        """投递一批，返回本次成功投递条数。"""
        now = self._clock.now()
        stale_iso = to_iso(now - self._stale)
        claimed = self._store.claim_due(to_iso(now), stale_iso, self._batch)
        sent = 0
        for row in claimed:
            try:
                self._notifier.send(row["target"], row["channel"], row["msg_type"], row["payload"])
            except Exception as exc:  # 通道失败：回队，稍后重试
                logger.warning("outbox 投递失败 id=%s type=%s err=%s", row["id"], row["msg_type"], exc)
                self._store.requeue(row["id"], str(exc))
            else:
                self._store.mark_sent(row["id"], to_iso(self._clock.now()))
                sent += 1
        return sent

    def drain(self, max_idle_passes: int = 25) -> int:
        """同步排空（测试/优雅关停用）：连续无消息即停止，返回总投递数。"""
        total = 0
        idle = 0
        while idle < max_idle_passes:
            n = self.flush_once()
            total += n
            idle = idle + 1 if n == 0 else 0
            if n == 0:
                self._sleep(self._interval)
        return total

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                self.flush_once()
            except Exception:
                logger.exception("outbox 分发循环异常")
            self._stop.wait(self._interval)

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, name="outbox-dispatcher", daemon=True)
        self._thread.start()

    def stop(self, drain: bool = True) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=5)
        if drain:
            self.drain()
