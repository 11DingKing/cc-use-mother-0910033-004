"""服务运行时装配：后台清扫线程、事务投递箱分发、进程重启恢复。"""
from __future__ import annotations

import logging
import threading

from .clock import Clock, SystemClock
from .engine import RelayConfig, RelayEngine
from .notifications import Notifier, OutboxDispatcher
from .store import Store

logger = logging.getLogger("relay_service")


class RelayService:
    """组合持久层、领域引擎、后台线程；所有进程级行为在此确定化。"""

    def __init__(
        self,
        store: Store,
        engine: RelayEngine,
        dispatcher: OutboxDispatcher,
    ) -> None:
        self.store = store
        self.engine = engine
        self.dispatcher = dispatcher
        self._stop = threading.Event()
        self._sweeper: threading.Thread | None = None

    @classmethod
    def create(
        cls,
        db_path: str = ":memory:",
        clock: Clock | None = None,
        notifier: Notifier | None = None,
        config: RelayConfig | None = None,
    ) -> "RelayService":
        config = config or RelayConfig()
        clock = clock or SystemClock()
        store = Store(db_path)
        engine = RelayEngine(store, clock=clock, config=config)
        dispatcher = OutboxDispatcher(store, notifier=notifier, clock=clock)
        return cls(store, engine, dispatcher)

    def recover(self) -> None:
        """进程启动恢复：按当前时间推进全部未决事件，并立即补投积压通知。"""
        self.engine.sweep_all()
        self.dispatcher.flush_once()

    def _sweep_loop(self) -> None:
        interval = self.engine.config.sweep_interval_seconds
        while not self._stop.is_set():
            try:
                self.engine.sweep_all()
            except Exception:  # 单轮失败不杀死后台线程
                logger.exception("后台清扫失败")
            self._stop.wait(interval)

    def start(self) -> "RelayService":
        self.recover()  # 重启后的确定恢复点
        self.dispatcher.start()
        self._stop.clear()
        self._sweeper = threading.Thread(target=self._sweep_loop, name="relay-sweeper", daemon=True)
        self._sweeper.start()
        return self

    def stop(self) -> None:
        self._stop.set()
        if self._sweeper:
            self._sweeper.join(timeout=5)
        self.dispatcher.stop(drain=True)
        self.store.close()
