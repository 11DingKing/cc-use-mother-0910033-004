"""进程入口：组装存储、服务、后台巡检与投递箱。

用法：

    python -m relay.server --db data/relay.db --host 0.0.0.0 --port 8080 [--seed]

进程重启语义：所有状态都在 SQLite 中；启动时立即执行一次 ``tick()`` 与
``pump_once()``，把崩溃期间到期的邀请失效、补发下一批/失败结论，并继续投递
投递箱中未发送的消息。后台两个守护线程之后周期性兜底。
"""
from __future__ import annotations

import argparse
import logging
import signal
import threading
import time
from pathlib import Path

from .clock import SystemClock
from .httpapi import build_server
from .outbox import LogTransport, OutboxRelay
from .service import RelayService
from .store import Store

logger = logging.getLogger("relay.server")

SEED_CANDIDATES = [
    {
        "id": "v001",
        "name": "陈曦",
        "qualifications": ["guide", "first_aid"],
        "distance_m": 800,
        "consecutive_shifts": 1,
        "max_consecutive_shifts": 3,
        "contact": "13800000001",
    },
    {
        "id": "v002",
        "name": "林一帆",
        "qualifications": ["guide"],
        "distance_m": 1500,
        "consecutive_shifts": 2,
        "max_consecutive_shifts": 3,
        "contact": "13800000002",
    },
    {
        "id": "v003",
        "name": "周童（未成年，监护人代确认）",
        "qualifications": ["guide", "english"],
        "distance_m": 2200,
        "consecutive_shifts": 0,
        "max_consecutive_shifts": 3,
        "contact": "13800000003",
        "guardian_contact": "13900000003",
    },
    {
        "id": "v004",
        "name": "赵珩",
        "qualifications": ["english"],  # 缺 guide 资格，将在筛选留痕中被剔除
        "distance_m": 600,
        "consecutive_shifts": 0,
        "max_consecutive_shifts": 3,
        "contact": "13800000004",
    },
    {
        "id": "v005",
        "name": "孙岚",
        "qualifications": ["guide", "first_aid"],
        "distance_m": 900,
        "consecutive_shifts": 3,  # 已达连续服务上限，将被剔除
        "max_consecutive_shifts": 3,
        "contact": "13800000005",
    },
    {
        "id": "v006",
        "name": "吴珂",
        "qualifications": ["guide"],
        "distance_m": 6500,  # 超出距离上限时将被剔除
        "consecutive_shifts": 0,
        "max_consecutive_shifts": 3,
        "contact": "13800000006",
    },
    {
        "id": "v007",
        "name": "郑言",
        "qualifications": ["guide", "english"],
        "distance_m": 2600,
        "consecutive_shifts": 1,
        "max_consecutive_shifts": 3,
        "contact": "13800000007",
    },
]


def seed_demo(service: RelayService) -> None:
    """写入演示候选（幂等）。"""
    existing = {c["id"] for c in service.list_candidates()}
    for cand in SEED_CANDIDATES:
        if cand["id"] not in existing:
            service.upsert_candidate(cand)


def _ticker(service: RelayService, stop: threading.Event, interval: float) -> None:
    """周期巡检：超时失效、批次推进、失败收口。"""
    while not stop.wait(interval):
        try:
            service.tick()
        except Exception:
            logger.exception("巡检异常")


def run(db_path: str, host: str, port: int, *, seed: bool, tick_interval: float = 0.5) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    if db_path != ":memory:":
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    store = Store(db_path)
    service = RelayService(store, SystemClock())
    if seed:
        seed_demo(service)

    # 重启恢复：先补领域状态，再投递积压通知。
    recovered = service.tick()
    relay = OutboxRelay(store, LogTransport(), SystemClock())
    relay.pump_once()
    if recovered:
        logger.info("重启恢复：推进了 %s 个事件", recovered)

    stop = threading.Event()
    tick_thread = threading.Thread(
        target=_ticker, args=(service, stop, tick_interval), name="relay-ticker", daemon=True
    )
    outbox_thread = relay.start_thread(idle_seconds=tick_interval)
    tick_thread.start()

    httpd = build_server(service, host, port)

    def shutdown(signum, frame) -> None:  # noqa: ANN001
        logger.info("收到退出信号，正在关闭……")
        stop.set()
        relay.stop()
        httpd.shutdown()

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)
    logger.info("紧急缺岗接力服务已启动：http://%s:%s（db=%s）", host, port, db_path)
    try:
        httpd.serve_forever()
    finally:
        stop.set()
        relay.stop()
        time.sleep(0.2)
        store.close()
        logger.info("已关闭")


def main() -> None:
    parser = argparse.ArgumentParser(description="紧急缺岗接力服务端")
    parser.add_argument("--db", default="data/relay.db", help="SQLite 路径，:memory: 为内存库")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--seed", action="store_true", help="写入演示候选数据")
    parser.add_argument("--tick-interval", type=float, default=0.5)
    args = parser.parse_args()
    run(args.db, args.host, args.port, seed=args.seed, tick_interval=args.tick_interval)


if __name__ == "__main__":
    main()
