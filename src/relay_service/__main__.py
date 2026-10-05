"""启动入口：python -m relay_service --db data/relay.sqlite3 --port 8080"""
from __future__ import annotations

import argparse
import logging
import signal

from .engine import RelayConfig
from .notifications import LoggingNotifier
from .service import RelayService


def main() -> None:
    parser = argparse.ArgumentParser(description="紧急缺岗接力服务端")
    parser.add_argument("--db", default="data/relay.sqlite3", help="SQLite 数据库路径")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--batch-size", type=int, default=3, help="每批邀请人数")
    parser.add_argument("--invite-ttl", type=float, default=900, help="单批邀请响应时限（秒）")
    parser.add_argument("--sweep-interval", type=float, default=5, help="后台清扫间隔（秒）")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    config = RelayConfig(
        batch_size=args.batch_size,
        invite_ttl_seconds=args.invite_ttl,
        sweep_interval_seconds=args.sweep_interval,
    )
    service = RelayService.create(args.db, notifier=LoggingNotifier(), config=config).start()

    from .httpapi import build_server

    server = build_server(service, host=args.host, port=args.port)
    logging.getLogger("relay_service").info(
        "紧急缺岗接力服务已启动：http://%s:%s （db=%s）", args.host, args.port, args.db,
    )

    def _shutdown(*_: object) -> None:
        logging.getLogger("relay_service").info("收到退出信号，开始优雅停机…")
        server.shutdown()

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)
    try:
        server.serve_forever()
    finally:
        service.stop()
        logging.getLogger("relay_service").info("已停机")


if __name__ == "__main__":
    main()
