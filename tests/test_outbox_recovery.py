"""事务投递箱测试：同事务落库、失败重试、僵死回收、重启补投。"""
from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from relay_service import (
    FakeClock, OutboxDispatcher, RecordingNotifier, RelayConfig, RelayEngine,
    RelayService, Store,
)
from relay_service.store import OUT_PENDING, OUT_SENT


class OutboxTest(unittest.TestCase):
    def setUp(self) -> None:
        self.clock = FakeClock()
        self.store = Store(":memory:")
        self.notifier = RecordingNotifier()
        self.engine = RelayEngine(self.store, clock=self.clock)
        self.dispatcher = OutboxDispatcher(
            self.store, notifier=self.notifier, clock=self.clock, interval_seconds=0,
        )

    def _event_with_invites(self) -> str:
        self.engine.register_candidate("c1", "甲", ["A"], 100, contact="m1")
        self.engine.register_candidate("c2", "乙", ["A"], 200, contact="m2")
        view = self.engine.create_event(
            shift_label="s", original_guide_id="g0",
            event_starts_at="2026-10-05T12:00:00Z",
            deadline="2026-10-05T11:30:00Z",
            required_qualifications=["A"],
        )
        return view["event_id"]

    def test_messages_deliver_at_least_once(self) -> None:
        self._event_with_invites()
        total = self.dispatcher.drain(max_idle_passes=2)
        self.assertGreaterEqual(total, 3)  # EVENT_OPENED + 2×INVITATION
        kinds = {m["type"] for m in self.notifier.sent}
        self.assertIn("INVITATION", kinds)
        self.assertIn("EVENT_OPENED", kinds)
        self.assertTrue(all(m["status"] == OUT_SENT for m in self.engine.list_outbox()))

    def test_failure_is_retried_until_success(self) -> None:
        flaky = RecordingNotifier(fail_types={"INVITATION"})
        dispatcher = OutboxDispatcher(self.store, notifier=flaky, clock=self.clock, interval_seconds=0)
        self._event_with_invites()

        n1 = dispatcher.flush_once()
        # EVENT_OPENED 成功；邀请失败回 PENDING
        self.assertEqual(n1, 1)
        pending = self.engine.list_outbox(OUT_PENDING)
        self.assertTrue(pending)
        self.assertTrue(all(p["msg_type"] == "INVITATION" for p in pending))
        self.assertGreaterEqual(pending[0]["attempts"], 1)

        # 通道恢复后换用正常 notifier，重试成功
        ok_dispatcher = OutboxDispatcher(
            self.store, notifier=RecordingNotifier(), clock=self.clock, interval_seconds=0,
        )
        ok_dispatcher.drain(max_idle_passes=2)
        self.assertTrue(all(m["status"] == OUT_SENT for m in self.engine.list_outbox()))

    def test_stale_processing_recovered_after_restart(self) -> None:
        self._event_with_invites()
        # 模拟进程崩溃：消息被领取到 PROCESSING 但未 SENT
        claimed = self.store.claim_due(
            self.clock.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            self.clock.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ"),  # stale=now
            10,
        )
        self.assertEqual(len(claimed), 3)
        # 新 dispatcher（stale_after 默认 60s）立即回收僵死消息并投递
        self.clock.advance(61)
        n = self.dispatcher.drain(max_idle_passes=2)
        self.assertEqual(n, 3)


class RestartRecoveryTest(unittest.TestCase):
    def test_restart_recovers_open_ttl_and_pending_notifications(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db = str(Path(tmp) / "relay.sqlite3")
            clock = FakeClock()
            notifier = RecordingNotifier()

            svc = RelayService.create(
                db, clock=clock, notifier=notifier,
                config=RelayConfig(batch_size=2, invite_ttl_seconds=900),
            )
            svc.recover()
            for cid, dist in (("c1", 100), ("c2", 200), ("c3", 300)):
                svc.engine.register_candidate(cid, cid, ["A"], dist, contact=cid)
            view = svc.engine.create_event(
                shift_label="s", original_guide_id="g0",
                event_starts_at="2026-10-05T12:00:00Z",
                deadline="2026-10-05T11:30:00Z",
                required_qualifications=["A"],
            )
            eid = view["event_id"]
            svc.dispatcher.drain(max_idle_passes=2)
            delivered_before = len(notifier.sent)
            svc.store.close()

            # 超过首批 TTL 后「重启」：新进程打开同一库，start() 内 recover 确定性推进
            clock.advance(901)
            before = len(notifier.sent)
            svc2 = RelayService.create(
                db, clock=clock, notifier=notifier,
                config=RelayConfig(batch_size=2, invite_ttl_seconds=900),
            )
            svc2.recover()
            view2 = svc2.engine.get_event(eid)
            self.assertEqual(len(view2["rounds"]), 2)  # 旧批超时，新批已发
            self.assertEqual(
                [i["candidate_id"] for i in view2["rounds"][1]["invitations"]], ["c3"]
            )
            svc2.dispatcher.drain(max_idle_passes=2)
            self.assertGreater(len(notifier.sent), before)  # 新通知被补投
            self.assertGreaterEqual(delivered_before, 3)
            svc2.store.close()


if __name__ == "__main__":
    unittest.main()
