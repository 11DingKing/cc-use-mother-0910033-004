"""事务投递箱测试。"""
from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from relay.clock import FakeClock
from relay.outbox import LogTransport, OutboxRelay, notify_key
from relay.service import RelayService
from relay.store import Store


class RecordingTransport:
    """记录下发内容、可按幂等键去重、可注入失败。"""

    name = "recording"

    def __init__(self, fail_times: int = 0) -> None:
        self.sent: list[tuple[str, str, str, str]] = []
        self._fail_times = fail_times

    def send(self, target: str, subject: str, body: str, key: str) -> None:
        if self._fail_times > 0:
            self._fail_times -= 1
            raise RuntimeError("网关临时故障")
        self.sent.append((target, subject, body, key))


class OutboxTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.clock = FakeClock("2026-10-05T09:00:00Z")
        self.store = Store(str(Path(self.tmp.name) / "relay.db"))
        self.service = RelayService(self.store, self.clock)
        self.service.upsert_candidate(
            {"id": "c1", "name": "甲", "qualifications": ["guide"], "distance_m": 100,
             "contact": "101"}
        )
        self.service.upsert_candidate(
            {"id": "c2", "name": "乙", "qualifications": ["guide"], "distance_m": 200,
             "contact": "102"}
        )

    def tearDown(self) -> None:
        self.store.close()
        self.tmp.cleanup()

    def _relay(self, transport) -> OutboxRelay:
        return OutboxRelay(self.store, transport, self.clock)

    def test_notifications_deliver_after_commit(self) -> None:
        transport = RecordingTransport()
        view = self.service.create_event(
            {
                "activity": "A",
                "starts_at": "2026-10-05T11:00:00Z",
                "deadline": "2026-10-05T10:50:00Z",
                "batch_size": 2,
                "invite_timeout_seconds": 300,
            }
        )
        # 创建即生成第一批邀请通知，尚未投递
        self.assertEqual(len(transport.sent), 0)
        pending = [r for r in self.service.list_outbox() if r["state"] == "pending"]
        self.assertEqual(len(pending), 2)
        delivered = self._relay(transport).pump_once()
        self.assertEqual(delivered, 2)
        self.assertEqual(len(transport.sent), 2)
        # 再投一次不会重复下发
        self.assertEqual(self._relay(transport).pump_once(), 0)
        self.assertEqual(len(transport.sent), 2)

    def test_failure_releases_claim_and_retries(self) -> None:
        transport = RecordingTransport(fail_times=1)
        self.service.create_event(
            {
                "activity": "A",
                "starts_at": "2026-10-05T11:00:00Z",
                "deadline": "2026-10-05T10:50:00Z",
                "batch_size": 1,
                "invite_timeout_seconds": 300,
            }
        )
        relay = self._relay(transport)
        self.assertEqual(relay.pump_once(), 0)  # 首次失败，释放认领
        row = self.service.list_outbox()[0]
        self.assertEqual(row["state"], "pending")
        self.assertGreaterEqual(row["attempts"], 1)
        self.assertEqual(relay.pump_once(), 1)  # 重试成功
        self.assertEqual(len(transport.sent), 1)

    def test_crash_between_send_and_mark_redelivers_with_stable_key(self) -> None:
        """发送成功但未置 sent 即崩溃：重启后陈旧 sending 被重投，幂等键稳定。"""
        transport1 = RecordingTransport()
        self.service.create_event(
            {
                "activity": "A",
                "starts_at": "2026-10-05T11:00:00Z",
                "deadline": "2026-10-05T10:50:00Z",
                "batch_size": 1,
                "invite_timeout_seconds": 300,
            }
        )
        relay1 = self._relay(transport1)
        # 手动制造"sending 已认领、通道已发送、未来得及置 sent"的崩溃现场：
        row = self.service.list_outbox()[0]
        with self.store.tx() as conn:
            self.store.outbox_claim(conn, row["id"], self.clock.now_iso())
        transport1.send(row["target"], "s", "b", notify_key(row["kind"], row["ref_type"], row["ref_id"], row["target"]))
        self.assertEqual(len(transport1.sent), 1)

        # 重启（新投递器）。陈旧阈值 30 秒未到：不重投；越过阈值后重投一次。
        relay2 = self._relay(RecordingTransport())
        self.assertEqual(relay2.pump_once(), 0)
        self.clock.advance(seconds=31)
        relay3 = self._relay(RecordingTransport())
        self.assertEqual(relay3.pump_once(), 1)
        # 两次下发使用同一稳定幂等键，接收方可去重
        self.assertEqual(transport1.sent[0][3], relay3._transport.sent[0][3])

    def test_win_and_lose_notifications(self) -> None:
        transport = RecordingTransport()
        view = self.service.create_event(
            {
                "activity": "A",
                "starts_at": "2026-10-05T11:00:00Z",
                "deadline": "2026-10-05T10:50:00Z",
                "batch_size": 2,
                "invite_timeout_seconds": 300,
            }
        )
        inv = view["chain"][0]["invitations"][0]["id"]
        self.service.respond(inv, "accept")
        self._relay(transport).pump_once()
        kinds = sorted(key.split(":")[0] for _, _, _, key in transport.sent)
        self.assertEqual(kinds, ["invitation", "invitation", "lost", "won", "won"])


if __name__ == "__main__":
    unittest.main()
