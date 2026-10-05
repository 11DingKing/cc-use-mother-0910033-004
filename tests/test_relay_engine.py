"""接力引擎领域规则测试：筛选、分批、原子获岗、拒绝/超时/复岗。"""
from __future__ import annotations

import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from relay_service import DomainError, FakeClock, RelayConfig, RelayEngine, Store
from relay_service.store import (
    EVENT_CANCELLED_RESTORED, EVENT_EXPIRED, EVENT_FILLED, EVENT_OPEN,
    INVITE_CONFIRMED, INVITE_DECLINED, INVITE_EXPIRED, INVITE_OPEN, INVITE_SUPERSEDED,
)


class EngineTest(unittest.TestCase):
    def setUp(self) -> None:
        self.clock = FakeClock()  # 2026-10-05T10:00:00Z
        self.store = Store(":memory:")
        self.engine = RelayEngine(
            self.store, clock=self.clock,
            config=RelayConfig(batch_size=2, invite_ttl_seconds=900),
        )
        self._seed()

    def _seed(self) -> None:
        e = self.engine
        e.register_candidate("c1", "甲一", ["A", "急救"], 800, contact="m1")
        e.register_candidate("c2", "乙二", ["A"], 1200, contact="m2")
        e.register_candidate("c3", "丙三", ["A", "B"], 2000, contact="m3")
        # 连续服务受限：5 分钟前刚结束一段服务
        e.register_candidate(
            "c4", "丁四", ["A"], 400,
            last_service_end="2026-10-05T09:55:00Z", contact="m4",
        )
        # 资格不符
        e.register_candidate("c5", "戊五", ["B"], 300, contact="m5")

    def _create(self, **kw):
        kwargs = dict(
            shift_label="14:00 青铜厅",
            original_guide_id="g0",
            event_starts_at="2026-10-05T12:00:00Z",
            deadline="2026-10-05T11:30:00Z",
            required_qualifications=["A"],
        )
        kwargs.update(kw)
        return self.engine.create_event(**kwargs)

    def test_filter_by_qualification_distance_service_limit(self) -> None:
        view = self._create()
        self.assertEqual(view["status"], EVENT_OPEN)
        first = view["rounds"][0]["invitations"]
        # 首批为距离最近的两名合格候选；c4 受限、c5 资格不符均不在列
        self.assertEqual([i["candidate_id"] for i in first], ["c1", "c2"])
        self.assertTrue(all(i["status"] == INVITE_OPEN for i in first))

    def test_service_limit_lifts_after_rest(self) -> None:
        self.clock.advance(31 * 60)
        view = self._create()
        first = view["rounds"][0]["invitations"]
        # c4 休息间隔已过，距离最近，回到榜首
        self.assertEqual([i["candidate_id"] for i in first], ["c4", "c1"])

    def test_deadline_must_be_future_and_before_start(self) -> None:
        with self.assertRaises(DomainError):
            self._create(deadline="2026-10-05T09:59:00Z")
        with self.assertRaises(DomainError):
            self._create(deadline="2026-10-05T12:30:00Z")

    # ---- 首确认原子获岗 -------------------------------------------------

    def test_first_confirm_wins_and_others_are_superseded(self) -> None:
        view = self._create()
        inv1, inv2 = [i["invite_id"] for i in view["rounds"][0]["invitations"]]
        # 距离次近的 c2 先确认
        won = self.engine.confirm(inv2, candidate_id="c2")
        self.assertEqual(won["status"], EVENT_FILLED)
        self.assertEqual(won["final_assignment"]["candidate_id"], "c2")
        chain = {i["candidate_id"]: i for i in won["chain"]}
        self.assertEqual(chain["c2"]["status"], INVITE_CONFIRMED)
        self.assertEqual(chain["c1"]["status"], INVITE_SUPERSEDED)

        # 失效邀请再确认必须被拒绝
        with self.assertRaises(DomainError):
            self.engine.confirm(inv1, candidate_id="c1")
        # 胜者重复确认也拒绝
        with self.assertRaises(DomainError):
            self.engine.confirm(inv2, candidate_id="c2")

    def test_confirm_with_wrong_candidate_rejected(self) -> None:
        view = self._create()
        inv1 = view["rounds"][0]["invitations"][0]["invite_id"]
        with self.assertRaises(DomainError):
            self.engine.confirm(inv1, candidate_id="c9")

    # ---- 拒绝与批次轮换 --------------------------------------------------

    def test_decline_advances_to_next_batch_when_round_closed(self) -> None:
        view = self._create()
        inv1, inv2 = [i["invite_id"] for i in view["rounds"][0]["invitations"]]
        self.engine.decline(inv1, reason="在外地", candidate_id="c1")
        # 还有一人未响应，不发新批
        self.assertEqual(len(self.engine.get_event(view["event_id"])["rounds"]), 1)
        self.engine.decline(inv2, candidate_id="c2")
        view2 = self.engine.get_event(view["event_id"])
        self.assertEqual(len(view2["rounds"]), 2)
        self.assertEqual(
            [i["candidate_id"] for i in view2["rounds"][1]["invitations"]], ["c3"]
        )
        self.assertEqual(
            [i["status"] for i in view2["rounds"][0]["invitations"]],
            [INVITE_DECLINED, INVITE_DECLINED],
        )

    def test_decline_after_filled_is_rejected(self) -> None:
        view = self._create()
        inv1, inv2 = [i["invite_id"] for i in view["rounds"][0]["invitations"]]
        self.engine.confirm(inv2, candidate_id="c2")
        with self.assertRaises(DomainError):
            self.engine.decline(inv1, candidate_id="c1")

    # ---- 超时 -----------------------------------------------------------

    def test_invite_ttl_expires_then_next_batch(self) -> None:
        view = self._create()
        self.clock.advance(901)
        view2 = self.engine.sweep_event(view["event_id"])
        self.assertEqual(
            [i["status"] for i in view2["rounds"][0]["invitations"]],
            [INVITE_EXPIRED, INVITE_EXPIRED],
        )
        self.assertEqual(len(view2["rounds"]), 2)
        self.assertEqual(
            [i["candidate_id"] for i in view2["rounds"][1]["invitations"]], ["c3"]
        )

    def test_late_confirm_after_ttl_rejected_and_chain_continues(self) -> None:
        view = self._create()
        inv1, _ = [i["invite_id"] for i in view["rounds"][0]["invitations"]]
        self.clock.advance(901)
        with self.assertRaises(DomainError):
            self.engine.confirm(inv1, candidate_id="c1")
        view2 = self.engine.get_event(view["event_id"])
        # 旧批超时，新批已发给 c3
        self.assertEqual(view2["rounds"][0]["invitations"][0]["status"], INVITE_EXPIRED)
        self.assertEqual(
            [i["candidate_id"] for i in view2["rounds"][1]["invitations"]], ["c3"]
        )

    def test_exhausted_candidates_expires_event(self) -> None:
        view = self._create()
        eid = view["event_id"]
        # 批次流转：c1,c2 → c3 → c4（强制休息结束后可派）→ 无候选 → EXPIRED
        for _ in range(3):
            self.clock.advance(901)
            self.engine.sweep_event(eid)
        view2 = self.engine.get_event(eid)
        self.assertEqual(view2["status"], EVENT_EXPIRED)
        self.assertIn("耗尽", view2["note"])

    def test_deadline_expires_open_invitations(self) -> None:
        # 截止时间很近：先到截止（而非候选耗尽），验证截止语义
        view = self._create(deadline="2026-10-05T10:10:00Z")
        self.clock.move_to("2026-10-05T10:10:00Z")
        view2 = self.engine.sweep_event(view["event_id"])
        self.assertEqual(view2["status"], EVENT_EXPIRED)
        self.assertIn("截止", view2["note"])
        self.assertTrue(all(i["status"] == INVITE_EXPIRED for i in view2["chain"]))
        # 终态后任何确认都无效
        with self.assertRaises(DomainError):
            self.engine.confirm(view2["chain"][0]["invite_id"])

    # ---- 原人员复岗 ------------------------------------------------------

    def test_original_restore_cancels_event_and_invalidates(self) -> None:
        view = self._create()
        inv1, inv2 = [i["invite_id"] for i in view["rounds"][0]["invitations"]]
        view2 = self.engine.restore_original(view["event_id"])
        self.assertEqual(view2["status"], EVENT_CANCELLED_RESTORED)
        self.assertTrue(all(i["status"] == INVITE_EXPIRED for i in view2["chain"]))
        with self.assertRaises(DomainError):
            self.engine.confirm(inv1)
        with self.assertRaises(DomainError):
            self.engine.restore_original(view["event_id"])

    # ---- 通知与状态同事务 ------------------------------------------------

    def test_notifications_written_transactionally(self) -> None:
        view = self._create()
        messages = self.engine.list_outbox()
        types = [m["msg_type"] for m in messages]
        # 事件开启 1 条 + 首批邀请 2 条，随状态同事务落库
        self.assertEqual(types.count("INVITATION"), 2)
        self.assertIn("EVENT_OPENED", types)

        inv2 = view["rounds"][0]["invitations"][1]["invite_id"]
        self.engine.confirm(inv2, candidate_id="c2")
        types = [m["msg_type"] for m in self.engine.list_outbox()]
        self.assertIn("CONFIRMED", types)
        self.assertIn("EVENT_FILLED", types)
        self.assertIn("INVITE_SUPERSEDED", types)


if __name__ == "__main__":
    unittest.main()
