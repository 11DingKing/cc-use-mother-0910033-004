"""接力领域服务的集成测试：确定性时钟 + 临时 SQLite 文件。"""
from __future__ import annotations

import tempfile
import threading
import unittest
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from relay.clock import FakeClock
from relay.service import (
    ACCEPTED,
    DECLINED,
    EXPIRED,
    FAILED,
    OPEN,
    RECOVERED,
    RESOLVED,
    SUPERSEDED,
    RelayError,
    RelayService,
)
from relay.store import Store

BASE = "2026-10-05T09:00:00Z"


def make_candidates(service: RelayService) -> None:
    for c in [
        {"id": "c1", "name": "甲", "qualifications": ["guide"], "distance_m": 500,
         "consecutive_shifts": 0, "max_consecutive_shifts": 3, "contact": "101"},
        {"id": "c2", "name": "乙", "qualifications": ["guide", "first_aid"], "distance_m": 900,
         "consecutive_shifts": 1, "max_consecutive_shifts": 3, "contact": "102"},
        {"id": "c3", "name": "丙", "qualifications": ["guide"], "distance_m": 1500,
         "consecutive_shifts": 2, "max_consecutive_shifts": 3, "contact": "103"},
        {"id": "c4", "name": "丁", "qualifications": ["english"], "distance_m": 300,
         "consecutive_shifts": 0, "max_consecutive_shifts": 3, "contact": "104"},
        {"id": "c5", "name": "戊", "qualifications": ["guide"], "distance_m": 700,
         "consecutive_shifts": 3, "max_consecutive_shifts": 3, "contact": "105"},
        {"id": "c6", "name": "己", "qualifications": ["guide"], "distance_m": 5000,
         "consecutive_shifts": 0, "max_consecutive_shifts": 3, "contact": "106"},
    ]:
        service.upsert_candidate(c)


def event_request(**overrides):
    req = {
        "activity": "青铜器展厅讲解",
        "starts_at": "2026-10-05T11:00:00Z",
        "deadline": "2026-10-05T10:50:00Z",
        "required_qualifications": ["guide"],
        "max_distance_m": 3000,
        "batch_size": 2,
        "invite_timeout_seconds": 300,
    }
    req.update(overrides)
    return req


class RelayServiceTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.db = str(Path(self.tmp.name) / "relay.db")
        self.clock = FakeClock(BASE)
        self.store = Store(self.db)
        self.service = RelayService(self.store, self.clock)
        make_candidates(self.service)

    def tearDown(self) -> None:
        self.store.close()
        self.tmp.cleanup()

    def first_wave(self, view):
        return view["chain"][0]["invitations"]

    def test_screening_filters_qualification_distance_consecutive(self) -> None:
        view = self.service.create_event(event_request(batch_size=10))
        wave0 = {r["candidate_id"]: r for r in view["screening"][0]["results"]}
        # 丁缺 guide 资格、戊达连续服务上限、己超距离
        self.assertEqual(wave0["c4"]["eligible"], False)
        self.assertTrue(wave0["c4"]["reason"].startswith("missing_qualification"))
        self.assertEqual(wave0["c5"]["reason"], "consecutive_limit")
        self.assertEqual(wave0["c6"]["reason"], "too_far")
        # 合格者按距离升序：甲(500) 乙(900) 丙(1500)
        invited = [i["candidate_id"] for i in self.first_wave(view)]
        self.assertEqual(invited, ["c1", "c2", "c3"])

    def test_batched_invites_and_decline_advances_wave(self) -> None:
        view = self.service.create_event(event_request(batch_size=2))
        self.assertEqual([i["candidate_id"] for i in self.first_wave(view)], ["c1", "c2"])
        self.assertEqual(view["pending_invitation_count"], 2)
        # 只有一人拒绝时不开新批（另一邀请仍在途）
        inv_c1 = self.first_wave(view)[0]["id"]
        view = self.service.respond(inv_c1, "decline")
        self.assertEqual(len(view["chain"]), 1)
        self.assertEqual(view["chain"][0]["invitations"][0]["status"], DECLINED)
        # 第二人也拒绝：整批落定，发出第二批
        inv_c2 = view["chain"][0]["invitations"][1]["id"]
        view = self.service.respond(inv_c2, "decline")
        self.assertEqual(len(view["chain"]), 2)
        self.assertEqual([i["candidate_id"] for i in view["chain"][1]["invitations"]], ["c3"])

    def test_first_accept_atomically_wins_concurrent(self) -> None:
        view = self.service.create_event(event_request(batch_size=3))
        inv_ids = [i["id"] for i in self.first_wave(view)]
        outcomes: list[str] = []
        barrier = threading.Barrier(len(inv_ids))

        def accept(inv_id: str) -> None:
            barrier.wait()
            try:
                self.service.respond(inv_id, "accept")
                outcomes.append("ok:" + inv_id)
            except RelayError as exc:
                outcomes.append("err:" + exc.code)

        threads = [threading.Thread(target=accept, args=(i,)) for i in inv_ids]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        view = self.service.get_event(view["event"]["id"])
        self.assertEqual(view["final_status"], RESOLVED)
        self.assertEqual(len([o for o in outcomes if o.startswith("ok")]), 1)
        statuses = sorted(i["status"] for i in view["chain"][0]["invitations"])
        self.assertEqual(statuses, [ACCEPTED, SUPERSEDED, SUPERSEDED])
        winner_id = view["final_assignment"]["candidate_id"]
        # 获胜者连续服务班次 +1
        self.assertEqual(self.service.get_candidate(winner_id)["consecutive_shifts"],
                         {"c1": 1, "c2": 2, "c3": 3}[winner_id])
        # 投递箱：1 条 won（候选）+ 1 条 won（运营员）+ 2 条 lost
        kinds = [(r["kind"], r["state"]) for r in self.service.list_outbox()]
        self.assertIn(("won", "pending"), kinds)
        self.assertEqual(sum(1 for k, _ in kinds if k == "lost"), 2)

    def test_late_accept_after_win_is_settled(self) -> None:
        view = self.service.create_event(event_request(batch_size=3))
        invs = self.first_wave(view)
        self.service.respond(invs[0]["id"], "accept")
        # 落败者随后才响应：409，邀请确定性落为 superseded，不会产生第二个获胜者
        with self.assertRaises(RelayError) as ctx:
            self.service.respond(invs[1]["id"], "accept")
        self.assertEqual(ctx.exception.code, "event_closed")
        view = self.service.get_event(view["event"]["id"])
        self.assertEqual(view["chain"][0]["invitations"][1]["status"], SUPERSEDED)

    def test_invitation_timeout_then_next_wave(self) -> None:
        view = self.service.create_event(
            event_request(batch_size=2, invite_timeout_seconds=300)
        )
        invs = self.first_wave(view)
        # 一人拒绝，另一人沉默；未到 300 秒不开批
        self.service.respond(invs[0]["id"], "decline")
        self.assertEqual(self.service.tick(), 0)
        self.assertEqual(len(self.service.get_event(view["event"]["id"])["chain"]), 1)
        # 超过邀请有效期：超时失效，下一批发出
        self.clock.advance(301)
        self.assertGreaterEqual(self.service.tick(), 1)
        view = self.service.get_event(view["event"]["id"])
        self.assertEqual(view["chain"][0]["invitations"][1]["status"], EXPIRED)
        self.assertEqual(len(view["chain"]), 2)
        self.assertEqual(view["chain"][1]["invitations"][0]["candidate_id"], "c3")

    def test_deadline_reached_without_cover_fails(self) -> None:
        view = self.service.create_event(
            event_request(batch_size=2, invite_timeout_seconds=300)
        )
        event_id = view["event"]["id"]
        # 把时间推过截止时间，巡检应把在途邀请失效且最终 failed
        self.clock.set("2026-10-05T10:50:01Z")
        self.service.tick()
        view = self.service.get_event(event_id)
        self.assertEqual(view["final_status"], FAILED)
        self.assertTrue(all(i["status"] == EXPIRED for i in view["chain"][0]["invitations"]))
        kinds = [r["kind"] for r in self.service.list_outbox()]
        self.assertIn("expired_event", kinds)

    def test_recover_revokes_all_pending(self) -> None:
        view = self.service.create_event(event_request(batch_size=2))
        event_id = view["event"]["id"]
        view = self.service.recover(event_id)
        self.assertEqual(view["final_status"], RECOVERED)
        self.assertTrue(all(i["status"] == "revoked" for i in view["chain"][0]["invitations"]))
        # 复岗后再响应，邀请落 revoked 且事件不改变
        inv_id = view["chain"][0]["invitations"][0]["id"]
        with self.assertRaises(RelayError) as ctx:
            self.service.respond(inv_id, "accept")
        self.assertEqual(ctx.exception.code, "event_closed")
        self.assertEqual(self.service.get_event(event_id)["final_status"], RECOVERED)
        # 复岗后不能重复复岗
        with self.assertRaises(RelayError):
            self.service.recover(event_id)

    def test_restart_recovers_state_from_disk(self) -> None:
        view = self.service.create_event(
            event_request(batch_size=1, invite_timeout_seconds=300)
        )
        event_id = view["event"]["id"]
        self.assertEqual(self.first_wave(view)[0]["candidate_id"], "c1")
        self.store.close()

        # 模拟进程重启：重新打开同一个库，启动巡检
        self.clock.advance(301)
        store2 = Store(self.db)
        service2 = RelayService(store2, self.clock)
        service2.tick()
        view = service2.get_event(event_id)
        self.assertEqual(view["chain"][0]["invitations"][0]["status"], EXPIRED)
        self.assertEqual(view["chain"][1]["invitations"][0]["candidate_id"], "c2")

        # 继续推进：c2、c3 先后超时，候选耗尽后到截止时间 -> failed
        self.clock.advance(301)
        service2.tick()
        self.clock.advance(301)
        service2.tick()
        view = service2.get_event(event_id)
        self.assertEqual(view["chain"][2]["invitations"][0]["candidate_id"], "c3")
        self.assertEqual(view["final_status"], OPEN)  # 未到截止仍等待
        self.clock.set("2026-10-05T10:50:01Z")
        service2.tick()
        self.assertEqual(service2.get_event(event_id)["final_status"], FAILED)
        store2.close()

    def test_deadline_must_be_before_start_and_future(self) -> None:
        with self.assertRaises(RelayError):
            self.service.create_event(event_request(deadline="2026-10-05T11:30:00Z"))
        with self.assertRaises(RelayError):
            self.service.create_event(event_request(deadline="2026-10-05T08:00:00Z"))


if __name__ == "__main__":
    unittest.main()
