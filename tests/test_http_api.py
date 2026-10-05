"""HTTP 接口端到端测试（标准库 urllib，零外部依赖）。"""
from __future__ import annotations

import json
import sys
import threading
import unittest
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from relay_service import FakeClock, RelayConfig, RecordingNotifier, RelayService, Store
from relay_service.httpapi import build_server


def _request(base: str, method: str, path: str, body: dict | None = None):
    data = json.dumps(body).encode("utf-8") if body is not None else None
    req = urllib.request.Request(
        base + path, data=data, method=method,
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            return resp.status, json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read().decode("utf-8"))


class HttpApiTest(unittest.TestCase):
    def setUp(self) -> None:
        from relay_service import RelayEngine
        from relay_service.notifications import OutboxDispatcher

        self.clock = FakeClock()
        self.store = Store(":memory:")
        self.engine = RelayEngine(
            self.store, clock=self.clock,
            config=RelayConfig(batch_size=2, invite_ttl_seconds=900),
        )
        self.notifier = RecordingNotifier()
        self.dispatcher = OutboxDispatcher(self.store, notifier=self.notifier, clock=self.clock)
        service = RelayService(self.store, self.engine, self.dispatcher)
        self.server = build_server(service, "127.0.0.1", 0)
        self.port = self.server.server_address[1]
        self.base = f"http://127.0.0.1:{self.port}"
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def tearDown(self) -> None:
        self.server.shutdown()
        self.thread.join(timeout=5)
        self.server.server_close()

    def test_full_relay_flow_over_http(self) -> None:
        # 登记候选
        for cid, dist, quals in (
            ("c1", 800, ["A", "急救"]),
            ("c2", 1200, ["A"]),
            ("c3", 2000, ["A"]),
            ("c4", 300, ["B"]),
        ):
            status, _ = _request(self.base, "POST", "/admin/candidates", {
                "candidate_id": cid, "name": cid, "qualifications": quals,
                "distance_meters": dist, "contact": cid,
            })
            self.assertEqual(status, 201)

        status, body = _request(self.base, "POST", "/events", {
            "shift_label": "14:00 青铜厅",
            "original_guide_id": "g0",
            "event_starts_at": "2026-10-05T12:00:00Z",
            "deadline": "2026-10-05T11:30:00Z",
            "required_qualifications": ["A"],
        })
        self.assertEqual(status, 201, body)
        eid = body["event_id"]
        self.assertEqual([i["candidate_id"] for i in body["rounds"][0]["invitations"]],
                         ["c1", "c2"])

        # 缺字段 → 400
        status, body = _request(self.base, "POST", "/events", {"shift_label": "x"})
        self.assertEqual(status, 400)

        # 不存在 → 404
        status, _ = _request(self.base, "GET", "/events/nope")
        self.assertEqual(status, 404)

        # c1 拒绝
        status, ev = _request(self.base, "GET", f"/events/{eid}")
        inv1 = ev["rounds"][0]["invitations"][0]["invite_id"]
        inv2 = ev["rounds"][0]["invitations"][1]["invite_id"]
        status, _ = _request(self.base, "POST", f"/invitations/{inv1}/decline",
                             {"candidate_id": "c1", "reason": "在外地"})
        self.assertEqual(status, 200)

        # c2 确认 → FILLED
        status, won = _request(self.base, "POST", f"/invitations/{inv2}/confirm",
                               {"candidate_id": "c2"})
        self.assertEqual(status, 200, won)
        self.assertEqual(won["status"], "FILLED")
        self.assertEqual(won["final_assignment"]["candidate_id"], "c2")

        # 迟到确认 → 409
        status, body = _request(self.base, "POST", f"/invitations/{inv1}/confirm",
                                {"candidate_id": "c1"})
        self.assertEqual(status, 409)

        # 接力链实时可见
        status, ev = _request(self.base, "GET", f"/events/{eid}")
        self.assertEqual(ev["open_invitation_count"], 0)
        self.assertEqual(len(ev["chain"]), 2)
        self.assertEqual(ev["chain"][0]["status"], "DECLINED")
        self.assertEqual(ev["chain"][1]["status"], "CONFIRMED")

        # 投递箱
        status, out = _request(self.base, "GET", f"/events/{eid}/outbox")
        self.assertEqual(status, 200)
        self.assertTrue(any(m["msg_type"] == "EVENT_FILLED" for m in out["messages"]))

    def test_restore_over_http(self) -> None:
        _request(self.base, "POST", "/admin/candidates", {
            "candidate_id": "c1", "name": "甲", "qualifications": ["A"],
            "distance_meters": 100,
        })
        _, ev = _request(self.base, "POST", "/events", {
            "shift_label": "s", "original_guide_id": "g0",
            "event_starts_at": "2026-10-05T12:00:00Z",
            "deadline": "2026-10-05T11:30:00Z",
            "required_qualifications": ["A"],
        })
        status, body = _request(self.base, "POST", f"/events/{ev['event_id']}/restore",
                                {"note": "讲解员已到馆"})
        self.assertEqual(status, 200)
        self.assertEqual(body["status"], "CANCELLED_RESTORED")
        self.assertTrue(all(i["status"] == "EXPIRED" for i in body["chain"]))


if __name__ == "__main__":
    unittest.main()
