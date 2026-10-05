"""HTTP/JSON 接口端到端测试（标准库客户端，无第三方依赖）。"""
from __future__ import annotations

import json
import sys
import tempfile
import threading
import unittest
import urllib.request
import urllib.error
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from relay.clock import FakeClock
from relay.httpapi import build_server
from relay.service import RelayService
from relay.store import Store


class ApiTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.clock = FakeClock("2026-10-05T09:00:00Z")
        self.store = Store(str(Path(self.tmp.name) / "relay.db"))
        self.service = RelayService(self.store, self.clock)
        self.httpd = build_server(self.service, "127.0.0.1", 0)
        self.port = self.httpd.server_address[1]
        self.thread = threading.Thread(target=self.httpd.serve_forever, daemon=True)
        self.thread.start()

    def tearDown(self) -> None:
        self.httpd.shutdown()
        self.httpd.server_close()
        self.thread.join(timeout=2)
        self.store.close()
        self.tmp.cleanup()

    def call(self, method: str, path: str, body=None):
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(
            f"http://127.0.0.1:{self.port}{path}",
            data=data,
            method=method,
            headers={"Content-Type": "application/json"},
        )
        try:
            with urllib.request.urlopen(req, timeout=5) as resp:
                return resp.status, json.loads(resp.read().decode())
        except urllib.error.HTTPError as exc:
            return exc.code, json.loads(exc.read().decode())

    def seed(self) -> None:
        for i, dist in enumerate([500, 900, 1500], start=1):
            self.call("PUT", f"/candidates/c{i}", {
                "name": f"候选{i}", "qualifications": ["guide"],
                "distance_m": dist, "contact": f"1390000000{i}",
            })

    def test_full_relay_flow_over_http(self) -> None:
        self.seed()
        status, body = self.call("GET", "/health")
        self.assertEqual(status, 200)

        status, event = self.call("POST", "/events", {
            "activity": "陶瓷厅讲解",
            "starts_at": "2026-10-05T11:00:00Z",
            "deadline": "2026-10-05T10:50:00Z",
            "required_qualifications": ["guide"],
            "batch_size": 2,
            "invite_timeout_seconds": 300,
        })
        self.assertEqual(status, 201)
        event_id = event["event"]["id"]
        self.assertEqual(len(event["chain"][0]["invitations"]), 2)

        # 首个确认获岗
        winner_inv = event["chain"][0]["invitations"][0]["id"]
        status, body = self.call("POST", f"/invitations/{winner_inv}/respond", {"action": "accept"})
        self.assertEqual(status, 200)
        self.assertEqual(body["final_status"], "resolved")
        self.assertEqual(body["final_assignment"]["candidate_id"], "c1")
        self.assertEqual(body["chain"][0]["invitations"][1]["status"], "superseded")

        # 另一个确认被拒绝（409）
        loser_inv = event["chain"][0]["invitations"][1]["id"]
        status, body = self.call("POST", f"/invitations/loser/respond", {"action": "accept"})
        self.assertEqual(status, 404)
        status, body = self.call("POST", f"/invitations/{loser_inv}/respond", {"action": "accept"})
        self.assertEqual(status, 409)
        self.assertEqual(body["error"]["code"], "event_closed")

        # 接力链实时视图与最终安排
        status, body = self.call("GET", f"/events/{event_id}")
        self.assertEqual(status, 200)
        self.assertEqual(body["final_assignment"]["name"], "候选1")

    def test_decline_timeout_next_batch_and_recover(self) -> None:
        self.seed()
        _, event = self.call("POST", "/events", {
            "activity": "A", "starts_at": "2026-10-05T11:00:00Z",
            "deadline": "2026-10-05T10:50:00Z",
            "required_qualifications": ["guide"],
            "batch_size": 1, "invite_timeout_seconds": 300,
        })
        first = event["chain"][0]["invitations"][0]["id"]
        self.call("POST", f"/invitations/{first}/respond", {"action": "decline"})
        # 第二批自动发给 c2
        _, event = self.call("GET", f"/events/{event['event']['id']}")
        self.assertEqual(event["chain"][1]["invitations"][0]["candidate_id"], "c2")

        # 复岗：撤销在途邀请
        status, body = self.call("POST", f"/events/{event['event']['id']}/recover", {"note": "back"})
        self.assertEqual(status, 200)
        self.assertEqual(body["final_status"], "recovered")
        self.assertEqual(body["chain"][1]["invitations"][0]["status"], "revoked")

    def test_validation_errors(self) -> None:
        self.seed()
        status, body = self.call("POST", "/events", {
            "activity": "A", "starts_at": "2026-10-05T11:00:00Z",
            "deadline": "2026-10-05T08:00:00Z",  # 早于当前
        })
        self.assertEqual(status, 400)
        self.assertIn("code", body["error"])
        status, _ = self.call("GET", "/events/evt_nonexistent")
        self.assertEqual(status, 404)


if __name__ == "__main__":
    unittest.main()
