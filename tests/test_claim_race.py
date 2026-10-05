"""并发确认竞态测试：多线程同时确认，保证且仅保证一人获岗。"""
from __future__ import annotations

import sys
import threading
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from relay_service import FakeClock, RelayConfig, RelayEngine, Store
from relay_service.store import EVENT_FILLED, INVITE_CONFIRMED


class ClaimRaceTest(unittest.TestCase):
    def test_concurrent_confirms_exactly_one_winner(self) -> None:
        clock = FakeClock()
        store = Store(":memory:")
        engine = RelayEngine(
            store, clock=clock, config=RelayConfig(batch_size=10, invite_ttl_seconds=900),
        )
        for i in range(8):
            engine.register_candidate(f"c{i}", f"候选{i}", ["A"], 100 * (i + 1))
        view = engine.create_event(
            shift_label="s", original_guide_id="g0",
            event_starts_at="2026-10-05T12:00:00Z",
            deadline="2026-10-05T11:30:00Z",
            required_qualifications=["A"],
        )
        invite_ids = [i["invite_id"] for i in view["chain"]]
        self.assertEqual(len(invite_ids), 8)

        results: list[tuple[str, bool]] = []
        barrier = threading.Barrier(8)

        def worker(idx: int, invite_id: str) -> None:
            barrier.wait()
            try:
                engine.confirm(invite_id, candidate_id=f"c{idx}")
                results.append((f"c{idx}", True))
            except Exception:
                results.append((f"c{idx}", False))

        threads = [
            threading.Thread(target=worker, args=(i, iid))
            for i, iid in enumerate(invite_ids)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        winners = [c for c, ok in results if ok]
        self.assertEqual(len(winners), 1, f"应有且仅有一个胜者，实际：{winners}")

        final = engine.get_event(view["event_id"])
        self.assertEqual(final["status"], EVENT_FILLED)
        self.assertEqual(final["final_assignment"]["candidate_id"], winners[0])
        confirmed = [i for i in final["chain"] if i["status"] == INVITE_CONFIRMED]
        self.assertEqual(len(confirmed), 1)


if __name__ == "__main__":
    unittest.main()
