"""SQLite 持久化层。

设计要点：

- 单连接 + ``BEGIN IMMEDIATE`` + 进程内互斥锁，把所有写操作串行化为
  带快照的严格可串行化事务；"读取岗位状态→写入获胜确认"在同一事务内完成，
  从存储层杜绝两个确认同时获岗。
- 所有时间以 ISO 字符串落库（可字典序比较），进程重启后状态完整可用。
- 提交后通过条件变量广播变更，供 SSE 实时推送与后台巡检唤醒。
"""
from __future__ import annotations

import json
import sqlite3
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

SCHEMA = """
CREATE TABLE IF NOT EXISTS candidates (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    qualifications TEXT NOT NULL,
    distance_m REAL NOT NULL,
    lat REAL,
    lng REAL,
    consecutive_shifts INTEGER NOT NULL DEFAULT 0,
    max_consecutive_shifts INTEGER NOT NULL DEFAULT 6,
    available INTEGER NOT NULL DEFAULT 1,
    contact TEXT,
    guardian_contact TEXT
);

CREATE TABLE IF NOT EXISTS relay_events (
    id TEXT PRIMARY KEY,
    activity TEXT NOT NULL,
    assignee_id TEXT,
    starts_at TEXT NOT NULL,
    deadline TEXT NOT NULL,
    batch_size INTEGER NOT NULL,
    invite_timeout_seconds REAL NOT NULL,
    batch_gap_seconds REAL NOT NULL,
    required_qualifications TEXT NOT NULL DEFAULT '[]',
    max_distance_m REAL,
    location TEXT,
    status TEXT NOT NULL,
    winner_invitation_id TEXT,
    close_reason TEXT,
    created_at TEXT NOT NULL,
    closed_at TEXT
);

CREATE TABLE IF NOT EXISTS invitations (
    id TEXT PRIMARY KEY,
    event_id TEXT NOT NULL REFERENCES relay_events(id),
    candidate_id TEXT NOT NULL,
    wave INTEGER NOT NULL,
    rank INTEGER NOT NULL,
    status TEXT NOT NULL,
    invited_at TEXT,
    expires_at TEXT,
    responded_at TEXT,
    outcome TEXT,
    UNIQUE(event_id, candidate_id)
);
CREATE INDEX IF NOT EXISTS idx_invitations_event ON invitations(event_id, wave, rank);
CREATE INDEX IF NOT EXISTS idx_invitations_status ON invitations(status, expires_at);

-- 每批评选留痕：合格者记录全序名次，不合格者记录剔除原因，供接口透明展示。
CREATE TABLE IF NOT EXISTS screening_trace (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT NOT NULL REFERENCES relay_events(id),
    wave INTEGER NOT NULL,
    candidate_id TEXT NOT NULL,
    rank INTEGER,
    eligible INTEGER NOT NULL,
    reason TEXT,
    distance_m REAL,
    created_at TEXT NOT NULL,
    UNIQUE(event_id, wave, candidate_id)
);
CREATE INDEX IF NOT EXISTS idx_screening_event ON screening_trace(event_id, wave);

CREATE TABLE IF NOT EXISTS outbox (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    target TEXT NOT NULL,
    channel TEXT NOT NULL,
    kind TEXT NOT NULL,
    ref_type TEXT NOT NULL,
    ref_id TEXT NOT NULL,
    payload TEXT NOT NULL,
    created_at TEXT NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    state TEXT NOT NULL DEFAULT 'pending',
    claimed_at TEXT,
    sent_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_outbox_state ON outbox(state, claimed_at, id);

-- 至少一次投递的幂等去重：同一个 notify_key 只允许真正下发一次。
CREATE TABLE IF NOT EXISTS notifications_delivered (
    notify_key TEXT PRIMARY KEY,
    delivered_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
"""


class Store:
    """线程安全的 SQLite 封装。"""

    def __init__(self, path: str | Path = ":memory:") -> None:
        self._path = str(path)
        self._lock = threading.RLock()
        self._cond = threading.Condition()
        self._conn = sqlite3.connect(self._path, check_same_thread=False, isolation_level=None)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._conn.execute("PRAGMA busy_timeout=5000")
        self._conn.executescript(SCHEMA)

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    # ---- 事务 ----------------------------------------------------------------

    @contextmanager
    def tx(self) -> Iterator[sqlite3.Connection]:
        """开启串行写事务；提交后广播一次变更。"""
        with self._lock:
            conn = self._conn
            conn.execute("BEGIN IMMEDIATE")
            try:
                yield conn
            except Exception:
                conn.execute("ROLLBACK")
                raise
            else:
                conn.execute("COMMIT")
        with self._cond:
            self._cond.notify_all()

    def query(self, sql: str, params: tuple = ()) -> list[sqlite3.Row]:
        with self._lock:
            return list(self._conn.execute(sql, params))

    def query_one(self, sql: str, params: tuple = ()) -> sqlite3.Row | None:
        with self._lock:
            return self._conn.execute(sql, params).fetchone()

    # ---- 变更等待（SSE / 巡检） ----------------------------------------------

    def wait_change(self, timeout: float | None = None) -> None:
        with self._cond:
            self._cond.wait(timeout)

    # ---- 候选主数据 -----------------------------------------------------------

    def upsert_candidate(self, c: dict[str, Any]) -> None:
        with self.tx() as conn:
            conn.execute(
                """
                INSERT INTO candidates(id, name, qualifications, distance_m, lat, lng,
                                       consecutive_shifts, max_consecutive_shifts,
                                       available, contact, guardian_contact)
                VALUES (:id, :name, :qualifications, :distance_m, :lat, :lng,
                        :consecutive_shifts, :max_consecutive_shifts,
                        :available, :contact, :guardian_contact)
                ON CONFLICT(id) DO UPDATE SET
                    name=excluded.name,
                    qualifications=excluded.qualifications,
                    distance_m=excluded.distance_m,
                    lat=excluded.lat,
                    lng=excluded.lng,
                    consecutive_shifts=excluded.consecutive_shifts,
                    max_consecutive_shifts=excluded.max_consecutive_shifts,
                    available=excluded.available,
                    contact=excluded.contact,
                    guardian_contact=excluded.guardian_contact
                """,
                {
                    "id": c["id"],
                    "name": c["name"],
                    "qualifications": json.dumps(c.get("qualifications", []), ensure_ascii=False),
                    "distance_m": c["distance_m"],
                    "lat": c.get("lat"),
                    "lng": c.get("lng"),
                    "consecutive_shifts": c.get("consecutive_shifts", 0),
                    "max_consecutive_shifts": c.get("max_consecutive_shifts", 6),
                    "available": 1 if c.get("available", True) else 0,
                    "contact": c.get("contact"),
                    "guardian_contact": c.get("guardian_contact"),
                },
            )

    def list_candidates(self) -> list[dict[str, Any]]:
        rows = self.query("SELECT * FROM candidates ORDER BY id")
        return [self._candidate_dict(r) for r in rows]

    @staticmethod
    def _candidate_dict(r: sqlite3.Row) -> dict[str, Any]:
        return {
            "id": r["id"],
            "name": r["name"],
            "qualifications": json.loads(r["qualifications"]),
            "distance_m": r["distance_m"],
            "lat": r["lat"],
            "lng": r["lng"],
            "consecutive_shifts": r["consecutive_shifts"],
            "max_consecutive_shifts": r["max_consecutive_shifts"],
            "available": bool(r["available"]),
            "contact": r["contact"],
            "guardian_contact": r["guardian_contact"],
        }

    # ---- 接力事件 -------------------------------------------------------------

    def insert_event(self, conn: sqlite3.Connection, e: dict[str, Any]) -> None:
        conn.execute(
            """
            INSERT INTO relay_events(id, activity, assignee_id, starts_at, deadline,
                                     batch_size, invite_timeout_seconds, batch_gap_seconds,
                                     required_qualifications, max_distance_m, location,
                                     status, winner_invitation_id, close_reason,
                                     created_at, closed_at)
            VALUES (:id, :activity, :assignee_id, :starts_at, :deadline,
                    :batch_size, :invite_timeout_seconds, :batch_gap_seconds,
                    :required_qualifications, :max_distance_m, :location,
                    :status, :winner_invitation_id, :close_reason,
                    :created_at, :closed_at)
            """,
            {
                "id": e["id"],
                "activity": e["activity"],
                "assignee_id": e.get("assignee_id"),
                "starts_at": e["starts_at"],
                "deadline": e["deadline"],
                "batch_size": e["batch_size"],
                "invite_timeout_seconds": e["invite_timeout_seconds"],
                "batch_gap_seconds": e["batch_gap_seconds"],
                "required_qualifications": json.dumps(
                    e.get("required_qualifications", []), ensure_ascii=False
                ),
                "max_distance_m": e.get("max_distance_m"),
                "location": json.dumps(e["location"], ensure_ascii=False)
                if e.get("location") is not None
                else None,
                "status": e["status"],
                "winner_invitation_id": e.get("winner_invitation_id"),
                "close_reason": e.get("close_reason"),
                "created_at": e["created_at"],
                "closed_at": e.get("closed_at"),
            },
        )

    def get_event_row(self, event_id: str) -> sqlite3.Row | None:
        return self.query_one("SELECT * FROM relay_events WHERE id=?", (event_id,))

    def list_open_event_rows(self) -> list[sqlite3.Row]:
        return self.query(
            "SELECT * FROM relay_events WHERE status IN ('open','waiting') ORDER BY created_at"
        )

    def list_event_rows(self, limit: int = 100) -> list[sqlite3.Row]:
        rows = self.query("SELECT * FROM relay_events ORDER BY created_at DESC LIMIT ?", (limit,))
        return list(rows)

    @staticmethod
    def event_dict(r: sqlite3.Row) -> dict[str, Any]:
        return {
            "id": r["id"],
            "activity": r["activity"],
            "assignee_id": r["assignee_id"],
            "starts_at": r["starts_at"],
            "deadline": r["deadline"],
            "batch_size": r["batch_size"],
            "invite_timeout_seconds": r["invite_timeout_seconds"],
            "batch_gap_seconds": r["batch_gap_seconds"],
            "required_qualifications": json.loads(r["required_qualifications"]),
            "max_distance_m": r["max_distance_m"],
            "location": json.loads(r["location"]) if r["location"] else None,
            "status": r["status"],
            "winner_invitation_id": r["winner_invitation_id"],
            "close_reason": r["close_reason"],
            "created_at": r["created_at"],
            "closed_at": r["closed_at"],
        }

    # ---- 邀请 -----------------------------------------------------------------

    def insert_invitation(self, conn: sqlite3.Connection, inv: dict[str, Any]) -> None:
        conn.execute(
            """
            INSERT INTO invitations(id, event_id, candidate_id, wave, rank, status,
                                    invited_at, expires_at, responded_at, outcome)
            VALUES (:id, :event_id, :candidate_id, :wave, :rank, :status,
                    :invited_at, :expires_at, :responded_at, :outcome)
            """,
            {
                "id": inv["id"],
                "event_id": inv["event_id"],
                "candidate_id": inv["candidate_id"],
                "wave": inv["wave"],
                "rank": inv["rank"],
                "status": inv["status"],
                "invited_at": inv.get("invited_at"),
                "expires_at": inv.get("expires_at"),
                "responded_at": inv.get("responded_at"),
                "outcome": inv.get("outcome"),
            },
        )

    def get_invitation_row(self, invitation_id: str) -> sqlite3.Row | None:
        return self.query_one("SELECT * FROM invitations WHERE id=?", (invitation_id,))

    def list_invitation_rows(self, event_id: str) -> list[sqlite3.Row]:
        return self.query(
            "SELECT * FROM invitations WHERE event_id=? ORDER BY wave, rank", (event_id,)
        )

    def list_screening_rows(self, event_id: str) -> list[sqlite3.Row]:
        return self.query(
            "SELECT * FROM screening_trace WHERE event_id=? ORDER BY wave, rank, candidate_id",
            (event_id,),
        )

    @staticmethod
    def invitation_dict(r: sqlite3.Row) -> dict[str, Any]:
        return {
            "id": r["id"],
            "event_id": r["event_id"],
            "candidate_id": r["candidate_id"],
            "wave": r["wave"],
            "rank": r["rank"],
            "status": r["status"],
            "invited_at": r["invited_at"],
            "expires_at": r["expires_at"],
            "responded_at": r["responded_at"],
            "outcome": r["outcome"],
        }

    # ---- 事务投递箱 ------------------------------------------------------------

    def outbox_add(self, conn: sqlite3.Connection, messages: list[dict[str, Any]]) -> None:
        """在业务事务内追加投递箱消息（与状态变更原子提交）。"""
        conn.executemany(
            """
            INSERT INTO outbox(target, channel, kind, ref_type, ref_id, payload,
                               created_at, state)
            VALUES (:target, :channel, :kind, :ref_type, :ref_id, :payload,
                    :created_at, 'pending')
            """,
            [
                {
                    "target": m["target"],
                    "channel": m["channel"],
                    "kind": m["kind"],
                    "ref_type": m["ref_type"],
                    "ref_id": m["ref_id"],
                    "payload": json.dumps(m["payload"], ensure_ascii=False),
                    "created_at": m["created_at"],
                }
                for m in messages
            ],
        )

    def outbox_claimable(self, stale_before: str, limit: int = 32) -> list[sqlite3.Row]:
        """待发或认领后崩溃（sending 超过陈旧阈值）的消息。"""
        return self.query(
            """
            SELECT * FROM outbox
            WHERE state='pending'
               OR (state='sending' AND claimed_at < ?)
            ORDER BY id LIMIT ?
            """,
            (stale_before, limit),
        )

    def outbox_is_delivered(self, conn: sqlite3.Connection, notify_key: str) -> bool:
        return conn.execute(
            "SELECT 1 FROM notifications_delivered WHERE notify_key=?", (notify_key,)
        ).fetchone() is not None

    def outbox_claim(self, conn: sqlite3.Connection, outbox_id: int, now_iso: str) -> None:
        conn.execute(
            "UPDATE outbox SET state='sending', claimed_at=?, attempts=attempts+1 WHERE id=?",
            (now_iso, outbox_id),
        )

    def outbox_release(self, conn: sqlite3.Connection, outbox_id: int) -> None:
        """发送失败：释放认领，回到 pending 等待重试。"""
        conn.execute("UPDATE outbox SET state='pending', claimed_at=NULL WHERE id=?", (outbox_id,))

    def outbox_complete(
        self, conn: sqlite3.Connection, outbox_id: int, notify_key: str, now_iso: str
    ) -> None:
        """发送成功：同一事务内标记 sent 并记录幂等键。"""
        conn.execute("UPDATE outbox SET state='sent', sent_at=? WHERE id=?", (now_iso, outbox_id))
        conn.execute(
            "INSERT OR IGNORE INTO notifications_delivered(notify_key, delivered_at) VALUES (?, ?)",
            (notify_key, now_iso),
        )

    # ---- 投递幂等 --------------------------------------------------------------

    def notification_already_delivered(self, notify_key: str) -> bool:
        return self.query_one(
            "SELECT 1 FROM notifications_delivered WHERE notify_key=?", (notify_key,)
        ) is not None

    def mark_notification_delivered(
        self, conn: sqlite3.Connection, notify_key: str, delivered_at: str
    ) -> None:
        conn.execute(
            "INSERT OR IGNORE INTO notifications_delivered(notify_key, delivered_at) VALUES (?, ?)",
            (notify_key, delivered_at),
        )
