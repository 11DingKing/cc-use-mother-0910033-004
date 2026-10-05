"""SQLite 持久化层。

所有状态（接力事件、候选、邀请、通知投递箱）只存放在数据库中，
进程重启后状态完整可恢复；业务层通过 BEGIN IMMEDIATE 事务保证原子性。
"""
from __future__ import annotations

import json
import sqlite3
import threading
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

# 事件状态
EVENT_OPEN = "OPEN"
EVENT_FILLED = "FILLED"
EVENT_CANCELLED_RESTORED = "CANCELLED_RESTORED"
EVENT_EXPIRED = "EXPIRED"

# 邀请状态
INVITE_OPEN = "OPEN"
INVITE_CONFIRMED = "CONFIRMED"
INVITE_DECLINED = "DECLINED"
INVITE_EXPIRED = "EXPIRED"
INVITE_SUPERSEDED = "SUPERSEDED"

# 投递箱状态
OUT_PENDING = "PENDING"
OUT_PROCESSING = "PROCESSING"
OUT_SENT = "SENT"

SCHEMA = """
CREATE TABLE IF NOT EXISTS relay_events (
    event_id            TEXT PRIMARY KEY,
    shift_label         TEXT NOT NULL,
    original_guide_id   TEXT NOT NULL,
    event_starts_at     TEXT NOT NULL,
    deadline            TEXT NOT NULL,
    required_qualifications TEXT NOT NULL DEFAULT '[]',
    batch_size          INTEGER NOT NULL,
    invite_ttl_seconds  REAL NOT NULL,
    status              TEXT NOT NULL,
    note                TEXT NOT NULL DEFAULT '',
    winner_invite_id    TEXT,
    final_candidate_id  TEXT,
    decided_at          TEXT,
    created_at          TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS candidates (
    candidate_id      TEXT PRIMARY KEY,
    name              TEXT NOT NULL,
    contact           TEXT NOT NULL DEFAULT '',
    qualifications    TEXT NOT NULL,
    distance_meters   REAL NOT NULL,
    last_service_end  TEXT,
    created_at        TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS invitations (
    invite_id      TEXT PRIMARY KEY,
    event_id       TEXT NOT NULL REFERENCES relay_events(event_id),
    candidate_id   TEXT NOT NULL,
    round          INTEGER NOT NULL,
    status         TEXT NOT NULL,
    sent_at        TEXT NOT NULL,
    expires_at     TEXT NOT NULL,
    responded_at   TEXT,
    decline_reason TEXT NOT NULL DEFAULT '',
    UNIQUE(event_id, candidate_id)
);
CREATE INDEX IF NOT EXISTS idx_invitations_event ON invitations(event_id);
CREATE TABLE IF NOT EXISTS outbox (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    idem_key    TEXT NOT NULL UNIQUE,
    event_id    TEXT,
    invite_id   TEXT,
    target      TEXT NOT NULL,
    channel     TEXT NOT NULL,
    msg_type    TEXT NOT NULL,
    payload     TEXT NOT NULL,
    status      TEXT NOT NULL,
    attempts    INTEGER NOT NULL DEFAULT 0,
    last_error  TEXT NOT NULL DEFAULT '',
    created_at  TEXT NOT NULL,
    locked_at   TEXT,
    sent_at     TEXT
);
CREATE INDEX IF NOT EXISTS idx_outbox_status ON outbox(status, id);
"""


class NotFound(LookupError):
    """聚合根不存在。"""


class Store:
    """线程安全的 SQLite 数据访问对象。"""

    def __init__(self, path: str | Path = ":memory:") -> None:
        self._path = str(path)
        self._lock = threading.RLock()
        self._depth = 0
        self._conn = sqlite3.connect(self._path, check_same_thread=False, isolation_level=None)
        self._conn.row_factory = sqlite3.Row
        if self._path != ":memory:":
            self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._conn.execute("PRAGMA busy_timeout=5000")
        self._conn.executescript(SCHEMA)
        self._migrate()
        self._conn.commit()

    def _migrate(self) -> None:
        """对旧库补齐后加的列（服务尚在迭代期，列级迁移足够）。"""
        event_cols = {r["name"] for r in self._conn.execute("PRAGMA table_info(relay_events)")}
        if "required_qualifications" not in event_cols:
            self._conn.execute("ALTER TABLE relay_events ADD COLUMN required_qualifications TEXT NOT NULL DEFAULT '[]'")
        cand_cols = {r["name"] for r in self._conn.execute("PRAGMA table_info(candidates)")}
        if "contact" not in cand_cols:
            self._conn.execute("ALTER TABLE candidates ADD COLUMN contact TEXT NOT NULL DEFAULT ''")

    @contextmanager
    def begin(self) -> Iterator[sqlite3.Connection]:
        """开启串行写事务（BEGIN IMMEDIATE），提交或回滚由上下文负责。

        同线程可重入：内层加入外层事务，只有最外层负责提交/回滚。
        """
        with self._lock:
            if self._depth > 0:
                self._depth += 1
                try:
                    yield self._conn
                except Exception:
                    raise
                else:
                    self._depth -= 1
                return
            self._conn.execute("BEGIN IMMEDIATE")
            self._depth = 1
            try:
                yield self._conn
                self._conn.commit()
            except Exception:
                self._conn.rollback()
                raise
            finally:
                self._depth = 0

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    # ---- 事件 ----------------------------------------------------------

    def insert_event(self, cur: sqlite3.Connection, data: dict[str, Any]) -> None:
        cur.execute(
            """INSERT INTO relay_events
               (event_id, shift_label, original_guide_id, event_starts_at, deadline,
                required_qualifications, batch_size, invite_ttl_seconds, status, note, created_at)
               VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
            (
                data["event_id"], data["shift_label"], data["original_guide_id"],
                data["event_starts_at"], data["deadline"],
                json.dumps(data["required_qualifications"], ensure_ascii=False),
                data["batch_size"], data["invite_ttl_seconds"], EVENT_OPEN, "", data["created_at"],
            ),
        )

    @staticmethod
    def _event_dict(row: sqlite3.Row | None) -> dict[str, Any] | None:
        if row is None:
            return None
        data = dict(row)
        data["required_qualifications"] = json.loads(data["required_qualifications"])
        return data

    def get_event(self, cur: sqlite3.Connection, event_id: str) -> dict[str, Any]:
        row = cur.execute("SELECT * FROM relay_events WHERE event_id=?", (event_id,)).fetchone()
        if row is None:
            raise NotFound(f"接力事件不存在：{event_id}")
        return self._event_dict(row)

    def find_event(self, cur: sqlite3.Connection, event_id: str) -> dict[str, Any] | None:
        return self._event_dict(
            cur.execute("SELECT * FROM relay_events WHERE event_id=?", (event_id,)).fetchone()
        )

    def list_events(self, cur: sqlite3.Connection) -> list[dict[str, Any]]:
        rows = cur.execute("SELECT * FROM relay_events ORDER BY created_at, event_id").fetchall()
        return [self._event_dict(r) for r in rows]

    def list_open_events(self, cur: sqlite3.Connection) -> list[dict[str, Any]]:
        rows = cur.execute(
            "SELECT * FROM relay_events WHERE status=? ORDER BY created_at, event_id",
            (EVENT_OPEN,),
        ).fetchall()
        return [self._event_dict(r) for r in rows]

    def mark_event_filled(
        self, cur: sqlite3.Connection, event_id: str, winner_invite_id: str,
        winner_candidate_id: str, decided_at: str,
    ) -> None:
        cur.execute(
            """UPDATE relay_events SET status=?, winner_invite_id=?, final_candidate_id=?,
               decided_at=? WHERE event_id=?""",
            (EVENT_FILLED, winner_invite_id, winner_candidate_id, decided_at, event_id),
        )

    def mark_event_terminal(
        self, cur: sqlite3.Connection, event_id: str, status: str, decided_at: str, note: str,
    ) -> None:
        cur.execute(
            "UPDATE relay_events SET status=?, decided_at=?, note=? WHERE event_id=?",
            (status, decided_at, note, event_id),
        )

    # ---- 候选 ----------------------------------------------------------

    def insert_candidate(self, cur: sqlite3.Connection, data: dict[str, Any]) -> None:
        cur.execute(
            """INSERT INTO candidates
               (candidate_id, name, contact, qualifications, distance_meters,
                last_service_end, created_at)
               VALUES (?,?,?,?,?,?,?)""",
            (
                data["candidate_id"], data["name"], data.get("contact", ""),
                json.dumps(data["qualifications"], ensure_ascii=False),
                data["distance_meters"], data.get("last_service_end"), data["created_at"],
            ),
        )

    def upsert_candidate(self, cur: sqlite3.Connection, data: dict[str, Any]) -> None:
        cur.execute(
            """INSERT INTO candidates
               (candidate_id, name, contact, qualifications, distance_meters,
                last_service_end, created_at)
               VALUES (?,?,?,?,?,?,?)
               ON CONFLICT(candidate_id) DO UPDATE SET
                 name=excluded.name,
                 contact=excluded.contact,
                 qualifications=excluded.qualifications,
                 distance_meters=excluded.distance_meters,
                 last_service_end=excluded.last_service_end""",
            (
                data["candidate_id"], data["name"], data.get("contact", ""),
                json.dumps(data["qualifications"], ensure_ascii=False),
                data["distance_meters"], data.get("last_service_end"), data["created_at"],
            ),
        )

    @staticmethod
    def _candidate_dict(row: sqlite3.Row) -> dict[str, Any]:
        return {
            "candidate_id": row["candidate_id"],
            "name": row["name"],
            "contact": row["contact"],
            "qualifications": json.loads(row["qualifications"]),
            "distance_meters": row["distance_meters"],
            "last_service_end": row["last_service_end"],
        }

    def list_candidates(self, cur: sqlite3.Connection) -> list[dict[str, Any]]:
        rows = cur.execute("SELECT * FROM candidates ORDER BY candidate_id").fetchall()
        return [self._candidate_dict(r) for r in rows]

    def get_candidate(self, cur: sqlite3.Connection, candidate_id: str) -> dict[str, Any] | None:
        row = cur.execute(
            "SELECT * FROM candidates WHERE candidate_id=?", (candidate_id,),
        ).fetchone()
        return self._candidate_dict(row) if row else None

    def list_uninvited_candidates(
        self, cur: sqlite3.Connection, event_id: str,
    ) -> list[dict[str, Any]]:
        rows = cur.execute(
            """SELECT c.* FROM candidates c
               WHERE c.candidate_id NOT IN (
                   SELECT candidate_id FROM invitations WHERE event_id=?)
               ORDER BY c.distance_meters ASC, c.candidate_id ASC""",
            (event_id,),
        ).fetchall()
        return [self._candidate_dict(r) for r in rows]

    # ---- 邀请 ----------------------------------------------------------

    def insert_invitation(self, cur: sqlite3.Connection, data: dict[str, Any]) -> None:
        cur.execute(
            """INSERT INTO invitations
               (invite_id, event_id, candidate_id, round, status, sent_at, expires_at)
               VALUES (?,?,?,?,?,?,?)""",
            (
                data["invite_id"], data["event_id"], data["candidate_id"], data["round"],
                INVITE_OPEN, data["sent_at"], data["expires_at"],
            ),
        )

    def get_invitation(self, cur: sqlite3.Connection, invite_id: str) -> dict[str, Any] | None:
        row = cur.execute("SELECT * FROM invitations WHERE invite_id=?", (invite_id,)).fetchone()
        return dict(row) if row else None

    def list_invitations(self, cur: sqlite3.Connection, event_id: str) -> list[dict[str, Any]]:
        rows = cur.execute(
            """SELECT i.*, c.name AS candidate_name, c.distance_meters, c.qualifications
               FROM invitations i JOIN candidates c ON c.candidate_id = i.candidate_id
               WHERE i.event_id=?
               ORDER BY i.round, i.sent_at, c.distance_meters ASC, c.candidate_id""",
            (event_id,),
        ).fetchall()
        result = []
        for row in rows:
            item = dict(row)
            item["qualifications"] = json.loads(item.pop("qualifications"))
            result.append(item)
        return result

    def list_open_invitation_ids(self, cur: sqlite3.Connection, event_id: str) -> list[str]:
        rows = cur.execute(
            "SELECT invite_id FROM invitations WHERE event_id=? AND status=?",
            (event_id, INVITE_OPEN),
        ).fetchall()
        return [r["invite_id"] for r in rows]

    def has_open_invitation(self, cur: sqlite3.Connection, event_id: str) -> bool:
        row = cur.execute(
            "SELECT 1 FROM invitations WHERE event_id=? AND status=? LIMIT 1",
            (event_id, INVITE_OPEN),
        ).fetchone()
        return row is not None

    def max_round(self, cur: sqlite3.Connection, event_id: str) -> int:
        row = cur.execute(
            "SELECT COALESCE(MAX(round),0) AS m FROM invitations WHERE event_id=?", (event_id,),
        ).fetchone()
        return int(row["m"])

    def expire_due_invitations(
        self, cur: sqlite3.Connection, event_id: str, now_iso: str, reason: str,
    ) -> list[str]:
        """把已到 TTL 的待响应邀请置为 EXPIRED，返回受影响的 invite_id 列表。"""
        rows = cur.execute(
            """UPDATE invitations SET status=?, decline_reason=?
               WHERE event_id=? AND status=? AND expires_at<=?
               RETURNING invite_id""",
            (INVITE_EXPIRED, reason, event_id, INVITE_OPEN, now_iso),
        ).fetchall()
        return [r["invite_id"] for r in rows]

    def expire_all_open(
        self, cur: sqlite3.Connection, event_id: str, reason: str,
    ) -> list[str]:
        rows = cur.execute(
            """UPDATE invitations SET status=?, decline_reason=?
               WHERE event_id=? AND status=? RETURNING invite_id""",
            (INVITE_EXPIRED, reason, event_id, INVITE_OPEN),
        ).fetchall()
        return [r["invite_id"] for r in rows]

    def supersede_other_open(
        self, cur: sqlite3.Connection, event_id: str, winner_invite_id: str, reason: str,
    ) -> list[str]:
        rows = cur.execute(
            """UPDATE invitations SET status=?, decline_reason=?
               WHERE event_id=? AND status=? AND invite_id<>?
               RETURNING invite_id""",
            (INVITE_SUPERSEDED, reason, event_id, INVITE_OPEN, winner_invite_id),
        ).fetchall()
        return [r["invite_id"] for r in rows]

    def confirm_if_open(
        self, cur: sqlite3.Connection, invite_id: str, responded_at: str, now_iso: str,
    ) -> bool:
        """原子获岗：仅当邀请 OPEN、事件 OPEN 且未过截止时间时确认成功。"""
        cursor = cur.execute(
            """UPDATE invitations SET status=?, responded_at=?
               WHERE invite_id=? AND status=? AND expires_at>?
                 AND (SELECT status FROM relay_events e WHERE e.event_id=invitations.event_id)=?
                 AND (SELECT deadline FROM relay_events e WHERE e.event_id=invitations.event_id)>?""",
            (INVITE_CONFIRMED, responded_at, invite_id, INVITE_OPEN, now_iso, EVENT_OPEN, now_iso),
        )
        return cursor.rowcount == 1

    def decline_if_open(
        self, cur: sqlite3.Connection, invite_id: str, responded_at: str, reason: str,
    ) -> bool:
        cursor = cur.execute(
            """UPDATE invitations SET status=?, responded_at=?, decline_reason=?
               WHERE invite_id=? AND status=?
                 AND (SELECT status FROM relay_events e WHERE e.event_id=invitations.event_id)=?""",
            (INVITE_DECLINED, responded_at, reason, invite_id, INVITE_OPEN, EVENT_OPEN),
        )
        return cursor.rowcount == 1

    # ---- 事务投递箱 -----------------------------------------------------

    def enqueue_outbox(self, cur: sqlite3.Connection, data: dict[str, Any]) -> None:
        cur.execute(
            """INSERT INTO outbox
               (idem_key, event_id, invite_id, target, channel, msg_type, payload,
                status, created_at)
               VALUES (?,?,?,?,?,?,?,?,?)""",
            (
                data["idem_key"], data.get("event_id"), data.get("invite_id"),
                data["target"], data["channel"], data["msg_type"],
                json.dumps(data["payload"], ensure_ascii=False), OUT_PENDING, data["created_at"],
            ),
        )

    def claim_due(self, now_iso: str, stale_after_iso: str, limit: int) -> list[dict[str, Any]]:
        """领取待投递消息：先回收僵死 PROCESSING，再把一批 PENDING 置为 PROCESSING。"""
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                self._conn.execute(
                    "UPDATE outbox SET status=? WHERE status=? AND (locked_at IS NULL OR locked_at<?)",
                    (OUT_PENDING, OUT_PROCESSING, stale_after_iso),
                )
                rows = self._conn.execute(
                    """SELECT * FROM outbox WHERE status=? ORDER BY id LIMIT ?""",
                    (OUT_PENDING, limit),
                ).fetchall()
                ids = [r["id"] for r in rows]
                if ids:
                    placeholders = ",".join("?" * len(ids))
                    self._conn.execute(
                        f"UPDATE outbox SET status=?, locked_at=?, attempts=attempts+1 WHERE id IN ({placeholders})",
                        [OUT_PROCESSING, now_iso, *ids],
                    )
                self._conn.commit()
                return [dict(r) for r in rows]
            except Exception:
                self._conn.rollback()
                raise

    def mark_sent(self, outbox_id: int, sent_at: str) -> None:
        with self._lock:
            self._conn.execute(
                "UPDATE outbox SET status=?, sent_at=?, last_error='' WHERE id=?",
                (OUT_SENT, sent_at, outbox_id),
            )
            self._conn.commit()

    def requeue(self, outbox_id: int, error: str) -> None:
        with self._lock:
            self._conn.execute(
                "UPDATE outbox SET status=?, locked_at=NULL, last_error=? WHERE id=?",
                (OUT_PENDING, error[:500], outbox_id),
            )
            self._conn.commit()

    def list_outbox(
        self, cur: sqlite3.Connection, status: str | None = None,
        event_id: str | None = None,
    ) -> list[dict[str, Any]]:
        sql = "SELECT * FROM outbox"
        clauses: list[str] = []
        params: list[Any] = []
        if status:
            clauses.append("status=?")
            params.append(status)
        if event_id:
            clauses.append("event_id=?")
            params.append(event_id)
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        sql += " ORDER BY id"
        rows = cur.execute(sql, params).fetchall()
        items = []
        for row in rows:
            item = dict(row)
            item["payload"] = json.loads(item["payload"])
            items.append(item)
        return items

    def pending_count(self, cur: sqlite3.Connection, event_id: str | None = None) -> int:
        if event_id:
            row = cur.execute(
                "SELECT COUNT(*) AS n FROM outbox WHERE status<>? AND event_id=?",
                (OUT_SENT, event_id),
            ).fetchone()
        else:
            row = cur.execute(
                "SELECT COUNT(*) AS n FROM outbox WHERE status<>?", (OUT_SENT,),
            ).fetchone()
        return int(row["n"])
