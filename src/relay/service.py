"""接力领域服务。

把存储、筛选、时钟、投递箱组合成确定性的接力流程：

- 创建事件时即按资格、距离、连续服务限制完成候选排序（筛选过程留痕）。
- 分批邀请：每批 ``batch_size`` 人；一批**全部落定**（拒绝或超时）后才发下一批，
  避免手工群花式的多人同时到场；同一批内的并发确认由数据库串行事务仲裁，
  **首个有效确认原子获岗**，其余邀请在同一事务内失效。
- 候选拒绝：邀请置 declined，推进批次。
- 超时：``expires_at`` 到期由巡检（或任意请求触发的顺手扫描）确定性失效。
- 原讲解员复岗：事件关闭、所有在途邀请 revoked 并通知。
- 进程重启：状态全部在 SQLite 中，启动即执行一次扫描，超时/下一批/失败结论
  都被补齐；投递箱未发送的消息继续投递。
"""
from __future__ import annotations

import json
import uuid
from typing import Any

from .clock import Clock, SystemClock, now_iso, parse_iso
from .eligibility import effective_distance as eligibility_effective_distance
from .eligibility import rank_candidates
from .store import Store

# 事件状态
OPEN = "open"            # 接力进行中（可能有在途邀请，也可能正在等待下一批）
RESOLVED = "resolved"    # 已由替补确认获岗
FAILED = "failed"        # 截止时间到达且候选耗尽，无人接岗
RECOVERED = "recovered"  # 原讲解员复岗，接力终止

CLOSED_STATUSES = {RESOLVED, FAILED, RECOVERED}

# 邀请状态
PENDING = "pending"
ACCEPTED = "accepted"
DECLINED = "declined"
EXPIRED = "expired"
SUPERSEDED = "superseded"  # 已有人获岗
REVOKED = "revoked"        # 原讲解员复岗

SETTLED = {ACCEPTED, DECLINED, EXPIRED, SUPERSEDED, REVOKED}


class RelayError(Exception):
    """带稳定错误码的领域异常，HTTP 层据此映射状态码。"""

    def __init__(self, code: str, message: str, status: int = 400) -> None:
        super().__init__(message)
        self.code = code
        self.status = status


def _new_id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:16]}"


def _notify_target(candidate_row) -> tuple[str, str]:
    """通知目的地：未成年候选（有监护人联系方式）发给监护人，否则发给本人。"""
    if candidate_row["guardian_contact"]:
        return candidate_row["guardian_contact"], "guardian_sms"
    return candidate_row["contact"] or candidate_row["id"], "sms"


class RelayService:
    def __init__(self, store: Store, clock: Clock | None = None) -> None:
        self._store = store
        self._clock = clock or SystemClock()

    # =================================================================== 候选

    def upsert_candidate(self, data: dict[str, Any]) -> dict[str, Any]:
        self._validate_candidate(data)
        data = dict(data)
        data.setdefault("distance_m", 0.0)
        self._store.upsert_candidate(data)
        return self.get_candidate(data["id"])

    def _validate_candidate(self, c: dict[str, Any]) -> None:
        if not c.get("id") or not isinstance(c["id"], str):
            raise RelayError("invalid_candidate", "候选缺少 id")
        if not c.get("name"):
            raise RelayError("invalid_candidate", "候选缺少 name")
        dist = c.get("distance_m", 0)
        if not isinstance(dist, (int, float)) or dist < 0:
            raise RelayError("invalid_candidate", "distance_m 必须是非负数字")
        if c.get("qualifications") is not None and not isinstance(
            c["qualifications"], list
        ):
            raise RelayError("invalid_candidate", "qualifications 必须是列表")
        shifts = c.get("consecutive_shifts", 0)
        limit = c.get("max_consecutive_shifts", 6)
        if not isinstance(shifts, int) or shifts < 0:
            raise RelayError("invalid_candidate", "consecutive_shifts 必须是非负整数")
        if not isinstance(limit, int) or limit <= 0:
            raise RelayError("invalid_candidate", "max_consecutive_shifts 必须是正整数")

    def get_candidate(self, candidate_id: str) -> dict[str, Any]:
        row = self._store.query_one("SELECT * FROM candidates WHERE id=?", (candidate_id,))
        if row is None:
            raise RelayError("candidate_not_found", f"候选不存在：{candidate_id}", 404)
        return self._store._candidate_dict(row)

    def list_candidates(self) -> list[dict[str, Any]]:
        return self._store.list_candidates()

    # ================================================================ 事件创建

    def create_event(self, req: dict[str, Any]) -> dict[str, Any]:
        """创建带截止时间的接力事件并立即发出第一批邀请。"""
        activity = req.get("activity")
        if not activity:
            raise RelayError("invalid_event", "缺少 activity")
        assignee_id = req.get("assignee_id")  # 缺岗的原讲解员（可空）
        starts_at = self._require_time(req, "starts_at")
        deadline = self._require_time(req, "deadline")
        now = self._clock.now()
        if deadline <= now:
            raise RelayError("invalid_deadline", "截止时间必须晚于当前时间")
        if starts_at <= now:
            raise RelayError("invalid_starts_at", "活动开始时间必须晚于当前时间")
        if deadline > starts_at:
            raise RelayError("invalid_deadline", "邀请截止时间不能晚于活动开始时间")

        batch_size = int(req.get("batch_size", 3))
        if batch_size <= 0:
            raise RelayError("invalid_batch_size", "batch_size 必须为正整数")
        invite_timeout = float(req.get("invite_timeout_seconds", 300))
        batch_gap = float(req.get("batch_gap_seconds", 0))
        if invite_timeout <= 0:
            raise RelayError("invalid_invite_timeout", "邀请超时必须为正数")
        if batch_gap < 0:
            raise RelayError("invalid_batch_gap", "批间隔不能为负")

        qualifications = list(req.get("required_qualifications", []))
        max_distance = req.get("max_distance_m")
        if max_distance is not None:
            max_distance = float(max_distance)
            if max_distance <= 0:
                raise RelayError("invalid_distance", "max_distance_m 必须为正数")
        location = req.get("location")

        event_id = _new_id("evt")
        with self._store.tx() as conn:
            self._store.insert_event(
                conn,
                {
                    "id": event_id,
                    "activity": activity,
                    "assignee_id": assignee_id,
                    "starts_at": now_iso(starts_at),
                    "deadline": now_iso(deadline),
                    "batch_size": batch_size,
                    "invite_timeout_seconds": invite_timeout,
                    "batch_gap_seconds": batch_gap,
                    "required_qualifications": qualifications,
                    "max_distance_m": max_distance,
                    "location": location,
                    "status": OPEN,
                    "created_at": now_iso(now),
                },
            )
            self._advance_locked(conn, event_id)
        return self.get_event(event_id)

    @staticmethod
    def _require_time(req: dict[str, Any], key: str):
        value = req.get(key)
        if not value:
            raise RelayError("invalid_event", f"缺少 {key}")
        try:
            return parse_iso(value)
        except ValueError as exc:
            raise RelayError("invalid_event", str(exc)) from exc

    # ============================================================ 接力状态推进

    def _event_row_locked(self, conn, event_id: str):
        row = conn.execute("SELECT * FROM relay_events WHERE id=?", (event_id,)).fetchone()
        if row is None:
            raise RelayError("event_not_found", f"接力事件不存在：{event_id}", 404)
        return row

    def _invitation_rows_locked(self, conn, event_id: str):
        return conn.execute(
            "SELECT * FROM invitations WHERE event_id=? ORDER BY wave, rank", (event_id,)
        ).fetchall()

    def _advance_locked(self, conn, event_id: str) -> bool:
        """在已持有的事务内推进事件。返回是否发生了状态变化。

        调用点：创建、响应、复岗、巡检（tick）。所有写互斥，故此处读到的
        "事件是否仍 open / 邀请是否仍 pending" 到提交之间不会被他人改动。
        """
        event = self._event_row_locked(conn, event_id)
        if event["status"] in CLOSED_STATUSES:
            return False
        now = self._clock.now()
        now_text = now_iso(now)
        deadline_text = event["deadline"]
        changed = False

        invitations = self._invitation_rows_locked(conn, event_id)
        pending_rows = [r for r in invitations if r["status"] == PENDING]

        # 1) 到期邀请确定性失效（expires_at 已过；deadline 也是硬上限）。
        for r in pending_rows:
            if r["expires_at"] <= now_text or deadline_text <= now_text:
                conn.execute(
                    "UPDATE invitations SET status='expired', outcome='timeout', responded_at=? "
                    "WHERE id=? AND status='pending'",
                    (now_text, r["id"]),
                )
                changed = True
        if changed:
            invitations = self._invitation_rows_locked(conn, event_id)
            pending_rows = [r for r in invitations if r["status"] == PENDING]

        # 2) 还有在途邀请：等待响应，不开新批。
        if pending_rows:
            return changed

        # 3) 已过截止时间：以失败收口（无人接岗），通知运营员。
        if deadline_text <= now_text:
            conn.execute(
                "UPDATE relay_events SET status='failed', close_reason='deadline_reached', "
                "closed_at=? WHERE id=?",
                (now_text, event_id),
            )
            self._store.outbox_add(
                conn,
                [
                    self._message(
                        target="operator",
                        kind="expired_event",
                        ref_type="event",
                        ref_id=event_id,
                        text=f"活动「{event['activity']}」邀请截止，暂无志愿者接岗，请人工处理。",
                        created_at=now_text,
                    )
                ],
            )
            return True

        # 4) 发出下一批：重新筛选（候选状态可能变化），排除已邀请过的人。
        batch, trace = self._select_next_batch_locked(conn, event, invitations)
        if not batch:
            # 候选耗尽且未到截止时间——不提前判死：等到截止再 failed。
            # 运营员可实时看到"无更多合格候选"，并可补充候选后由巡检再次推进。
            return changed

        next_wave = (max((r["wave"] for r in invitations), default=-1)) + 1
        # 记录本批评选留痕（合格名次 / 不合格原因）。
        for item in trace:
            conn.execute(
                """
                INSERT OR REPLACE INTO screening_trace
                    (event_id, wave, candidate_id, rank, eligible, reason, distance_m, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    event_id,
                    next_wave,
                    item["candidate_id"],
                    item.get("rank"),
                    1 if item["eligible"] else 0,
                    item.get("reason"),
                    item.get("distance_m"),
                    now_text,
                ),
            )
        timeout = float(event["invite_timeout_seconds"])
        batch_gap = float(event["batch_gap_seconds"])
        # 下一批最早邀请时间 = 上一批最后落定时间 + gap。
        if invitations and batch_gap > 0:
            last_settled = max(
                parse_iso(r["responded_at"] or r["invited_at"])
                for r in invitations
                if r["status"] in SETTLED
            )
            earliest = last_settled.timestamp() + batch_gap
            if now.timestamp() < earliest:
                return changed  # 批间隔未到，巡检稍后重试

        hard_expiry = min(now.timestamp() + timeout, parse_iso(deadline_text).timestamp())
        expires_text = now_iso(now.fromtimestamp(hard_expiry))
        messages: list[dict[str, Any]] = []
        for rank, cand in enumerate(batch):
            inv_id = _new_id("inv")
            conn.execute(
                """
                INSERT INTO invitations(id, event_id, candidate_id, wave, rank, status,
                                        invited_at, expires_at)
                VALUES (?, ?, ?, ?, ?, 'pending', ?, ?)
                """,
                (inv_id, event_id, cand["id"], next_wave, rank, now_text, expires_text),
            )
            target, channel = _notify_target(cand)
            text = (
                f"紧急缺岗接力：活动「{event['activity']}」需要讲解员，"
                f"请于 {expires_text} 前确认（邀请 {inv_id}）。"
            )
            if channel == "guardian_sms":
                text = f"志愿者 {cand['name']} 收到紧急讲解岗邀请，请监护人代为确认。" + text
            messages.append(
                self._message(
                    target=target,
                    channel=channel,
                    kind="invitation",
                    ref_type="invitation",
                    ref_id=inv_id,
                    text=text,
                    created_at=now_text,
                    invitation_id=inv_id,
                    event_id=event_id,
                )
            )
        self._store.outbox_add(conn, messages)
        return True

    def _select_next_batch_locked(
        self, conn, event, invitations
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """按资格/距离/连续服务限制筛选下一批候选。

        返回（本批候选, 评选留痕）。已邀请者不参与后续批次，也不重复留痕。
        """
        invited_ids = {r["candidate_id"] for r in invitations}
        rows = conn.execute("SELECT * FROM candidates ORDER BY id").fetchall()
        candidates = [self._store._candidate_dict(r) for r in rows]
        filters = self._event_filters(event)
        ranked, rejected = rank_candidates(
            candidates,
            required_qualifications=filters["required_qualifications"],
            max_distance_m=filters["max_distance_m"],
            location=filters["location"],
            exclude_ids=invited_ids,
        )
        trace: list[dict[str, Any]] = [
            {
                "candidate_id": item.candidate_id,
                "rank": rank,
                "eligible": True,
                "reason": None,
                "distance_m": item.distance_m,
            }
            for rank, item in enumerate(ranked)
        ]
        # 不合格者补充距离用于展示
        dist_by_id = {
            c["id"]: eligibility_effective_distance(c, filters["location"]) for c in candidates
        }
        for item in rejected:
            trace.append(
                {
                    "candidate_id": item["candidate_id"],
                    "rank": None,
                    "eligible": False,
                    "reason": item["reason"],
                    "distance_m": dist_by_id.get(item["candidate_id"]),
                }
            )
        chosen_ids = [item.candidate_id for item in ranked[: event["batch_size"]]]
        by_id = {c["id"]: c for c in candidates}
        return [by_id[cid] for cid in chosen_ids if cid in by_id], trace

    @staticmethod
    def _event_filters(event) -> dict[str, Any]:
        """从原始事件行读取筛选条件（JSON 字段在此解析）。"""
        return {
            "required_qualifications": json.loads(event["required_qualifications"]),
            "max_distance_m": event["max_distance_m"],
            "location": json.loads(event["location"]) if event["location"] else None,
        }

    # =================================================================== 响应

    def respond(self, invitation_id: str, action: str) -> dict[str, Any]:
        """候选对邀请作出 ``accept`` / ``decline`` 响应。"""
        if action not in ("accept", "decline"):
            raise RelayError("invalid_action", "action 必须是 accept 或 decline")
        terminal_error: RelayError | None = None
        with self._store.tx() as conn:
            inv = conn.execute(
                "SELECT * FROM invitations WHERE id=?", (invitation_id,)
            ).fetchone()
            if inv is None:
                raise RelayError("invitation_not_found", f"邀请不存在：{invitation_id}", 404)
            event = self._event_row_locked(conn, inv["event_id"])
            now_text = now_iso(self._clock.now())

            if inv["status"] != PENDING:
                # 获胜事务已把其余在途邀请原子失效（superseded/revoked/expired）。
                if event["status"] in CLOSED_STATUSES:
                    raise RelayError(
                        "event_closed",
                        f"接力已结束（邀请状态：{inv['status']}）",
                        409,
                    )
                raise RelayError(
                    "invitation_not_pending",
                    f"邀请已结束，当前状态：{inv['status']}",
                    409,
                )
            if event["status"] in CLOSED_STATUSES:
                # 事件已收口（并发响应落败 / 已复岗）：把迟到的在途邀请确定性落定。
                settle_status, settle_outcome = (
                    (REVOKED, "recovered")
                    if event["status"] == RECOVERED
                    else (SUPERSEDED, "superseded")
                )
                conn.execute(
                    "UPDATE invitations SET status=?, outcome=?, responded_at=? WHERE id=?",
                    (settle_status, settle_outcome, now_text, invitation_id),
                )
                terminal_error = RelayError(
                    "event_closed", f"接力已结束：{event['status']}", 409
                )
            elif inv["expires_at"] <= now_text or event["deadline"] <= now_text:
                # 并发边界：响应与超时巡检赛跑，事务内以时间为准确定性落为失效。
                conn.execute(
                    "UPDATE invitations SET status='expired', outcome='timeout', responded_at=? "
                    "WHERE id=?",
                    (now_text, invitation_id),
                )
                self._advance_locked(conn, event["id"])
                terminal_error = RelayError("invitation_expired", "邀请已超时失效", 409)
            elif action == "decline":
                conn.execute(
                    "UPDATE invitations SET status='declined', outcome='declined', responded_at=? "
                    "WHERE id=?",
                    (now_text, invitation_id),
                )
                self._advance_locked(conn, event["id"])
            else:
                self._accept_locked(conn, event, inv, now_text)
            event_id = event["id"]
        if terminal_error is not None:
            raise terminal_error
        return self.get_event(event_id)

    def _accept_locked(self, conn, event, inv, now_text: str) -> None:
        """首个有效确认原子获岗：在同一串行事务内收口整个事件。"""
        winner = conn.execute("SELECT * FROM candidates WHERE id=?", (inv["candidate_id"],)).fetchone()
        # 1) 获胜邀请
        conn.execute(
            "UPDATE invitations SET status='accepted', outcome='won', responded_at=? WHERE id=?",
            (now_text, inv["id"]),
        )
        # 2) 事件收口到该候选
        conn.execute(
            """
            UPDATE relay_events SET status='resolved', winner_invitation_id=?,
                                    assignee_id=?, close_reason=NULL, closed_at=?
            WHERE id=? AND status='open'
            """,
            (inv["id"], inv["candidate_id"], now_text, event["id"]),
        )
        # 3) 其余所有在途邀请原子失效，并通知落选者
        others = conn.execute(
            "SELECT * FROM invitations WHERE event_id=? AND status='pending'",
            (event["id"],),
        ).fetchall()
        messages: list[dict[str, Any]] = []
        for r in others:
            conn.execute(
                "UPDATE invitations SET status='superseded', outcome='superseded', responded_at=? "
                "WHERE id=?",
                (now_text, r["id"]),
            )
            cand = conn.execute("SELECT * FROM candidates WHERE id=?", (r["candidate_id"],)).fetchone()
            target, channel = _notify_target(cand)
            messages.append(
                self._message(
                    target=target,
                    channel=channel,
                    kind="lost",
                    ref_type="invitation",
                    ref_id=r["id"],
                    text=f"活动「{event['activity']}」已由其他志愿者首先确认，感谢支持。",
                    created_at=now_text,
                )
            )
        # 4) 获胜者连续服务班次 +1（影响后续事件的连续服务限制）
        conn.execute(
            "UPDATE candidates SET consecutive_shifts=consecutive_shifts+1 WHERE id=?",
            (inv["candidate_id"],),
        )
        # 5) 通知获胜者与运营员
        win_target, win_channel = _notify_target(winner)
        messages.append(
            self._message(
                target=win_target,
                channel=win_channel,
                kind="won",
                ref_type="invitation",
                ref_id=inv["id"],
                text=f"您已确认接岗活动「{event['activity']}」，开始时间 {event['starts_at']}，请准时到场。",
                created_at=now_text,
            )
        )
        messages.append(
            self._message(
                target="operator",
                kind="won",
                ref_type="event",
                ref_id=event["id"],
                text=f"活动「{event['activity']}」已由 {winner['name']}（{winner['id']}）确认接岗。",
                created_at=now_text,
            )
        )
        self._store.outbox_add(conn, messages)

    # =================================================================== 复岗

    def recover(self, event_id: str, note: str | None = None) -> dict[str, Any]:
        """原讲解员复岗：终止接力，撤销全部在途邀请并通知。"""
        with self._store.tx() as conn:
            event = self._event_row_locked(conn, event_id)
            now_text = now_iso(self._clock.now())
            if event["status"] in CLOSED_STATUSES:
                raise RelayError(
                    "event_closed", f"接力已结束：{event['status']}", 409
                )
            pending = conn.execute(
                "SELECT * FROM invitations WHERE event_id=? AND status='pending'",
                (event_id,),
            ).fetchall()
            messages: list[dict[str, Any]] = []
            for r in pending:
                conn.execute(
                    "UPDATE invitations SET status='revoked', outcome='recovered', responded_at=? "
                    "WHERE id=?",
                    (now_text, r["id"]),
                )
                cand = conn.execute("SELECT * FROM candidates WHERE id=?", (r["candidate_id"],)).fetchone()
                target, channel = _notify_target(cand)
                messages.append(
                    self._message(
                        target=target,
                        channel=channel,
                        kind="recovered",
                        ref_type="invitation",
                        ref_id=r["id"],
                        text=f"活动「{event['activity']}」原讲解员已复岗，本次邀请取消，感谢支持。",
                        created_at=now_text,
                    )
                )
            conn.execute(
                "UPDATE relay_events SET status='recovered', close_reason=?, closed_at=? WHERE id=?",
                (note or "assignee_returned", now_text, event_id),
            )
            messages.append(
                self._message(
                    target="operator",
                    kind="recovered",
                    ref_type="event",
                    ref_id=event_id,
                    text=f"活动「{event['activity']}」原讲解员已复岗，接力终止。",
                    created_at=now_text,
                )
            )
            self._store.outbox_add(conn, messages)
        return self.get_event(event_id)

    # =================================================================== 巡检

    def tick(self) -> int:
        """扫描全部进行中事件，推进超时/下一批/失败收口。返回发生变化的事件数。

        进程启动时立即执行一次（重启恢复），之后由后台线程周期执行；
        HTTP 请求路径自身也在事务内推进，因此即使巡检间隔较长结果依然确定。
        """
        changed_count = 0
        for row in self._store.list_open_event_rows():
            with self._store.tx() as conn:
                if self._advance_locked(conn, row["id"]):
                    changed_count += 1
        return changed_count

    # =================================================================== 查询

    def list_events(self, limit: int = 100) -> list[dict[str, Any]]:
        return [self._chain(self._store.event_dict(r)) for r in self._store.list_event_rows(limit)]

    def get_event(self, event_id: str) -> dict[str, Any]:
        row = self._store.get_event_row(event_id)
        if row is None:
            raise RelayError("event_not_found", f"接力事件不存在：{event_id}", 404)
        return self._chain(self._store.event_dict(row))

    def _chain(self, event: dict[str, Any]) -> dict[str, Any]:
        """组装实时接力链视图：事件 + 分批邀请链 + 最终安排 + 筛选留痕。"""
        inv_rows = self._store.list_invitation_rows(event["id"])
        candidates = {c["id"]: c for c in self._store.list_candidates()}
        waves: dict[int, list] = {}
        for r in inv_rows:
            inv = self._store.invitation_dict(r)
            cand = candidates.get(inv["candidate_id"], {})
            inv["candidate_name"] = cand.get("name")
            inv["distance_m"] = cand.get("distance_m")
            inv["contact"] = cand.get("contact")
            waves.setdefault(inv["wave"], []).append(inv)
        chain = [
            {"wave": wave, "invitations": sorted(items, key=lambda x: x["rank"])}
            for wave, items in sorted(waves.items())
        ]
        screening: dict[int, list] = {}
        for r in self._store.list_screening_rows(event["id"]):
            screening.setdefault(r["wave"], []).append(
                {
                    "candidate_id": r["candidate_id"],
                    "candidate_name": candidates.get(r["candidate_id"], {}).get("name"),
                    "rank": r["rank"],
                    "eligible": bool(r["eligible"]),
                    "reason": r["reason"],
                    "distance_m": r["distance_m"],
                }
            )
        screening_by_wave = [
            {"wave": wave, "results": items}
            for wave, items in sorted(screening.items())
        ]
        winner = None
        if event["status"] == RESOLVED and event["winner_invitation_id"]:
            win_row = self._store.get_invitation_row(event["winner_invitation_id"])
            if win_row:
                cand = candidates.get(win_row["candidate_id"], {})
                winner = {
                    "candidate_id": win_row["candidate_id"],
                    "name": cand.get("name"),
                    "contact": cand.get("contact"),
                    "invitation_id": win_row["id"],
                    "confirmed_at": win_row["responded_at"],
                }
        pending_count = sum(1 for r in inv_rows if r["status"] == PENDING)
        return {
            "event": event,
            "chain": chain,
            "screening": screening_by_wave,
            "pending_invitation_count": pending_count,
            "final_assignment": winner,
            "final_status": event["status"],
        }

    def get_invitation(self, invitation_id: str) -> dict[str, Any]:
        row = self._store.get_invitation_row(invitation_id)
        if row is None:
            raise RelayError("invitation_not_found", f"邀请不存在：{invitation_id}", 404)
        return self._store.invitation_dict(row)

    def list_outbox(self, limit: int = 100) -> list[dict[str, Any]]:
        """投递箱内容（含状态/尝试次数），供运营核对与排障。"""
        rows = self._store.query(
            "SELECT id,target,channel,kind,ref_type,ref_id,payload,created_at,"
            "attempts,state,claimed_at,sent_at FROM outbox ORDER BY id DESC LIMIT ?",
            (limit,),
        )
        return [dict(r) for r in rows]

    def wait_change(self, timeout: float | None = None) -> None:
        """等待一次提交广播（SSE 用）。"""
        self._store.wait_change(timeout)

    def now_iso(self) -> str:
        return self._clock.now_iso()

    # ================================================================ 通知工具

    @staticmethod
    def _message(
        *,
        target: str,
        kind: str,
        ref_type: str,
        ref_id: str,
        text: str,
        created_at: str,
        channel: str = "sms",
        **extra: Any,
    ) -> dict[str, Any]:
        payload = {"text": text, **extra}
        return {
            "target": target,
            "channel": channel,
            "kind": kind,
            "ref_type": ref_type,
            "ref_id": ref_id,
            "payload": payload,
            "created_at": created_at,
        }
