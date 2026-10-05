"""紧急缺岗接力领域引擎。

核心规则：
1. 运营员创建带截止时间的接力事件；
2. 按「资格 → 距离 → 连续服务限制」筛选候选，分批发送邀请；
3. 首个有效确认在同一事务内原子获岗，其余待响应邀请自动失效；
4. 拒绝 / 超时 / 原讲解员复岗 / 进程重启均有确定处理；
5. 所有通知与状态变更在同一事务写入事务投递箱（transactional outbox）。
"""
from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Iterable

from .clock import Clock, SystemClock, parse_ts, to_iso
from .store import (
    EVENT_CANCELLED_RESTORED, EVENT_EXPIRED, EVENT_FILLED, EVENT_OPEN,
    INVITE_CONFIRMED, INVITE_DECLINED, INVITE_EXPIRED, INVITE_OPEN, INVITE_SUPERSEDED,
    NotFound, Store,
)

# 连续服务时长上限：候选资料中的 last_service_end 表示刚结束一段达此上限的服务
MAX_CONSECUTIVE_SERVICE = timedelta(hours=4)
# 达上限后再次可派岗前的强制休息间隔
REQUIRED_REST_GAP = timedelta(minutes=30)


class DomainError(Exception):
    """可预期的业务冲突（HTTP 409/422），消息面向调用方。"""


@dataclass(frozen=True)
class RelayConfig:
    batch_size: int = 3
    invite_ttl_seconds: float = 900.0  # 单批邀请的响应时限
    sweep_interval_seconds: float = 5.0


def _new_id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:16]}"


class RelayEngine:
    """无内存状态的领域服务：一切以数据库为准，进程重启即恢复。"""

    def __init__(
        self,
        store: Store,
        clock: Clock | None = None,
        config: RelayConfig | None = None,
        default_channel: str = "sms",
    ) -> None:
        self.store = store
        self.clock = clock or SystemClock()
        self.config = config or RelayConfig()
        self.default_channel = default_channel

    # ------------------------------------------------------------------ #
    # 候选管理
    # ------------------------------------------------------------------ #

    def register_candidate(
        self,
        candidate_id: str,
        name: str,
        qualifications: Iterable[str],
        distance_meters: float,
        last_service_end: str | datetime | None = None,
        contact: str = "",
    ) -> dict[str, Any]:
        if distance_meters < 0:
            raise DomainError("距离不能为负")
        quals = sorted(set(qualifications))
        if not quals:
            raise DomainError("候选至少需要一项资格")
        now_iso = to_iso(self.clock.now())
        data = {
            "candidate_id": candidate_id, "name": name, "contact": contact or candidate_id,
            "qualifications": quals, "distance_meters": float(distance_meters),
            "last_service_end": to_iso(parse_ts(last_service_end)) if last_service_end else None,
            "created_at": now_iso,
        }
        with self.store.begin() as cur:
            self.store.upsert_candidate(cur, data)
        return {k: v for k, v in data.items() if k != "created_at"}

    def list_candidates(self) -> list[dict[str, Any]]:
        with self.store.begin() as cur:
            return self.store.list_candidates(cur)

    # ------------------------------------------------------------------ #
    # 事件创建
    # ------------------------------------------------------------------ #

    def create_event(
        self,
        shift_label: str,
        original_guide_id: str,
        event_starts_at: str | datetime,
        deadline: str | datetime,
        required_qualifications: Iterable[str] | None = None,
        batch_size: int | None = None,
        invite_ttl_seconds: float | None = None,
        event_id: str | None = None,
        auto_dispatch: bool = True,
    ) -> dict[str, Any]:
        now = self.clock.now()
        starts = parse_ts(event_starts_at)
        dl = parse_ts(deadline)
        if dl <= now:
            raise DomainError("截止时间必须晚于当前时间")
        if dl > starts:
            raise DomainError("截止时间不能晚于活动开始时间")
        size = int(batch_size or self.config.batch_size)
        ttl = float(invite_ttl_seconds or self.config.invite_ttl_seconds)
        if size < 1:
            raise DomainError("每批人数至少为 1")
        if ttl <= 0:
            raise DomainError("邀请时限必须为正")
        eid = event_id or _new_id("evt")
        data = {
            "event_id": eid, "shift_label": shift_label,
            "original_guide_id": original_guide_id,
            "event_starts_at": to_iso(starts), "deadline": to_iso(dl),
            "required_qualifications": sorted(set(required_qualifications or [])),
            "batch_size": size, "invite_ttl_seconds": ttl,
            "created_at": to_iso(now),
        }
        with self.store.begin() as cur:
            if self.store.find_event(cur, eid) is not None:
                raise DomainError(f"接力事件已存在：{eid}")
            self.store.insert_event(cur, data)
            self._enqueue(
                cur, event_id=eid, target=original_guide_id,
                msg_type="EVENT_OPENED",
                payload={"event_id": eid, "shift_label": shift_label, "deadline": data["deadline"]},
            )
        if auto_dispatch:
            self.sweep_event(eid)
        return self.get_event(eid)

    # ------------------------------------------------------------------ #
    # 候选筛选与分批邀请
    # ------------------------------------------------------------------ #

    def _eligible_candidates(
        self, cur, event: dict[str, Any], now: datetime,
    ) -> list[dict[str, Any]]:
        """资格 ⊇ 岗位要求，且不触碰连续服务限制；按距离升序。"""
        required = set(event["required_qualifications"])
        pool = self.store.list_uninvited_candidates(cur, event["event_id"])
        eligible = [
            cand for cand in pool
            if required.issubset(set(cand["qualifications"]))
            and not self._service_restricted(cand, now)
        ]
        return eligible

    @staticmethod
    def _service_restricted(cand: dict[str, Any], now: datetime) -> bool:
        """连续服务限制：刚结束一段达上限的连续服务、尚在强制休息间隔内的候选排除。

        ``last_service_end`` 记录最近一段已满 MAX_CONSECUTIVE_SERVICE 的连续服务
        结束时刻；距今不足 REQUIRED_REST_GAP 即视为连续服务受限。
        """
        raw = cand.get("last_service_end")
        if not raw:
            return False
        end = parse_ts(raw)
        if end > now:  # 资料显示仍在服务中（或时钟数据异常），不可再排
            return True
        return now - end < REQUIRED_REST_GAP

    def _dispatch_next_batch(self, cur, event: dict[str, Any], now: datetime) -> int:
        """前提：事件 OPEN、当前无待响应邀请。发送下一批，返回发出数量。"""
        eligible = self._eligible_candidates(cur, event, now)
        take = eligible[: event["batch_size"]]
        round_no = self.store.max_round(cur, event["event_id"]) + 1
        sent_at = to_iso(now)
        expires_at = to_iso(now + timedelta(seconds=event["invite_ttl_seconds"]))
        for cand in take:
            invite_id = _new_id("inv")
            self.store.insert_invitation(cur, {
                "invite_id": invite_id, "event_id": event["event_id"],
                "candidate_id": cand["candidate_id"], "round": round_no,
                "sent_at": sent_at, "expires_at": expires_at,
            })
            self._enqueue(
                cur, event_id=event["event_id"], invite_id=invite_id,
                target=cand["contact"], channel="sms", msg_type="INVITATION",
                payload={
                    "event_id": event["event_id"], "invite_id": invite_id,
                    "shift_label": event["shift_label"], "round": round_no,
                    "candidate_id": cand["candidate_id"],
                    "expires_at": expires_at, "deadline": event["deadline"],
                },
            )
        return len(take)

    # ------------------------------------------------------------------ #
    # 确定性状态推进（超时 / 批次轮换 / 截止）
    # ------------------------------------------------------------------ #

    def _settle(self, cur, event: dict[str, Any], now: datetime) -> dict[str, Any]:
        """在写事务内推进单个事件到确定状态（幂等）。

        顺序：已终态 → 截止时间到（EXPIRED）→ TTL 到期邀请失效并通知
              → 有待响应邀请则等待 → 发下一批 → 无候选则 EXPIRED。
        """
        if event["status"] != EVENT_OPEN:
            return event
        now_iso = to_iso(now)

        if now_iso >= event["deadline"]:
            expired = self.store.expire_all_open(cur, event["event_id"], reason="EVENT_DEADLINE")
            self._notify_batch_end(cur, event, expired, "INVITE_EXPIRED_DEADLINE")
            self.store.mark_event_terminal(
                cur, event["event_id"], EVENT_EXPIRED, now_iso, note="超过接力截止时间仍无人确认",
            )
            self._enqueue(
                cur, event_id=event["event_id"], target=event["original_guide_id"],
                msg_type="EVENT_EXPIRED",
                payload={"event_id": event["event_id"], "deadline": event["deadline"]},
            )
            return self.store.get_event(cur, event["event_id"])

        due = self.store.expire_due_invitations(
            cur, event["event_id"], now_iso, reason="INVITE_TTL",
        )
        if due:
            self._notify_batch_end(cur, event, due, "INVITE_EXPIRED_TTL")

        if self.store.has_open_invitation(cur, event["event_id"]):
            return event  # 本批仍有人未响应，等待确认/拒绝/超时

        sent = self._dispatch_next_batch(cur, event, now)
        if sent == 0:
            self.store.mark_event_terminal(
                cur, event["event_id"], EVENT_EXPIRED, now_iso,
                note="候选池已耗尽且无人确认",
            )
            self._enqueue(
                cur, event_id=event["event_id"], target=event["original_guide_id"],
                msg_type="EVENT_EXPIRED",
                payload={"event_id": event["event_id"], "reason": "CANDIDATES_EXHAUSTED"},
            )
        return self.store.get_event(cur, event["event_id"])

    def sweep_event(self, event_id: str) -> dict[str, Any]:
        """对单个事件做一次确定性推进（惰性触发：读、写、后台线程都会调）。"""
        with self.store.begin() as cur:
            event = self.store.get_event(cur, event_id)
            event = self._settle(cur, event, self.clock.now())
        return self._event_view(event_id)

    def sweep_all(self) -> int:
        """推进所有未决事件，返回处理数量（后台线程 / 进程重启恢复入口）。"""
        with self.store.begin() as cur:
            events = self.store.list_open_events(cur)
            ids = [e["event_id"] for e in events]
        for eid in ids:
            self.sweep_event(eid)
        return len(ids)

    # ------------------------------------------------------------------ #
    # 候选响应：确认 / 拒绝
    # ------------------------------------------------------------------ #

    def confirm(self, invite_id: str, candidate_id: str | None = None) -> dict[str, Any]:
        """首个有效确认原子获岗；重复/迟到/他人确认一律拒绝。"""
        now = self.clock.now()
        now_iso = to_iso(now)
        # 第一事务：确认邀请存在并先做 TTL/截止推进（迟到确认必须被确定性拒绝），
        # 推进结果独立提交，不随后续确认失败而回滚。
        with self.store.begin() as cur:
            invite = self.store.get_invitation(cur, invite_id)
            if invite is None:
                raise NotFound(f"邀请不存在：{invite_id}")
            event_id = invite["event_id"]
        self.sweep_event(event_id)

        # 第二事务：串行化下的原子获岗
        with self.store.begin() as cur:
            invite = self.store.get_invitation(cur, invite_id)
            event = self.store.get_event(cur, event_id)
            if candidate_id is not None and invite["candidate_id"] != candidate_id:
                raise DomainError("该邀请不属于此候选")
            if event["status"] != EVENT_OPEN:
                raise DomainError(f"接力已结束（{event['status']}），确认无效")
            if invite["status"] == INVITE_CONFIRMED:
                raise DomainError("请勿重复确认")
            if invite["status"] == INVITE_SUPERSEDED:
                raise DomainError("岗位已由其他候选确认，邀请已失效")
            if invite["status"] in (INVITE_EXPIRED, INVITE_DECLINED):
                raise DomainError(
                    "邀请已超时失效" if invite["status"] == INVITE_EXPIRED else "邀请已被拒绝"
                )

            ok = self.store.confirm_if_open(cur, invite_id, now_iso, now_iso)
            if not ok:
                raise DomainError("确认竞态失败：邀请已失效或接力已关闭")

            self.store.supersede_other_open(
                cur, event_id, invite_id, reason="WINNER_CONFIRMED",
            )
            self.store.mark_event_filled(
                cur, event_id, invite_id, invite["candidate_id"], now_iso,
            )
            candidates = {c["candidate_id"]: c for c in self.store.list_candidates(cur)}
            winner = candidates[invite["candidate_id"]]
            self._enqueue(
                cur, event_id=event_id, invite_id=invite_id,
                target=winner["contact"], msg_type="CONFIRMED",
                payload={
                    "event_id": event_id, "invite_id": invite_id,
                    "candidate_id": winner["candidate_id"], "shift_label": event["shift_label"],
                    "event_starts_at": event["event_starts_at"],
                },
            )
            self._enqueue(
                cur, event_id=event_id, target=event["original_guide_id"],
                msg_type="EVENT_FILLED",
                payload={
                    "event_id": event_id,
                    "winner_candidate_id": invite["candidate_id"],
                    "invite_id": invite_id,
                },
            )
            # 其余仍待响应者收到失效通知
            self._notify_superseded(cur, event, invite_id)
        return self._event_view(event_id)

    def decline(self, invite_id: str, reason: str = "", candidate_id: str | None = None) -> dict[str, Any]:
        """候选拒绝：邀请关闭；若该批全部关闭则立即轮换下一批。"""
        now_iso = to_iso(self.clock.now())
        with self.store.begin() as cur:
            invite = self.store.get_invitation(cur, invite_id)
            if invite is None:
                raise NotFound(f"邀请不存在：{invite_id}")
            event = self.store.get_event(cur, invite["event_id"])
            event_id = event["event_id"]
            if candidate_id is not None and invite["candidate_id"] != candidate_id:
                raise DomainError("该邀请不属于此候选")
            if event["status"] != EVENT_OPEN:
                raise DomainError(f"接力已结束（{event['status']}），拒绝窗口已关闭")
            if invite["status"] == INVITE_DECLINED:
                raise DomainError("该邀请已拒绝")
            if invite["status"] != INVITE_OPEN:
                raise DomainError(f"邀请当前状态为 {invite['status']}，无法拒绝")
            ok = self.store.decline_if_open(cur, invite_id, now_iso, reason[:200])
            if not ok:
                raise DomainError("拒绝失败：邀请状态已变化")
            self._enqueue(
                cur, event_id=event_id, invite_id=invite_id,
                target=self._contact_of(cur, invite["candidate_id"]),
                msg_type="DECLINE_ACK",
                payload={"event_id": event_id, "invite_id": invite_id, "reason": reason},
            )
        # 拒绝独立提交后再推进：该批可能已全部关闭 → TTL/下一批/耗尽
        self.sweep_event(event_id)
        return self._event_view(event_id)

    # ------------------------------------------------------------------ #
    # 原讲解员复岗
    # ------------------------------------------------------------------ #

    def restore_original(self, event_id: str, note: str = "原讲解员已复岗") -> dict[str, Any]:
        """原讲解员复岗：取消接力，待响应邀请全部失效并通知，事件确定关闭。"""
        now_iso = to_iso(self.clock.now())
        with self.store.begin() as cur:
            event = self.store.get_event(cur, event_id)
            if event["status"] == EVENT_CANCELLED_RESTORED:
                raise DomainError("该接力已因复岗取消")
            if event["status"] != EVENT_OPEN:
                raise DomainError(f"接力已结束（{event['status']}），无法复岗取消")
            expired = self.store.expire_all_open(cur, event_id, reason="ORIGINAL_RESTORED")
            self._notify_batch_end(cur, event, expired, "INVITE_CANCELLED_RESTORED")
            self.store.mark_event_terminal(cur, event_id, EVENT_CANCELLED_RESTORED, now_iso, note)
            self._enqueue(
                cur, event_id=event_id, target=event["original_guide_id"],
                msg_type="EVENT_CANCELLED_RESTORED",
                payload={"event_id": event_id, "note": note},
            )
        return self._event_view(event_id)

    # ------------------------------------------------------------------ #
    # 查询：接力链与最终安排
    # ------------------------------------------------------------------ #

    def get_event(self, event_id: str) -> dict[str, Any]:
        return self._event_view(event_id)

    def list_events(self) -> list[dict[str, Any]]:
        with self.store.begin() as cur:
            events = self.store.list_events(cur)
        return [self._event_view(e["event_id"]) for e in events]

    def get_invitation(self, invite_id: str) -> dict[str, Any]:
        with self.store.begin() as cur:
            invite = self.store.get_invitation(cur, invite_id)
            if invite is None:
                raise NotFound(f"邀请不存在：{invite_id}")
            return self._invite_view(invite)

    def _event_view(self, event_id: str) -> dict[str, Any]:
        """组装实时视图：事件 + 接力链（按批次/距离）+ 最终安排 + 投递状态。"""
        with self.store.begin() as cur:
            event = self.store.get_event(cur, event_id)
            invites = self.store.list_invitations(cur, event_id)
            pending_outbox = self.store.pending_count(cur, event_id)
        chain = [self._invite_view(i) for i in invites]
        rounds: dict[int, list[dict[str, Any]]] = {}
        for link in chain:
            rounds.setdefault(link["round"], []).append(link)
        final = None
        if event["status"] == EVENT_FILLED:
            final = {
                "candidate_id": event["final_candidate_id"],
                "invite_id": event["winner_invite_id"],
                "decided_at": event["decided_at"],
                "name": next(
                    (l["candidate_name"] for l in chain if l["invite_id"] == event["winner_invite_id"]),
                    None,
                ),
            }
        return {
            "event_id": event["event_id"],
            "shift_label": event["shift_label"],
            "original_guide_id": event["original_guide_id"],
            "event_starts_at": event["event_starts_at"],
            "deadline": event["deadline"],
            "required_qualifications": event["required_qualifications"],
            "batch_size": event["batch_size"],
            "invite_ttl_seconds": event["invite_ttl_seconds"],
            "status": event["status"],
            "note": event["note"],
            "created_at": event["created_at"],
            "decided_at": event["decided_at"],
            "final_assignment": final,
            "rounds": [
                {"round": r, "invitations": rounds[r]} for r in sorted(rounds)
            ],
            "chain": chain,
            "open_invitation_count": sum(1 for l in chain if l["status"] == INVITE_OPEN),
            "notifications_pending": pending_outbox,
            "server_time": to_iso(self.clock.now()),
        }

    @staticmethod
    def _invite_view(row: dict[str, Any]) -> dict[str, Any]:
        return {
            "invite_id": row["invite_id"],
            "event_id": row["event_id"],
            "round": row["round"],
            "candidate_id": row["candidate_id"],
            "candidate_name": row["candidate_name"],
            "distance_meters": row["distance_meters"],
            "qualifications": row["qualifications"],
            "status": row["status"],
            "sent_at": row["sent_at"],
            "expires_at": row["expires_at"],
            "responded_at": row["responded_at"],
            "reason": row["decline_reason"],
        }

    # ------------------------------------------------------------------ #
    # 投递箱辅助
    # ------------------------------------------------------------------ #

    def list_outbox(self, status: str | None = None, event_id: str | None = None) -> list[dict[str, Any]]:
        with self.store.begin() as cur:
            return self.store.list_outbox(cur, status=status, event_id=event_id)

    def _enqueue(
        self, cur, *, event_id: str, target: str, msg_type: str,
        payload: dict[str, Any], invite_id: str | None = None,
        channel: str | None = None,
    ) -> None:
        idem = f"{msg_type}:{invite_id or event_id}"
        self.store.enqueue_outbox(cur, {
            "idem_key": idem, "event_id": event_id, "invite_id": invite_id,
            "target": target, "channel": channel or self.default_channel,
            "msg_type": msg_type, "payload": payload, "created_at": to_iso(self.clock.now()),
        })

    def _contact_of(self, cur, candidate_id: str) -> str:
        cand = self.store.get_candidate(cur, candidate_id)
        return (cand or {}).get("contact") or candidate_id

    def _notify_batch_end(self, cur, event: dict[str, Any], invite_ids: list[str], msg_type: str) -> None:
        """批量结束（TTL/截止/复岗）时向失效邀请的候选各发一条通知。"""
        for invite_id in invite_ids:
            invite = self.store.get_invitation(cur, invite_id)
            if invite is None:
                continue
            self._enqueue(
                cur, event_id=event["event_id"], invite_id=invite_id,
                target=self._contact_of(cur, invite["candidate_id"]), msg_type=msg_type,
                payload={"event_id": event["event_id"], "invite_id": invite_id,
                         "candidate_id": invite["candidate_id"]},
            )

    def _notify_superseded(self, cur, event: dict[str, Any], winner_invite_id: str) -> None:
        """胜者产生后，其余同批/跨批待响应者收到失效通知。"""
        for invite in self.store.list_invitations(cur, event["event_id"]):
            if invite["invite_id"] == winner_invite_id:
                continue
            if invite["status"] == INVITE_SUPERSEDED:
                self._enqueue(
                    cur, event_id=event["event_id"], invite_id=invite["invite_id"],
                    target=self._contact_of(cur, invite["candidate_id"]),
                    msg_type="INVITE_SUPERSEDED",
                    payload={
                        "event_id": event["event_id"], "invite_id": invite["invite_id"],
                        "candidate_id": invite["candidate_id"],
                        "winner_invite_id": winner_invite_id,
                    },
                )
