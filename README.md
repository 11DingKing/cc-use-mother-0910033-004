# 紧急缺岗接力

活动开始前两小时讲解员缺岗时，运营员需要按**资格、距离、连续服务限制**快速寻找替补。
手工群发常造成多人同时到场或无人确认。本服务把这一过程做成确定性的「接力链」：

- 创建带**截止时间**的接力事件，按规则筛选候选并**分批邀请**；
- **首个有效确认原子获岗**，其余待响应邀请自动失效；
- 候选拒绝、邀请超时、原讲解员复岗、进程重启均有确定处理；
- 通知走**事务投递箱（transactional outbox）**，与状态变更同事务落库，至少一次投递；
- 接口实时返回完整接力链与最终安排。

## 领域约束与不变量

| 约束 | 实现 |
| --- | --- |
| 紧急候选筛选 | 资格须覆盖岗位要求 → 距离升序 → 排除连续服务受限者（刚结束一段达上限服务、未满强制休息间隔） |
| 首确认原子获岗 | 单写连接 + `BEGIN IMMEDIATE` 串行化，条件 `UPDATE ... WHERE status='OPEN'`，行计数判定唯一胜者 |
| 邀请超时失效 | 每批邀请有 TTL；后台线程 + 启动恢复 + 每次读/写惰性清扫三重触发，到点失效并自动发下一批 |
| 事务通知投递 | 通知与状态变更在同一事务写入 `outbox`，提交后由分发器投递；失败退避重试，重启回收僵死消息 |

事件状态机：`OPEN` → `FILLED`（有人确认）／`EXPIRED`（到截止时间或候选耗尽）／`CANCELLED_RESTORED`（原讲解员复岗）。
邀请状态机：`OPEN` → `CONFIRMED`／`DECLINED`／`EXPIRED`（TTL、截止、复岗）／`SUPERSEDED`（他人先确认）。

## 目录

- `domain/contract.json`：领域角色、状态、约束与样例。
- `src/domain_contract/`：契约读取与确定性校验（既有）。
- `src/relay_service/`：接力服务端。
  - `store.py`：SQLite 持久化（WAL）、状态机表结构、条件原子更新、outbox 领取/回收。
  - `engine.py`：领域引擎（筛选、分批、确认/拒绝、超时、复岗、接力链视图）。
  - `notifications.py`：通知通道与 outbox 分发器（重试、僵死回收、幂等键）。
  - `service.py`：运行时装配、后台清扫线程、**进程重启恢复入口**。
  - `httpapi.py` / `__main__.py`：标准库 HTTP 接口与启动入口（零第三方依赖）。
  - `clock.py`：时间抽象，测试可拨快时钟验证超时与重启。
- `tests/`：契约回归 + 引擎规则 + 并发竞态 + outbox/重启恢复 + HTTP 端到端。

## 运行

需要 Python ≥ 3.11，无第三方依赖。

```bash
python3 -m relay_service --db data/relay.sqlite3 --port 8080 \
    --batch-size 3 --invite-ttl 900 --sweep-interval 5
# 或安装后：relay-server --db data/relay.sqlite3
```

## HTTP 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/admin/candidates` | 登记/更新候选（资格、距离、最近连续服务结束时间、联系方式） |
| GET | `/admin/candidates` | 候选名册 |
| POST | `/events` | 创建接力事件（含 `deadline`、`required_qualifications`、`batch_size`、`invite_ttl_seconds`），自动发首批 |
| GET | `/events` | 事件列表（读时惰性推进超时/轮换） |
| GET | `/events/{id}` | **实时接力链**：各批次邀请、状态、最终安排、待投通知数 |
| POST | `/events/{id}/restore` | 原讲解员复岗，取消接力并使全部邀请失效 |
| POST | `/events/{id}/sweep` | 手动推进（排障/测试） |
| GET | `/events/{id}/outbox` | 该事件投递箱明细 |
| POST | `/invitations/{id}/confirm` | 候选确认；首个有效确认原子获岗，重复/迟到/他人确认返回 409 |
| POST | `/invitations/{id}/decline` | 候选拒绝；本批全部关闭时立即轮换下一批 |
| GET | `/invitations/{id}` | 邀请状态 |

### 典型时序

```bash
# 1) 运营员建事件（首批邀请随事务入投递箱）
curl -XPOST localhost:8080/events -H 'Content-Type: application/json' -d '{
  "shift_label": "14:00 青铜厅", "original_guide_id": "g77",
  "event_starts_at": "2026-10-05T12:00:00Z",
  "deadline": "2026-10-05T11:30:00Z",
  "required_qualifications": ["A"], "batch_size": 3, "invite_ttl_seconds": 900}'

# 2) 候选确认 / 拒绝（带 candidate_id 防冒认）
curl -XPOST localhost:8080/invitations/<invite_id>/confirm \
  -H 'Content-Type: application/json' -d '{"candidate_id": "c1"}'

# 3) 实时查看接力链与最终安排
curl localhost:8080/events/<event_id>
```

## 确定性处理说明

- **拒绝**：邀请关闭；若该批其余人也已关闭，不等 TTL，立即发下一批。
- **超时**：TTL 到点未响应的邀请置 `EXPIRED` 并通知候选；本批清空后自动发下一批；无候选可发时事件 `EXPIRED`（候选耗尽）。
- **截止时间**：到达 `deadline` 仍 OPEN，则全部待响应邀请失效、事件 `EXPIRED`，此后任何确认 409。
- **原人员复岗**：`OPEN` 中任意时刻可取消，全部待响应邀请失效并通知，事件 `CANCELLED_RESTORED`。
- **进程重启**：状态全部在 SQLite；启动时 `recover()` 以当前时间推进所有 OPEN 事件（补超时/轮换/截止），并回收投递箱中僵死的 `PROCESSING` 消息继续投递。
- **多人同时确认**：串行写事务保证恰有一人成功；其余邀请在同一事务内被置 `SUPERSEDED` 并收到失效通知。

## 验证

```bash
python3 -m unittest discover -s tests -v     # 21 个用例
python3 -m compileall -q src tools tests     # 编译检查
python3 tools/check_contract.py domain/contract.json
```
