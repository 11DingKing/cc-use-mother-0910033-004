# 紧急缺岗接力

活动开始前两小时讲解员缺岗时，运营员创建带截止时间的接力事件；服务端按**资格、距离、连续服务限制**筛选候选，**分批发送邀请**，**首个有效确认原子获得岗位**，其余邀请自动失效。候选拒绝、超时、原人员复岗、进程重启均有确定性处理；通知走**事务投递箱**；接口实时呈现**接力链**与最终安排。

## 设计要点与确定性语义

- **首确认原子获岗**：SQLite 单连接 + `BEGIN IMMEDIATE` + 进程内互斥锁，把"读取岗位状态 → 写入获胜确认 → 其余邀请失效"串行化为一个事务。同批并发确认只有一人成功，其余在同一事务内落为 `superseded`。
- **分批邀请**：每批 N 人；一批全部落定（拒绝或超时）后才发下一批，避免多人同时到场；支持批间隔。
- **候选筛选**：必备资格为子集、距离（档案距离或活动坐标 Haversine 距离）不超上限、`consecutive_shifts < max_consecutive_shifts`；按距离升序（同距按连续班次、再按编号）排序。每次评选的合格名次与剔除原因落 `screening_trace`，接口透明可见。
- **拒绝**：邀请置 `declined`；该批仍有在途邀请时不开新批。
- **超时**：每个邀请有 `expires_at` 且不晚于事件 `deadline`；后台巡检（默认 0.5s）与每个写请求都在事务内推进，到期确定性失效并发下一批；截止时间到且无人接岗则事件 `failed`。
- **原人员复岗**：事件置 `recovered`，全部在途邀请 `revoked` 并通知；复岗后到达的确认确定性返回 409。
- **进程重启**：状态全部在 SQLite；启动先 `tick()` 补齐超时/批次/失败结论，再排空投递箱。
- **事务投递箱**：通知与业务状态**同事务提交**；投递为"认领(pending→sending)→事务外发送→置 sent 并记录稳定幂等键"；发送失败释放认领重试；崩溃在发送后/标记前产生的陈旧 `sending` 会重投（至少一次），通道凭 `notify_key` 去重。
- **实时接口**：`GET /events/{id}` 返回完整接力链；`GET /events/{id}/stream` 为 SSE，提交后广播推送。

## 目录

- `domain/contract.json`：领域角色、状态、约束和样例。
- `src/domain_contract/`：契约读取与确定性校验。
- `src/relay/`：接力服务端。
  - `clock.py`：可注入时钟（确定性测试/演示）。
  - `store.py`：SQLite 持久化与串行事务。
  - `eligibility.py`：资格、距离、连续服务筛选与排序。
  - `outbox.py`：事务投递箱与至少一次投递。
  - `service.py`：接力领域服务（分批、原子获岗、超时/复岗/重启）。
  - `httpapi.py`：标准库 HTTP/JSON 接口与 SSE。
  - `server.py`：进程入口、后台巡检与投递线程、演示种子数据。
- `tools/check_contract.py`：命令行摘要检查。
- `tests/`：契约、领域服务（含并发/重启）、投递箱、HTTP 接口测试。

## 运行

仅需 Python ≥ 3.11，无第三方依赖：

```bash
PYTHONPATH=src python3 -m relay.server --db data/relay.db --host 0.0.0.0 --port 8080 --seed
```

也可在项目根目录直接执行 `PYTHONPATH=src python3 -m relay.server --seed`（默认监听 `0.0.0.0:8080`、库文件 `data/relay.db`）。

`--seed` 写入 7 名演示候选（含缺资格、连续班次达上限、超距离、未成年需监护人代确认等样例）。

## 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/health` | 存活检查 |
| GET / PUT | `/candidates`、`/candidates/{id}` | 候选档案（资格、距离、连续班次、监护人联系方式） |
| POST | `/events` | 创建接力事件（立即发第一批） |
| GET | `/events`、`/events/{id}` | 事件列表 / 实时接力链（分批、筛选留痕、最终安排） |
| GET | `/events/{id}/stream` | SSE 实时推送（`snapshot`/`update`/`ping`） |
| POST | `/invitations/{id}/respond` | `{"action":"accept"}` 或 `decline` |
| POST | `/events/{id}/recover` | 原讲解员复岗 |
| POST | `/tick` | 手动触发一次巡检 |
| GET | `/outbox` | 投递箱内容（状态/尝试次数） |

创建事件请求示例：

```json
{
  "activity": "青铜器展厅讲解",
  "assignee_id": "v009",
  "starts_at": "2026-10-05T11:00:00Z",
  "deadline": "2026-10-05T10:50:00Z",
  "required_qualifications": ["guide", "first_aid"],
  "max_distance_m": 3000,
  "location": {"lat": 30.2, "lng": 120.1},
  "batch_size": 3,
  "invite_timeout_seconds": 300,
  "batch_gap_seconds": 0
}
```

事件状态：`open` / `resolved` / `failed` / `recovered`；邀请状态：`pending` / `accepted` / `declined` / `expired` / `superseded` / `revoked`。

## 验证

```bash
python3 -m unittest discover -s tests -v
python3 -m compileall -q src tools tests
python3 tools/check_contract.py domain/contract.json
```
