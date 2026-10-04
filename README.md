# 燃气管线泄漏检测与隔离协调

模块化纯 Python 3.9.6+ 标准库项目，默认端口 `8333`。

- `app.py`：参数、依赖和服务生命周期。
- `src/domain.py`：管段、传感值和来源记录校验。
- `src/topology.py`：阀门/管段拓扑、相邻阀扩展、边界差异计算。
- `src/isolation.py`：隔离作业状态机——顺序关阀、断点暂停、相邻阀替代、失效重算。
- `src/rules.py`：泄漏评分、阀门顺序、修复、试压、恢复状态机。
- `src/repository.py`：SQLite、重复保护、乐观版本和审计链。
- `src/service.py`：角色权限和业务编排。
- `src/http_api.py`：JSON 接口与首页。
- `src/audit.py`：可校验的审计事件。

```bash
python3 app.py --init --db ./data.db
python3 app.py --db ./data.db --port 8333
python3 -m unittest discover -s tests -v
```

## 隔离作业（断点恢复，不整单取消）

- `POST /api/topology/valves` 登记阀门（`valve_id`、`kind`）。
- `PUT 语义：POST /api/segments/{segment}/links` 维护管段连接（有序阀门链路，相邻阀门互为邻接）。
- `POST /api/receipts` 阀门回执（`result=closed|stuck|mismatch`、`idempotency_key`），重复 key 只记一次。
- `POST /api/segments/{segment}/isolation-jobs` 提交上游/下游边界，创建顺序关阀作业。
- `POST /api/isolation/jobs/{id}/close` 提交**当前待关阀**回执（按 upstream→downstream 顺序，乱序拒绝）。
- `POST /api/isolation/jobs/{id}/resume` 关阀失败（卡死/回执与现场阀位不符）后从断点恢复：失败阀保留 `failed` 记录，沿非目标管段的相邻阀门改选新边界（可 `replacements` 指定），**已关阀门保持关闭**。
- `POST /api/isolation/jobs/{id}/recompute` 连接关系或阀位回执更新后，未执行边界标记 `stale` 并失效重算；`closed` 步骤一律保留；外部回执确认关闭的直接采纳。
- `POST /api/isolation/jobs/{id}/abandon` 作废作业（已关阀不会被自动重新打开）。
- 查询：`GET /api/isolation/jobs`、`GET /api/isolation/jobs/{id}`、`GET /api/segments/{segment}/jobs`、`GET /api/topology`、`GET /api/receipts`。

### 关键语义

- **同一管段抢边界只让一方继续**：管段至多一个活跃作业（`pending_close/paused/isolated`，部分唯一索引兜底）。后来方收到 409 `active_job_exists`，`details` 带现用阀门 `active_boundary`、作业归属以及按侧比较的 `diff`（`active_only`/`submitted_only`）。只有作业 owner 可推进作业。
- **失效与重算**：拓扑修订号 `topology_revision` 与阀位回执修订号 `position_revision` 单调递增。读取作业时与作业保存的修订号比较得出 `stale/stale_reasons`；`stale` 时 `close` 被拒，必须先 `recompute`。重算只动未执行步骤，已关阀结果留着。
- **持久化与幂等**：作业、步骤、回执全部落 SQLite，服务重启可继续；回执按 `idempotency_key` 去重，重复关阀回执不产生新版本、不重复驱动作业。所有变更走乐观版本 `expected_version` 与审计链。

泄漏处置接口还包括 `GET /health`、`GET /api/state`、`POST /api/items`、`POST /api/items/<id>/sources`、`POST /api/items/<id>/actions` 和审计查询。测试覆盖完整抢修流程、重复事件、阀门顺序、试压阈值、现场危险条件、权限、版本冲突，以及断点恢复、抢边界差异、失效重算、重启续做和回执幂等。模型不替代 SCADA、管网水力计算或正式应急预案。
