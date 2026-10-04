# 燃气管线泄漏检测与隔离协调

模块化纯 Python 3.9.6+ 标准库项目，默认端口 `8333`。

- `app.py`：参数、依赖和服务生命周期。
- `src/domain.py`：管段、传感值和来源记录校验。
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

接口包括 `GET /health`、`GET /api/state`、`POST /api/items`、`POST /api/items/<id>/sources`、`POST /api/items/<id>/actions` 和审计查询。测试覆盖完整抢修流程、重复事件、阀门顺序、试压阈值、现场危险条件、权限和版本冲突。模型不替代 SCADA、管网水力计算或正式应急预案。

### 管网拓扑与隔离协调

- `POST /api/network/segments`：登记管段（`segment_id`、`pipeline_id`）。
- `POST /api/network/valves`：登记阀门。
- `POST /api/network/connections`：登记阀门连接关系（`valve_id`、`segment_a`、`segment_b`，`segment_b` 为空表示尽头阀）。
- `GET /api/network`：查看管段、阀门与连接关系。
- `POST /api/items/<id>/actions` 的 `isolate`：提交关阀顺序并创建隔离作业；同一管段已有进行中的作业时返回 `boundary_conflict`，响应含 `current_valves` 与 `diff`（现用阀门与差异）。
- `POST /api/isolation-jobs/<job_id>/valve-result`：报告关阀结果（`result` 为 `closed`/`stuck`/`mismatch`）。阀门卡死或回执对不上时从断点恢复，改选相邻阀门组成新边界，已关阀门保持关闭。
- `POST /api/valves/<valve_id>/receipts`：提交阀位回执（`position` 为 `open`/`closed`），同一阀门同一观测时间只记一次。
- `GET /api/valves/<valve_id>`：查看阀门最新阀位与回执。
- `GET /api/isolation-jobs/<job_id>`、`GET /api/items/<id>/isolation-job`：查看隔离作业进度。

连接关系或阀位回执更新后，未执行的边界失效重算，已关阀结果保留；服务重启后作业可继续。
