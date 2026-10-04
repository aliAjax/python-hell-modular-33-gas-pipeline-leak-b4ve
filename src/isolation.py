"""隔离作业状态机。

作业负载（payload）结构：
{
  "segment_id": "S-8",
  "boundary": {"upstream": ["V-1"], "downstream": ["V-2"]},
  "sides": {
    "upstream": [step, ...],
    "downstream": [step, ...]
  },
  "history": [{"boundary": {...}, "actor": ..., "reason": ...}],
  "failures": [记录失败点，便于审计]
}

step: {"valve": "V-2", "status": pending|closed|failed,
       "receipt_ids": [id...], "failures": [{"valve","result","receipt_id"}]}

不变式：
- status=closed 的步骤在 resume/recompute 中永不改动、永不重开；
- 失败阀门保留 failed 记录，替代阀插在同一侧失败点之后；
- 关阀按 upstream 侧再 downstream 侧的顺序逐步推进，一次只处理第一个 pending 步骤。
"""

from collections import deque

from .domain import DomainError
from .topology import SIDES, UPSTREAM, expand_failed

JOB_PENDING = "pending_close"
JOB_PAUSED = "paused"
JOB_ISOLATED = "isolated"
JOB_ABANDONED = "abandoned"
ACTIVE_STATUSES = (JOB_PENDING, JOB_PAUSED, JOB_ISOLATED)

MAX_EXPANSION_DEPTH = 8


def make_step(valve):
    return {"valve": valve, "status": "pending", "receipt_ids": [], "failures": []}


def initial_payload(segment_id, boundary, actor, history_extra=None):
    sides = {side: [make_step(v) for v in boundary[side]] for side in SIDES}
    history = [{"boundary": {side: list(boundary[side]) for side in SIDES},
                "actor": actor, "reason": history_extra or "submitted"}]
    return {"segment_id": segment_id, "boundary": boundary, "sides": sides, "history": history, "failures": []}


def iter_steps(sides):
    """按执行顺序展平步骤。"""
    for side in SIDES:
        for index, step in enumerate(sides.get(side, [])):
            yield side, index, step


def current_step(payload):
    """返回下一个待关阀步骤 (side, index, step)；全部关完返回 None。"""
    for side, index, step in iter_steps(payload["sides"]):
        if step["status"] == "pending":
            return side, index, step
    return None


def failed_steps(payload):
    return [(side, index, step) for side, index, step in iter_steps(payload["sides"]) if step["status"] == "failed"]


def unresolved_failures(payload):
    """尚未被后续替代阀覆盖的失败步骤。

    failed 步骤只是死记录：若同一侧其后存在已关（或待关）的替代阀，
    该失败点已被化解；否则作业必须停在断点等待恢复。
    """
    result = []
    for side in SIDES:
        steps = payload["sides"][side]
        for index, step in enumerate(steps):
            if step["status"] != "failed":
                continue
            covered = any(later["status"] in ("closed", "pending") for later in steps[index + 1:])
            if not covered:
                result.append((side, index, step))
    return result


def is_isolated(payload):
    if current_step(payload) is not None:
        return False
    return not unresolved_failures(payload)


def job_status_after_change(payload):
    if is_isolated(payload):
        return JOB_ISOLATED
    if unresolved_failures(payload):
        return JOB_PAUSED
    return JOB_PENDING


def closed_valves(payload):
    return [step["valve"] for _s, _i, step in iter_steps(payload["sides"]) if step["status"] == "closed"]


def used_valves(payload):
    return [step["valve"] for _s, _i, step in iter_steps(payload["sides"])]


def _record_failure(payload, side, step, receipt):
    step["status"] = "failed"
    entry = {"valve": step["valve"], "result": receipt["result"], "receipt_id": receipt.get("id")}
    step["failures"].append(entry)
    payload["failures"].append({"side": side, **entry})


def apply_closing_receipt(payload, receipt):
    """处理当前待关阀步骤的一条回执，返回新的作业状态。

    - closed：该步关紧并推进；
    - stuck/mismatch：该步失败，作业在断点暂停，已关阀门保持关闭。
    """
    current = current_step(payload)
    if current is None:
        if unresolved_failures(payload):
            raise DomainError("job_paused", "作业停在失败断点，请先 resume 改选边界", 409)
        raise DomainError("job_already_isolated", "边界阀门已全部关闭", 409)
    side, _index, step = current
    if step["valve"] != receipt["valve_id"]:
        raise DomainError(
            "out_of_order_valve",
            "当前应关阀门是 %s，不能先处理 %s" % (step["valve"], receipt["valve_id"]),
            409,
        )
    if receipt["result"] == "closed":
        step["status"] = "closed"
        step["receipt_ids"].append(receipt.get("id"))
        return job_status_after_change(payload)
    _record_failure(payload, side, step, receipt)
    return JOB_PAUSED


def _pick_replacement(topo, target_segment, failed_valve, used, failed_set):
    """沿失败阀门向目标管段之外确定性地找替代阀。

    候选本身若已有失败回执，则继续向外（BFS），直到找到可用阀门。
    返回 (valve_id, via_segment_id) 或 None。
    """
    queue = deque()
    seen = set(used) | {failed_valve}
    for neighbor, via in expand_failed(topo, target_segment, failed_valve, used):
        queue.append((neighbor, via, 1))
        seen.add(neighbor)
    while queue:
        valve, via, depth = queue.popleft()
        if depth > MAX_EXPANSION_DEPTH:
            continue
        if valve in failed_set:
            for neighbor, next_via in topo.neighbors(valve):
                if next_via == target_segment or neighbor in seen:
                    continue
                seen.add(neighbor)
                queue.append((neighbor, next_via, depth + 1))
            continue
        return valve, via
    return None


def resume(topo, payload, replacements=None, actor=None):
    """从失败断点恢复：失败阀保留 failed 记录，改选相邻阀组成新边界。

    replacements 可选 {failed_valve: chosen_valve}，由值班员指定替代阀；
    不指定则按拓扑确定性选择。已关阀门保持关闭。
    """
    replacements = replacements or {}
    target_segment = payload["segment_id"]
    open_failures = unresolved_failures(payload)
    if not open_failures:
        if current_step(payload) is None:
            raise DomainError("job_already_isolated", "边界阀门已全部关闭", 409)
        raise DomainError("no_breakpoint", "作业没有失败断点，无需恢复", 409)
    used = set(used_valves(payload))
    latest_failed = {f["valve"] for f in payload.get("failures", [])}
    while True:
        open_failures = unresolved_failures(payload)
        if not open_failures:
            break
        side, index, step = open_failures[0]
        failed_valve = step["valve"]
        chosen = replacements.get(failed_valve)
        if chosen is not None:
            chosen = chosen.strip()
            if chosen not in topo.valves:
                raise DomainError("unknown_valve", "替代阀门未登记: %s" % chosen)
            via = None
            for neighbor, neighbor_via in topo.neighbors(failed_valve):
                if neighbor == chosen and neighbor_via != target_segment:
                    via = neighbor_via
                    break
            if via is None:
                raise DomainError(
                    "replacement_not_adjacent",
                    "替代阀门 %s 与失败阀 %s 不在相邻管段（或仍在目标管段上）" % (chosen, failed_valve),
                )
            if chosen in used:
                raise DomainError("replacement_already_used", "替代阀门 %s 已在边界中" % chosen)
        else:
            picked = _pick_replacement(topo, target_segment, failed_valve, used, latest_failed)
            if picked is None:
                raise DomainError("no_alternative_valve", "失败阀 %s 找不到可用的相邻替代阀" % failed_valve, 409)
            chosen, via = picked
        used.add(chosen)
        new_step = make_step(chosen)
        payload["sides"][side].insert(index + 1, new_step)
        payload.setdefault("history", []).append({
            "actor": actor,
            "reason": "resume",
            "replaced": failed_valve,
            "with": chosen,
            "via_segment": via,
        })
    # 失败记录保留在步骤上；插入的替代阀是后续 pending 步骤
    return job_status_after_change(payload)


def _reachable(topo, start, goal, blocked=None):
    if start == goal:
        return True
    blocked = blocked or set()
    seen = {start}
    queue = deque([start])
    while queue:
        current = queue.popleft()
        for neighbor, _via in topo.neighbors(current):
            if neighbor in blocked or neighbor in seen:
                continue
            if neighbor == goal:
                return True
            seen.add(neighbor)
            queue.append(neighbor)
    return False


def recompute(topo, payload, latest_receipts, actor=None):
    """连接关系或阀位回执更新后重算未执行的边界。

    - closed 步骤一律保留（已关阀结果留着，绝不重开）；
    - pending/failed 步骤逐一重验：外部回执确认 closed 的直接采纳；
      确认失败或在新拓扑中与已关锚点断链的，向相邻管段扩展替代阀；
    - 重算后作业仍需按顺序执行后续 pending 步骤。
    """
    target_segment = payload["segment_id"]
    sequence = topo.segment(target_segment)
    endpoints = {UPSTREAM: sequence[0], "downstream": sequence[-1]}

    for side in SIDES:
        steps = payload["sides"][side]
        kept = []
        closed_anchor = None
        used = set(used_valves(payload))
        for step in steps:
            if step["status"] == "closed":
                kept.append(step)
                closed_anchor = step["valve"]
                continue
            valve = step["valve"]
            receipt = latest_receipts.get(valve)
            # 外部回执已确认该阀关紧：采纳为已关结果
            if receipt and receipt.get("result") == "closed" and receipt.get("reported_position") in (None, "closed"):
                adopted = make_step(valve)
                adopted["status"] = "closed"
                if receipt.get("id") is not None:
                    adopted["receipt_ids"].append(receipt["id"])
                kept.append(adopted)
                closed_anchor = valve
                continue
            failed_by_receipt = bool(receipt and receipt.get("result") in ("stuck", "mismatch"))
            broken_link = valve not in topo.valves or (
                closed_anchor is not None and not _reachable(topo, closed_anchor, valve)
            )
            if not failed_by_receipt and not broken_link:
                # 仍是有效待关阀，复位为 pending（清掉旧失败标记并保留失败履历）
                fresh = make_step(valve)
                fresh["failures"] = step.get("failures", [])
                kept.append(fresh)
                continue
            # 记录失败点并改选相邻阀
            failed_record = {"valve": valve,
                             "result": receipt.get("result") if receipt else "topology_changed",
                             "receipt_id": receipt.get("id") if receipt else None}
            failed_step = make_step(valve)
            failed_step["status"] = "failed"
            failed_step["failures"].append(failed_record)
            payload.setdefault("failures", []).append({"side": side, **failed_record})
            kept.append(failed_step)
            failed_set = {f["valve"] for f in payload.get("failures", [])}
            # 失败阀已不在拓扑中时，从已关锚点或管段端点向外扩展
            anchor = valve if valve in topo.valves else (closed_anchor or endpoints[side])
            picked = _pick_replacement(topo, target_segment, anchor, used, failed_set)
            if picked is None:
                payload["sides"][side] = kept
                payload.setdefault("history", []).append({
                    "actor": actor, "reason": "recompute", "failed": valve, "with": None,
                })
                continue
            chosen, via = picked
            used.add(chosen)
            kept.append(make_step(chosen))
            payload.setdefault("history", []).append({
                "actor": actor, "reason": "recompute", "replaced": valve, "with": chosen, "via_segment": via,
            })
        payload["sides"][side] = kept

    return job_status_after_change(payload)


def active_boundary(payload):
    """当前实际在用边界（按侧、按执行顺序，含已扩展的替代阀，不含 failed 死阀）。"""
    result = {}
    for side in SIDES:
        result[side] = [step["valve"] for step in payload["sides"][side] if step["status"] != "failed"]
    return result
