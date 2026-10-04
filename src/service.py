from datetime import datetime, timezone

from . import domain, rules
from .domain import DomainError, BoundaryConflictError


def _now():
    return datetime.now(timezone.utc).isoformat()


class Service:
    def __init__(self, repository):
        self.repository = repository

    # ------------------------------------------------------------------
    # 既有业务：泄漏事件、来源、状态机
    # ------------------------------------------------------------------

    def create_item(self, payload, actor, role, region=None):
        if not actor or not role:
            raise DomainError("identity_required", "需要用户身份和角色", 401)
        if role not in rules.CREATE_ROLES:
            raise DomainError("forbidden", "当前角色不能创建此类业务记录", 403)
        normalized = domain.normalize_create(payload)
        stable_key = normalized.pop("_stable_key")
        return self.repository.create_item(
            rules.ENTITY_TYPE, stable_key, rules.INITIAL_STATUS, normalized, actor, role
        )

    def add_source(self, item_id, payload, actor, role, region=None):
        if not actor or not role:
            raise DomainError("identity_required", "需要用户身份和角色", 401)
        if role not in rules.SOURCE_ROLES:
            raise DomainError("forbidden", "当前角色不能提交来源记录", 403)
        item = self.repository.get_item(item_id)
        normalized = domain.normalize_source(payload)
        if region and rules.ENFORCE_REGION and role != "regulator" and normalized.get("region") and normalized["region"] != region:
            raise DomainError("region_mismatch", "来源记录不属于当前管辖区域", 403)
        result = self.repository.add_source(
            item_id,
            normalized.pop("source_type"),
            normalized.pop("external_id"),
            normalized,
            normalized.pop("observed_at"),
            actor,
            role,
        )
        return result

    def act(self, item_id, action, payload, actor, role, expected_version=None, region=None):
        if not actor or not role:
            raise DomainError("identity_required", "需要用户身份和角色", 401)
        item = self.repository.get_item(item_id)
        allowed = rules.ACTION_ROLES.get(action, set())
        if role not in allowed:
            raise DomainError("forbidden", "当前角色不能执行该操作", 403)
        if rules.ENFORCE_REGION and action in rules.REGION_SENSITIVE_ACTIONS and region and role != "regulator":
            if item["payload"].get("region") != region:
                raise DomainError("region_mismatch", "不能处理其他区域的记录", 403)
        if action in rules.ACTION_REQUIRES_VERSION and expected_version is None:
            raise DomainError("expected_version_required", "该操作需要 expected_version", 400)

        if action == "isolate":
            return self._isolate(item, payload, actor, role, expected_version)

        new_status, new_payload, event_payload = rules.apply_action(item, action, payload, actor, role)
        self.repository.apply_action(
            item_id, action, actor, role, new_status, new_payload, event_payload, expected_version
        )
        return self.get_item(item_id)

    def _isolate(self, item, payload, actor, role, expected_version):
        item_id = item["id"]
        segment_id = item["payload"]["segment_id"]
        sequence = payload.get("valve_sequence") or []

        # 抢边界：同一管段已有进行中的隔离作业，只让一方继续，另一方看现用阀门与差异
        existing = self.repository.get_active_job_for_segment(segment_id)
        if existing:
            diff = self._diff(sequence, existing["boundary"])
            raise BoundaryConflictError(
                "该管段已有进行中的隔离作业，不能重复抢边界",
                existing["boundary"],
                diff,
                existing["id"],
            )

        new_status, new_payload, event_payload = rules.apply_action(item, "isolate", payload, actor, role)
        job = self._build_job(item, sequence, actor, role)
        self.repository.apply_isolate(
            item_id, actor, role, new_status, new_payload, event_payload, expected_version, job
        )
        return self.get_item(item_id)

    def _build_job(self, item, sequence, actor, role):
        segment_id = item["payload"]["segment_id"]
        pipeline_id = item["payload"].get("pipeline_id")
        graph = self.repository.load_graph()
        region = [segment_id]
        if graph.has_segment(segment_id):
            boundary = graph.boundary(region)
        else:
            boundary = list(sequence)
        planned = list(sequence)
        for valve in boundary:
            if valve not in planned:
                planned.append(valve)
        return {
            "item_id": item["id"],
            "segment_id": segment_id,
            "pipeline_id": pipeline_id,
            "status": "planned",
            "version": 1,
            "region": region,
            "boundary": boundary,
            "planned_sequence": planned,
            "closed_valves": [],
            "failed_valves": [],
            "current_valve": boundary[0] if boundary else None,
            "created_by": actor,
            "created_role": role,
            "created_at": _now(),
            "updated_at": _now(),
        }

    @staticmethod
    def _diff(submitted, current):
        submitted_set = set(submitted)
        current_set = set(current)
        return {
            "only_in_submitted": [v for v in submitted if v not in current_set],
            "only_in_current": [v for v in current if v not in submitted_set],
            "common": [v for v in submitted if v in current_set],
        }

    def get_item(self, item_id):
        item = self.repository.get_item(item_id)
        item["sources"] = self.repository.list_sources(item_id)
        item["audit"] = self.repository.audit_trail(item_id)
        item["assessment"] = rules.assess(item["payload"])
        item["isolation_job"] = self.repository.get_job_for_item(item_id)
        return item

    def list_items(self, status=None):
        return self.repository.list_items(status)

    def state(self):
        return self.repository.state_summary()

    # ------------------------------------------------------------------
    # 管网拓扑
    # ------------------------------------------------------------------

    def upsert_segment(self, payload, actor, role):
        if not actor or not role:
            raise DomainError("identity_required", "需要用户身份和角色", 401)
        segment_id = payload.get("segment_id")
        if not segment_id:
            raise DomainError("segment_id_required", "需要管段编号")
        pipeline_id = payload.get("pipeline_id", "")
        return self.repository.upsert_segment(segment_id, pipeline_id, payload.get("name"))

    def upsert_valve(self, payload, actor, role):
        if not actor or not role:
            raise DomainError("identity_required", "需要用户身份和角色", 401)
        valve_id = payload.get("valve_id")
        if not valve_id:
            raise DomainError("valve_id_required", "需要阀门编号")
        return self.repository.upsert_valve(valve_id, payload.get("name"))

    def upsert_connection(self, payload, actor, role):
        if not actor or not role:
            raise DomainError("identity_required", "需要用户身份和角色", 401)
        valve_id = payload.get("valve_id")
        if not valve_id:
            raise DomainError("valve_id_required", "需要阀门编号")
        segment_a = payload.get("segment_a")
        segment_b = payload.get("segment_b")
        pipeline_id = payload.get("pipeline_id", "")
        self.repository.upsert_valve(valve_id, payload.get("valve_name"))
        if segment_a:
            self.repository.upsert_segment(segment_a, pipeline_id, None)
        if segment_b:
            self.repository.upsert_segment(segment_b, pipeline_id, None)
        result = self.repository.upsert_connection(valve_id, segment_a, segment_b)
        # 连接关系更新后，未执行的边界失效重算，已关阀结果留着
        self.recompute_active_jobs()
        return result

    def get_network(self):
        return {
            "segments": self.repository.list_segments(),
            "valves": self.repository.list_valves(),
            "connections": self.repository.list_connections(),
        }

    # ------------------------------------------------------------------
    # 阀门回执
    # ------------------------------------------------------------------

    def add_valve_receipt(self, valve_id, payload, actor, role):
        if not actor or not role:
            raise DomainError("identity_required", "需要用户身份和角色", 401)
        position = payload.get("position")
        observed_at = payload.get("observed_at")
        note = payload.get("note", "")
        outcome = payload.get("outcome")
        if position not in ("open", "closed"):
            raise DomainError("invalid_position", "阀位必须是 open/closed")
        if not observed_at:
            raise DomainError("observed_at_required", "需要观测时间")
        if outcome is not None and outcome not in ("closed", "stuck", "mismatch"):
            raise DomainError("invalid_outcome", "回执结果必须是 closed/stuck/mismatch")

        receipt_payload = {"valve_id": valve_id, "position": position, "outcome": outcome, "note": note}
        receipt, is_new = self.repository.add_valve_receipt(
            valve_id, position, outcome, observed_at, note, receipt_payload
        )

        graph = self.repository.load_graph()
        for job in self.repository.list_active_jobs():
            if outcome == "closed" or position == "closed":
                self._mark_closed(job, valve_id, graph)
            elif outcome in ("stuck", "mismatch"):
                self._replan(job, valve_id, graph)
            else:
                self._refresh_job(job, graph)
            self.repository.update_isolation_job(job)

        return {
            "receipt": receipt,
            "receipt_recorded": is_new,
            "latest_position": self.repository.latest_valve_position(valve_id),
        }

    def get_valve(self, valve_id):
        receipts = self.repository.list_valve_receipts(valve_id)
        return {
            "valve_id": valve_id,
            "latest_position": self.repository.latest_valve_position(valve_id),
            "receipts": receipts,
        }

    # ------------------------------------------------------------------
    # 隔离作业：关阀失败从断点恢复
    # ------------------------------------------------------------------

    def report_valve_result(self, job_id, payload, actor, role):
        if not actor or not role:
            raise DomainError("identity_required", "需要用户身份和角色", 401)
        job = self.repository.get_isolation_job(job_id)
        valve_id = payload.get("valve_id")
        result = payload.get("result")
        observed_at = payload.get("observed_at")
        note = payload.get("note", "")
        if not valve_id:
            raise DomainError("valve_id_required", "需要阀门编号")
        if result not in ("closed", "stuck", "mismatch"):
            raise DomainError("invalid_result", "结果必须是 closed/stuck/mismatch")
        if not observed_at:
            raise DomainError("observed_at_required", "需要观测时间")

        position = "closed" if result == "closed" else "open"
        receipt_payload = {"valve_id": valve_id, "result": result, "note": note}
        receipt, is_new = self.repository.add_valve_receipt(
            valve_id, position, result, observed_at, note, receipt_payload
        )

        graph = self.repository.load_graph()
        if result == "closed":
            self._mark_closed(job, valve_id, graph)
        else:
            self._replan(job, valve_id, graph)
        self.repository.update_isolation_job(job)

        return {"job": job, "receipt": receipt, "receipt_recorded": is_new}

    def _mark_closed(self, job, valve_id, graph):
        closed = set(job["closed_valves"])
        if valve_id not in closed:
            closed.add(valve_id)
            job["closed_valves"] = sorted(closed)
        return self._refresh_job(job, graph)

    def _replan(self, job, valve_id, graph):
        failed = set(job["failed_valves"])
        if valve_id not in failed:
            failed.add(valve_id)
            job["failed_valves"] = sorted(failed)
            region = list(job["region"])
            if graph.has_segment(region[0]) and valve_id in graph.valves:
                other = graph.other_side(valve_id, region)
                if other is not None and other not in region:
                    region.append(other)
                    job["region"] = region
        return self._refresh_job(job, graph)

    def _refresh_job(self, job, graph):
        region = list(job["region"])
        closed = set(job["closed_valves"])
        failed = set(job["failed_valves"])
        if region and graph.has_segment(region[0]):
            new_boundary = graph.boundary(region)
        else:
            new_boundary = list(job["boundary"])
        pending = [v for v in new_boundary if v not in closed and v not in failed]
        failed_on_boundary = [v for v in new_boundary if v in failed]
        if failed_on_boundary:
            status = "isolation_failed"
        elif not pending:
            status = "isolated"
        elif failed:
            status = "replan"
        elif closed:
            status = "in_progress"
        else:
            status = "planned"
        job["boundary"] = new_boundary
        job["current_valve"] = pending[0] if pending else None
        job["status"] = status
        return job

    def recompute_active_jobs(self):
        graph = self.repository.load_graph()
        count = 0
        for job in self.repository.list_active_jobs():
            self._refresh_job(job, graph)
            self.repository.update_isolation_job(job)
            count += 1
        return count

    def get_isolation_job(self, job_id):
        return self.repository.get_isolation_job(job_id)

    def get_job_for_item(self, item_id):
        return self.repository.get_job_for_item(item_id)

    def list_isolation_jobs(self, segment_id=None, status=None):
        return self.repository.list_isolation_jobs(segment_id, status)
