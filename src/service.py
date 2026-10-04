from . import domain, rules
from .domain import DomainError, ConflictError
from .topology import Topology, normalize_boundary, validate_proposal, boundary_diff
from .isolation import (
    JOB_PENDING, JOB_PAUSED, JOB_ABANDONED,
    ACTIVE_STATUSES, initial_payload, apply_closing_receipt,
    resume as engine_resume, recompute as engine_recompute,
    current_step, closed_valves, active_boundary,
)

VALVE_ROLES = {"dispatcher", "supervisor"}
SEGMENT_ROLES = {"dispatcher", "supervisor"}
RECEIPT_ROLES = {"dispatcher", "responder", "sensor", "patrol"}
JOB_CREATE_ROLES = {"supervisor", "responder"}


class Service:
    def __init__(self, repository):
        self.repository = repository

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
        new_status, new_payload, event_payload = rules.apply_action(item, action, payload, actor, role)
        self.repository.apply_action(
            item_id, action, actor, role, new_status, new_payload, event_payload, expected_version
        )
        return self.get_item(item_id)

    def get_item(self, item_id):
        item = self.repository.get_item(item_id)
        item["sources"] = self.repository.list_sources(item_id)
        item["audit"] = self.repository.audit_trail(item_id)
        item["assessment"] = rules.assess(item["payload"])
        return item

    def list_items(self, status=None):
        return self.repository.list_items(status)

    def state(self):
        return self.repository.state_summary()

    # ------------------------------------------------------------------
    # 管网拓扑：阀门登记与管段连接
    # ------------------------------------------------------------------

    def topology(self):
        valves, segments, links = self.repository.load_topology()
        topo_rev, _pos_rev = self.repository.revisions()
        return {"revision": topo_rev, "valves": valves, "segments": segments,
                "links": [{"valve_a": a, "valve_b": b, "segment_id": sid} for a, b, sid in links]}

    def _load_topo(self):
        return Topology(*self.repository.load_topology())

    def register_valve(self, payload, actor, role):
        if not actor or not role:
            raise DomainError("identity_required", "需要用户身份和角色", 401)
        if role not in VALVE_ROLES:
            raise DomainError("forbidden", "当前角色不能登记阀门", 403)
        normalized = domain.normalize_valve(payload)
        self.repository.upsert_valve(normalized["valve_id"], normalized["kind"], actor, role)
        return normalized

    def put_segment(self, segment_id, payload, actor, role):
        if not actor or not role:
            raise DomainError("identity_required", "需要用户身份和角色", 401)
        if role not in SEGMENT_ROLES:
            raise DomainError("forbidden", "当前角色不能维护管段连接", 403)
        normalized = domain.normalize_segment_links(payload, segment_id)
        result = self.repository.put_segment(
            normalized["segment_id"], normalized["sequence"], normalized["links"], actor, role
        )
        # 连接关系更新会推进 topology_revision；受影响管段上未执行的边界在读取时
        # 自动判为 stale，已关阀门步骤不受影响（见 _job_view）。
        return result

    # ------------------------------------------------------------------
    # 阀门回执（重复回执只记一次）
    # ------------------------------------------------------------------

    def record_receipt(self, payload, actor, role):
        if not actor or not role:
            raise DomainError("identity_required", "需要用户身份和角色", 401)
        if role not in RECEIPT_ROLES:
            raise DomainError("forbidden", "当前角色不能提交阀门回执", 403)
        normalized = domain.normalize_receipt(payload)
        receipt = self.repository.record_receipt(normalized, actor, role)
        # 回执推进 position_revision：涉及活跃边界阀门时，未执行边界读取时判 stale
        return receipt

    # ------------------------------------------------------------------
    # 隔离作业
    # ------------------------------------------------------------------

    def _job_view(self, job, topo=None, latest=None, stale_against=None, ignore_receipt_id=None):
        current_topo_rev, current_pos_rev = self.repository.revisions()
        check_topo_rev, check_pos_rev = stale_against or (current_topo_rev, current_pos_rev)
        payload = job["payload"]
        latest = latest if latest is not None else self.repository.latest_receipts()
        if ignore_receipt_id is not None:
            latest = {v: r for v, r in latest.items() if r.get("id") != ignore_receipt_id}
        stale_reasons = []
        if job["status"] in (JOB_PENDING, JOB_PAUSED):
            if int(job["topology_revision"]) != check_topo_rev:
                stale_reasons.append("topology_changed")
            if int(job["position_revision"]) != check_pos_rev:
                boundary_valves = {s["valve"] for ss in payload["sides"].values()
                                   for s in ss if s["status"] != "closed"}
                for valve in sorted(boundary_valves):
                    receipt = latest.get(valve)
                    if receipt and receipt.get("result") in ("closed", "stuck", "mismatch"):
                        stale_reasons.append("valve_position:%s" % valve)
                        break
        current = current_step(payload)
        view = {
            "id": job["id"],
            "segment_id": job["segment_id"],
            "item_id": job["item_id"],
            "status": job["status"],
            "version": job["version"],
            "owner": job["created_by"],
            "boundary": payload.get("boundary"),
            "active_boundary": active_boundary(payload),
            "sides": payload.get("sides"),
            "next_valve": current[2]["valve"] if current else None,
            "closed_valves": closed_valves(payload),
            "stale": bool(stale_reasons),
            "stale_reasons": stale_reasons,
            "revisions": {
                "topology": int(job["topology_revision"]),
                "position": int(job["position_revision"]),
                "current_topology": current_topo_rev,
                "current_position": current_pos_rev,
            },
            "history": payload.get("history", []),
            "failures": payload.get("failures", []),
        }
        return view

    def _enforce_fresh(self, view, action):
        if view["stale"]:
            raise ConflictError(
                "boundary_stale",
                "连接关系或阀位回执已更新，未执行的边界已失效，请先 recompute 再 %s" % action,
                {"job_id": view["id"], "stale_reasons": view["stale_reasons"],
                 "active_boundary": view["active_boundary"]},
            )

    def _enqueue_job_receipt(self, job, receipt, actor, role, expected_version, baseline):
        """作业内按顺序关阀：处理当前步骤回执，失败则停在断点。"""
        payload = job["payload"]
        view = self._job_view(job, stale_against=baseline, ignore_receipt_id=receipt.get("id"))
        self._enforce_fresh(view, "关阀")
        new_status = apply_closing_receipt(payload, receipt)
        # 作业自身产生的回执会推进版本游标：这是已知结果，不构成“边界失效”
        topo_rev, pos_rev = self.repository.revisions()
        updated = self.repository.update_job(
            job["id"], new_status, payload, expected_version,
            topology_revision=topo_rev, position_revision=pos_rev,
            event_type="valve_closed" if receipt["result"] == "closed" else "close_failed",
            event_payload={
                "job_id": job["id"], "valve_id": receipt["valve_id"],
                "result": receipt["result"], "receipt_id": receipt.get("id"),
                "status": new_status,
            },
            actor=actor, role=role,
        )
        return self.get_job(updated["id"])

    def create_isolation_job(self, segment_id, payload, actor, role, expected_version=None, region=None):
        if not actor or not role:
            raise DomainError("identity_required", "需要用户身份和角色", 401)
        if role not in JOB_CREATE_ROLES:
            raise DomainError("forbidden", "当前角色不能提交隔离边界", 403)
        boundary = normalize_boundary(payload)
        item_id = payload.get("item_id")
        if item_id is not None and not isinstance(item_id, int):
            raise DomainError("invalid_item_id", "item_id 必须是整数")
        topo = self._load_topo()
        validate_proposal(topo, segment_id, boundary)
        topo_rev, pos_rev = self.repository.revisions()
        ipayload = initial_payload(segment_id, boundary, actor)
        # 同一管段抢边界：只让一方继续；另一方拿到现用阀门与差异
        try:
            job = self.repository.create_job(
                segment_id, item_id, JOB_PENDING, ipayload, actor, role, topo_rev, pos_rev
            )
        except ConflictError as exc:
            details = exc.details or {}
            active = details.get("active_boundary")
            submitted = details.get("submitted_boundary")
            if active is not None and submitted is not None:
                details["diff"] = boundary_diff(active, submitted)
            details["can_resume"] = details.get("active_status") in ACTIVE_STATUSES
            raise ConflictError(exc.code, str(exc), details)
        return self.get_job(job["id"])

    def get_job(self, job_id):
        job = self.repository.get_job(job_id)
        return self._job_view(job)

    def get_active_job(self, segment_id):
        job = self.repository.get_active_job(segment_id)
        if job is None:
            return None
        return self._job_view(job)

    def list_jobs(self, segment_id=None, active_only=False):
        latest = self.repository.latest_receipts()
        return [self._job_view(job, latest=latest) for job in
                self.repository.list_jobs(segment_id, active_only)]

    def _require_owner(self, job, actor, action):
        if actor != job["created_by"]:
            raise DomainError(
                "not_job_owner",
                "只有边界提交人 %s 可以 %s（当前 %s）" % (job["created_by"], action, actor),
                403,
            )

    def _get_owned_job(self, job_id, actor, role, action, expected_version):
        if not actor or not role:
            raise DomainError("identity_required", "需要用户身份和角色", 401)
        if role not in JOB_CREATE_ROLES:
            raise DomainError("forbidden", "当前角色不能执行该隔离作业操作", 403)
        if expected_version is None:
            raise DomainError("expected_version_required", "该操作需要 expected_version", 400)
        job = self.repository.get_job(job_id)
        self._require_owner(job, actor, action)
        return job

    def close_valve(self, job_id, payload, actor, role, expected_version=None):
        """提交当前待关阀门的回执并推进作业。"""
        job = self._get_owned_job(job_id, actor, role, "关阀", expected_version)
        receipt_input = domain.normalize_receipt(payload)
        # 边界已失效时必须先 recompute，不能让新回执写入再污染重算依据
        self._enforce_fresh(self._job_view(job), "关阀")
        # 以回执写入前的版本作为失效判定基准，避免把本次回执误判为“外部更新”
        baseline = self.repository.revisions()
        stored = self.repository.record_receipt(receipt_input, actor, role)
        if stored.get("duplicate"):
            # 重复回执只记一次：不重复驱动作业，返回当前视图
            return self.get_job(job_id)
        return self._enqueue_job_receipt(job, stored, actor, role, expected_version, baseline)

    def resume_job(self, job_id, payload, actor, role, expected_version=None):
        """从断点恢复：已关阀保持关闭，改选相邻阀组成新边界。"""
        job = self._get_owned_job(job_id, actor, role, "resume", expected_version)
        if job["status"] != JOB_PAUSED:
            raise DomainError("job_not_paused", "只有停在断点的作业才能 resume（当前 %s）" % job["status"], 409)
        replacements = payload.get("replacements") if isinstance(payload, dict) else None
        if replacements is not None and not isinstance(replacements, dict):
            raise DomainError("invalid_replacements", "replacements 必须是 {失败阀: 替代阀}")
        topo = self._load_topo()
        pload = job["payload"]
        new_status = engine_resume(topo, pload, replacements, actor)
        topo_rev, pos_rev = self.repository.revisions()
        updated = self.repository.update_job(
            job["id"], new_status, pload, expected_version,
            topology_revision=topo_rev, position_revision=pos_rev,
            event_type="boundary_resumed",
            event_payload={
                "job_id": job["id"], "status": new_status,
                "active_boundary": active_boundary(pload),
                "closed_valves": closed_valves(pload),
            },
            actor=actor, role=role,
        )
        return self.get_job(updated["id"])

    def recompute_job(self, job_id, payload, actor, role, expected_version=None):
        """边界失效后重算：保留已关阀，只重算未执行步骤。"""
        job = self._get_owned_job(job_id, actor, role, "recompute", expected_version)
        if job["status"] not in (JOB_PENDING, JOB_PAUSED):
            raise DomainError("job_not_recomputable", "当前状态 %s 不需要重算边界" % job["status"], 409)
        topo = self._load_topo()
        latest = self.repository.latest_receipts()
        pload = job["payload"]
        new_status = engine_recompute(topo, pload, latest, actor)
        topo_rev, pos_rev = self.repository.revisions()
        updated = self.repository.update_job(
            job["id"], new_status, pload, expected_version,
            topology_revision=topo_rev, position_revision=pos_rev,
            event_type="boundary_recomputed",
            event_payload={
                "job_id": job["id"], "status": new_status,
                "active_boundary": active_boundary(pload),
                "closed_valves": closed_valves(pload),
            },
            actor=actor, role=role,
        )
        return self.get_job(updated["id"])

    def abandon_job(self, job_id, payload, actor, role, expected_version=None):
        """作废作业。已关阀门不会自动重新打开（安全起见需另行显式处置）。"""
        job = self._get_owned_job(job_id, actor, role, "abandon", expected_version)
        if job["status"] not in ACTIVE_STATUSES:
            raise DomainError("job_not_active", "作业已结束，不能作废", 409)
        reason = domain.require_text(payload or {}, "reason")
        updated = self.repository.update_job(
            job["id"], JOB_ABANDONED, job["payload"], expected_version,
            event_type="isolation_job_abandoned",
            event_payload={"job_id": job["id"], "reason": reason,
                           "closed_valves": closed_valves(job["payload"])},
            actor=actor, role=role,
        )
        return self.get_job(updated["id"])
