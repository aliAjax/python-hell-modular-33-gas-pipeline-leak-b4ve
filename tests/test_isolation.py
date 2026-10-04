import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from src.repository import Repository
from src.service import Service
from src.domain import ConflictError, DomainError

# 测试拓扑：
#   S-1:  V-3 -- V-1
#   S-8(目标): V-1 -- V-2
#   S-2:  V-2 -- V-4
#   S-3:  V-3 -- V-5 （失败阀向外再扩展时用）
VALVES = ["V-1", "V-2", "V-3", "V-4", "V-5", "V-6"]
SEGMENTS = {
    "S-1": ["V-3", "V-1"],
    "S-8": ["V-1", "V-2"],
    "S-2": ["V-2", "V-4"],
    "S-3": ["V-3", "V-5"],
}


class IsolationTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self.tmp.close()
        self.repo = Repository(self.tmp.name)
        self.repo.initialize()
        self.service = Service(self.repo)
        for valve in VALVES:
            self.service.register_valve({"valve_id": valve}, "sup", "supervisor")
        for sid, seq in SEGMENTS.items():
            self.service.put_segment(sid, {"valve_sequence": seq}, "sup", "supervisor")

    def tearDown(self):
        os.unlink(self.tmp.name)

    def _create_job(self, boundary=None, actor="crewA", role="supervisor"):
        boundary = boundary or {"upstream": ["V-1"], "downstream": ["V-2"]}
        return self.service.create_isolation_job("S-8", {"boundary": boundary}, actor, role)

    # ------------------------------------------------------------------
    # 需求 1：关阀失败后从断点恢复，改选相邻阀，已关阀门保持关闭
    # ------------------------------------------------------------------

    def test_failure_keeps_closed_valves_and_resumes_with_adjacent(self):
        job = self._create_job()
        job = self.service.close_valve(job["id"], {
            "valve_id": "V-1", "result": "closed", "idempotency_key": "rc-1",
        }, "crewA", "supervisor", job["version"])
        self.assertEqual(job["closed_valves"], ["V-1"])
        # V-2 卡死：作业停在断点，而不是整单取消
        job = self.service.close_valve(job["id"], {
            "valve_id": "V-2", "result": "stuck", "idempotency_key": "rc-2",
        }, "crewA", "supervisor", job["version"])
        self.assertEqual(job["status"], "paused")
        self.assertEqual(job["next_valve"], None)
        self.assertEqual(job["closed_valves"], ["V-1"])
        # 不能跳过断点继续关别的阀
        with self.assertRaises(DomainError) as ctx:
            self.service.close_valve(job["id"], {
                "valve_id": "V-4", "result": "closed", "idempotency_key": "rc-x",
            }, "crewA", "supervisor", job["version"])
        self.assertEqual(ctx.exception.code, "job_paused")
        # 恢复：失败阀 V-2 保留 failed 记录，自动改选相邻（且不在目标管段上）的 V-4
        job = self.service.resume_job(job["id"], {}, "crewA", "supervisor", job["version"])
        self.assertEqual(job["status"], "pending_close")
        self.assertEqual(job["next_valve"], "V-4")
        self.assertEqual(job["active_boundary"], {"upstream": ["V-1"], "downstream": ["V-4"]})
        # 已关的上游 V-1 没有被重新打开
        self.assertEqual(job["closed_valves"], ["V-1"])
        sides = job["sides"]["downstream"]
        self.assertEqual([(s["valve"], s["status"]) for s in sides], [("V-2", "failed"), ("V-4", "pending")])
        # 关上替代阀后作业隔离完成
        job = self.service.close_valve(job["id"], {
            "valve_id": "V-4", "result": "closed", "idempotency_key": "rc-3",
        }, "crewA", "supervisor", job["version"])
        self.assertEqual(job["status"], "isolated")
        self.assertEqual(sorted(job["closed_valves"]), ["V-1", "V-4"])

    def test_receipt_mismatch_pauses_at_breakpoint(self):
        job = self._create_job()
        job = self.service.close_valve(job["id"], {
            "valve_id": "V-1", "result": "mismatch",
            "expected_position": "closed", "reported_position": "open", "idempotency_key": "rm-1",
        }, "crewA", "supervisor", job["version"])
        self.assertEqual(job["status"], "paused")
        # 值班员显式指定相邻替代阀 V-3（V-1 沿 S-1 向外）
        job = self.service.resume_job(job["id"], {"replacements": {"V-1": "V-3"}},
                                      "crewA", "supervisor", job["version"])
        self.assertEqual(job["next_valve"], "V-3")

    def test_replacement_must_be_adjacent_off_target_segment(self):
        job = self._create_job()
        job = self.service.close_valve(job["id"], {
            "valve_id": "V-1", "result": "stuck", "idempotency_key": "ra-1",
        }, "crewA", "supervisor", job["version"])
        # V-2 与 V-1 相邻但在目标管段 S-8 上，不能作为替代
        with self.assertRaises(DomainError) as ctx:
            self.service.resume_job(job["id"], {"replacements": {"V-1": "V-2"}},
                                    "crewA", "supervisor", job["version"])
        self.assertEqual(ctx.exception.code, "replacement_not_adjacent")

    def test_out_of_order_valve_rejected(self):
        job = self._create_job()
        with self.assertRaises(DomainError) as ctx:
            self.service.close_valve(job["id"], {
                "valve_id": "V-2", "result": "closed", "idempotency_key": "ro-1",
            }, "crewA", "supervisor", job["version"])
        self.assertEqual(ctx.exception.code, "out_of_order_valve")

    # ------------------------------------------------------------------
    # 需求 2：同一管段抢边界，只让一方继续，另一方看到现用阀门与差异
    # ------------------------------------------------------------------

    def test_concurrent_boundary_only_one_wins_with_diff(self):
        first = self._create_job(actor="crewA")
        self.assertEqual(first["owner"], "crewA")
        with self.assertRaises(ConflictError) as ctx:
            self.service.create_isolation_job(
                "S-8",
                {"boundary": {"upstream": ["V-3"], "downstream": ["V-2"]}},
                "crewB", "supervisor",
            )
        self.assertEqual(ctx.exception.code, "active_job_exists")
        details = ctx.exception.details
        self.assertEqual(details["active_job_id"], first["id"])
        self.assertEqual(details["active_owner"], "crewA")
        self.assertEqual(details["active_boundary"],
                         {"upstream": ["V-1"], "downstream": ["V-2"]})
        self.assertEqual(details["diff"]["upstream"],
                         {"active_only": ["V-1"], "submitted_only": ["V-3"]})
        self.assertEqual(details["diff"]["downstream"],
                         {"active_only": [], "submitted_only": []})
        # 另一方读作业能看到现用阀门
        seen = self.service.get_active_job("S-8")
        self.assertEqual(seen["active_boundary"],
                         {"upstream": ["V-1"], "downstream": ["V-2"]})

    def test_only_owner_can_drive_job(self):
        job = self._create_job(actor="crewA")
        with self.assertRaises(DomainError) as ctx:
            self.service.close_valve(job["id"], {
                "valve_id": "V-1", "result": "closed", "idempotency_key": "rp-1",
            }, "crewB", "supervisor", job["version"])
        self.assertEqual(ctx.exception.status, 403)
        self.assertEqual(ctx.exception.code, "not_job_owner")

    # ------------------------------------------------------------------
    # 需求 3：连接关系/阀位更新后，未执行边界失效重算，已关结果留着
    # ------------------------------------------------------------------

    def test_topology_change_invalidates_pending_boundary_and_recompute_keeps_closed(self):
        job = self._create_job()
        job = self.service.close_valve(job["id"], {
            "valve_id": "V-1", "result": "closed", "idempotency_key": "rt-1",
        }, "crewA", "supervisor", job["version"])
        self.assertFalse(job["stale"])
        # 改连接关系：S-2 由 V-2--V-4 改为 V-2--V-6
        self.service.register_valve({"valve_id": "V-6"}, "sup", "supervisor")
        self.service.put_segment("S-2", {"valve_sequence": ["V-2", "V-6"]},
                                 "sup", "supervisor")
        view = self.service.get_job(job["id"])
        self.assertTrue(view["stale"])
        self.assertIn("topology_changed", view["stale_reasons"])
        # stale 期间不能继续关阀
        with self.assertRaises(ConflictError) as ctx:
            self.service.close_valve(view["id"], {
                "valve_id": "V-2", "result": "closed", "idempotency_key": "rt-2",
            }, "crewA", "supervisor", view["version"])
        self.assertEqual(ctx.exception.code, "boundary_stale")
        # 重算：已关 V-1 保留；V-2 仍然有效（仍挂在 S-8 上），复位为 pending
        job = self.service.recompute_job(view["id"], {}, "crewA", "supervisor", view["version"])
        self.assertFalse(job["stale"])
        self.assertEqual(job["closed_valves"], ["V-1"])
        self.assertEqual(job["next_valve"], "V-2")

    def test_external_receipt_on_pending_valve_invalidates_boundary(self):
        job = self._create_job()
        job = self.service.close_valve(job["id"], {
            "valve_id": "V-1", "result": "closed", "idempotency_key": "rx-1",
        }, "crewA", "supervisor", job["version"])
        # 现场外部回执：下一个待关阀 V-2 卡死
        self.service.record_receipt({
            "valve_id": "V-2", "result": "stuck", "idempotency_key": "rx-ext-1",
        }, "patrol-9", "patrol")
        view = self.service.get_job(job["id"])
        self.assertTrue(view["stale"])
        self.assertTrue(any(r.startswith("valve_position:V-2") for r in view["stale_reasons"]))
        # 重算：采纳失败事实，自动扩展到相邻的 V-4；已关 V-1 保留
        job = self.service.recompute_job(view["id"], {}, "crewA", "supervisor", view["version"])
        self.assertEqual(job["closed_valves"], ["V-1"])
        self.assertEqual(job["next_valve"], "V-4")
        sides = job["sides"]["downstream"]
        self.assertEqual([(s["valve"], s["status"]) for s in sides], [("V-2", "failed"), ("V-4", "pending")])

    def test_external_closed_receipt_is_adopted_as_closed_result(self):
        job = self._create_job()
        job = self.service.close_valve(job["id"], {
            "valve_id": "V-1", "result": "closed", "idempotency_key": "rd-1",
        }, "crewA", "supervisor", job["version"])
        # 外部直接上报 V-2 已关紧（现场操作完成）
        self.service.record_receipt({
            "valve_id": "V-2", "result": "closed",
            "reported_position": "closed", "idempotency_key": "rd-ext-1",
        }, "field-1", "responder")
        view = self.service.get_job(job["id"])
        self.assertTrue(view["stale"])
        job = self.service.recompute_job(view["id"], {}, "crewA", "supervisor", view["version"])
        self.assertEqual(job["status"], "isolated")
        self.assertEqual(sorted(job["closed_valves"]), ["V-1", "V-2"])

    # ------------------------------------------------------------------
    # 需求 4：服务重启后还能接着做
    # ------------------------------------------------------------------

    def test_state_survives_restart(self):
        job = self._create_job()
        job = self.service.close_valve(job["id"], {
            "valve_id": "V-1", "result": "closed", "idempotency_key": "rs-1",
        }, "crewA", "supervisor", job["version"])
        job = self.service.close_valve(job["id"], {
            "valve_id": "V-2", "result": "stuck", "idempotency_key": "rs-2",
        }, "crewA", "supervisor", job["version"])
        job_id, version = job["id"], job["version"]
        # 重启：重新打开同一个数据库
        repo2 = Repository(self.tmp.name)
        service2 = Service(repo2)
        job = service2.get_job(job_id)
        self.assertEqual(job["status"], "paused")
        self.assertEqual(job["closed_valves"], ["V-1"])
        job = service2.resume_job(job_id, {}, "crewA", "supervisor", version)
        self.assertEqual(job["next_valve"], "V-4")
        topology = service2.topology()
        self.assertIn("S-8", topology["segments"])

    # ------------------------------------------------------------------
    # 需求 5：重复回执只记一次
    # ------------------------------------------------------------------

    def test_duplicate_receipt_recorded_once(self):
        receipt = {"valve_id": "V-9", "result": "closed", "idempotency_key": "dup-key-1"}
        first = self.service.record_receipt(dict(receipt), "p", "patrol")
        second = self.service.record_receipt(dict(receipt), "p", "patrol")
        self.assertFalse(first["duplicate"])
        self.assertTrue(second["duplicate"])
        self.assertEqual(first["id"], second["id"])
        rows = self.repo.list_receipts("V-9")
        self.assertEqual(len(rows), 1)

    def test_duplicate_closing_receipt_does_not_double_apply(self):
        job = self._create_job()
        payload = {"valve_id": "V-1", "result": "closed", "idempotency_key": "dup-close-1"}
        job = self.service.close_valve(job["id"], dict(payload), "crewA", "supervisor", job["version"])
        before_version = job["version"]
        # 同样的回执再发一次：不产生新版本，作业停在 V-2
        view = self.service.close_valve(job["id"], dict(payload), "crewA", "supervisor", before_version)
        self.assertEqual(view["version"], before_version)
        self.assertEqual(view["next_valve"], "V-2")
        self.assertEqual(view["closed_valves"], ["V-1"])

    # ------------------------------------------------------------------
    # 其他：版本冲突、边界校验、作废
    # ------------------------------------------------------------------

    def test_optimistic_version_conflict(self):
        job = self._create_job()
        with self.assertRaises(ConflictError):
            self.service.close_valve(job["id"], {
                "valve_id": "V-1", "result": "closed", "idempotency_key": "rv-1",
            }, "crewA", "supervisor", job["version"] - 1)

    def test_boundary_requires_two_sides_and_known_adjacent_valves(self):
        with self.assertRaises(DomainError) as ctx:
            self.service.create_isolation_job(
                "S-8", {"boundary": {"upstream": ["V-1"]}}, "crewA", "supervisor")
        self.assertEqual(ctx.exception.code, "boundary_side_required")
        with self.assertRaises(DomainError) as ctx:
            self.service.create_isolation_job(
                "S-8", {"boundary": {"upstream": ["V-1"], "downstream": ["V-404"]}},
                "crewA", "supervisor")
        self.assertEqual(ctx.exception.code, "unknown_valve")

    def test_abandon_does_not_reopen_closed_valves(self):
        job = self._create_job()
        job = self.service.close_valve(job["id"], {
            "valve_id": "V-1", "result": "closed", "idempotency_key": "rn-1",
        }, "crewA", "supervisor", job["version"])
        job = self.service.abandon_job(job["id"], {"reason": "换方案"},
                                       "crewA", "supervisor", job["version"])
        self.assertEqual(job["status"], "abandoned")
        self.assertEqual(job["closed_valves"], ["V-1"])
        # 作废后同管段可以提交新边界
        new_job = self._create_job()
        self.assertEqual(new_job["status"], "pending_close")


if __name__ == "__main__":
    unittest.main()
