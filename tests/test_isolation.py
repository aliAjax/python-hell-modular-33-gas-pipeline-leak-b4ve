import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from src.repository import Repository
from src.service import Service
from src.domain import ConflictError, BoundaryConflictError


def make_item(service, segment_id, pipeline_id="P-1", reported_at="2026-09-27T08:00:00+00:00"):
    return service.create_item({
        "pipeline_id": pipeline_id,
        "segment_id": segment_id,
        "reported_at": reported_at,
        "pressure_drop_kpa": 30,
        "sensor_value_ppm": 120,
        "odor_reports": 3,
        "reporter": "dispatch-1",
    }, "dispatch-1", "dispatcher")


def verify(service, item):
    return service.act(item["id"], "verify", {"field_confirmed": True}, "resp-1", "responder", item["version"])


class IsolationTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self.tmp.close()
        self.repo = Repository(self.tmp.name)
        self.repo.initialize()
        self.service = Service(self.repo)
        self._build_network()

    def tearDown(self):
        os.unlink(self.tmp.name)

    def _build_network(self):
        # S-1 的边界阀：V-1(->S-2)、V-2(->S-3)、V-3(->S-4)
        # S-3 外侧相邻阀：V-4(->S-5)、V-5(->S-6)
        s = self.service
        s.upsert_segment({"segment_id": "S-1", "pipeline_id": "P-1"}, "u", "dispatcher")
        s.upsert_segment({"segment_id": "S-2", "pipeline_id": "P-1"}, "u", "dispatcher")
        s.upsert_segment({"segment_id": "S-3", "pipeline_id": "P-1"}, "u", "dispatcher")
        s.upsert_segment({"segment_id": "S-4", "pipeline_id": "P-1"}, "u", "dispatcher")
        s.upsert_segment({"segment_id": "S-5", "pipeline_id": "P-1"}, "u", "dispatcher")
        s.upsert_segment({"segment_id": "S-6", "pipeline_id": "P-1"}, "u", "dispatcher")
        s.upsert_connection({"valve_id": "V-1", "segment_a": "S-1", "segment_b": "S-2"}, "u", "dispatcher")
        s.upsert_connection({"valve_id": "V-2", "segment_a": "S-1", "segment_b": "S-3"}, "u", "dispatcher")
        s.upsert_connection({"valve_id": "V-3", "segment_a": "S-1", "segment_b": "S-4"}, "u", "dispatcher")
        s.upsert_connection({"valve_id": "V-4", "segment_a": "S-3", "segment_b": "S-5"}, "u", "dispatcher")
        s.upsert_connection({"valve_id": "V-5", "segment_a": "S-3", "segment_b": "S-6"}, "u", "dispatcher")

    def _isolate(self, segment_id, sequence, actor="sup-1"):
        item = verify(self.service, make_item(self.service, segment_id))
        return self.service.act(item["id"], "isolate", {"valve_sequence": sequence}, actor, "supervisor", item["version"])

    def test_boundary_derived_from_connections(self):
        item = self._isolate("S-1", ["V-1", "V-2", "V-3"])
        job = item["isolation_job"]
        self.assertEqual(job["boundary"], ["V-1", "V-2", "V-3"])
        self.assertEqual(job["region"], ["S-1"])
        self.assertEqual(job["current_valve"], "V-1")

    def test_close_valves_in_order(self):
        item = self._isolate("S-1", ["V-1", "V-2", "V-3"])
        job = item["isolation_job"]
        for valve in ("V-1", "V-2", "V-3"):
            result = self.service.report_valve_result(
                job["id"], {"valve_id": valve, "result": "closed", "observed_at": "2026-09-27T09:0%s:00+00:00" % valve[-1]},
                "field-1", "responder")
            job = result["job"]
        self.assertEqual(job["status"], "isolated")
        self.assertEqual(set(job["closed_valves"]), {"V-1", "V-2", "V-3"})

    def test_stuck_valve_replans_keeps_closed(self):
        item = self._isolate("S-1", ["V-1", "V-2", "V-3"])
        job = item["isolation_job"]
        # 先关 V-1
        r = self.service.report_valve_result(
            job["id"], {"valve_id": "V-1", "result": "closed", "observed_at": "2026-09-27T09:01:00+00:00"},
            "field-1", "responder")
        job = r["job"]
        self.assertEqual(job["closed_valves"], ["V-1"])
        # V-2 卡死 -> 从断点恢复，改选相邻阀门组成新边界
        r = self.service.report_valve_result(
            job["id"], {"valve_id": "V-2", "result": "stuck", "observed_at": "2026-09-27T09:02:00+00:00"},
            "field-1", "responder")
        job = r["job"]
        # 已关的 V-1 保持关闭
        self.assertIn("V-1", job["closed_valves"])
        # 失败的 V-2 被记录
        self.assertIn("V-2", job["failed_valves"])
        # 隔离区扩大到 S-3
        self.assertIn("S-3", job["region"])
        # 新边界包含 S-3 外侧的相邻阀门 V-4、V-5，V-1 仍在边界但已关
        self.assertIn("V-4", job["boundary"])
        self.assertIn("V-5", job["boundary"])
        # V-2 已变为内部阀门，不再在边界上
        self.assertNotIn("V-2", job["boundary"])
        # 当前阀门从断点继续（V-3）
        self.assertEqual(job["current_valve"], "V-3")
        self.assertEqual(job["status"], "replan")
        # 继续关 V-3、V-4、V-5 -> 隔离完成
        for valve in ("V-3", "V-4", "V-5"):
            r = self.service.report_valve_result(
                job["id"], {"valve_id": valve, "result": "closed", "observed_at": "2026-09-27T09:0%s:00+00:00" % valve[-1]},
                "field-1", "responder")
            job = r["job"]
        self.assertEqual(job["status"], "isolated")
        # V-1 始终保持关闭
        self.assertIn("V-1", job["closed_valves"])

    def test_mismatch_triggers_replan(self):
        item = self._isolate("S-1", ["V-1", "V-2", "V-3"])
        job = item["isolation_job"]
        r = self.service.report_valve_result(
            job["id"], {"valve_id": "V-2", "result": "mismatch", "observed_at": "2026-09-27T09:02:00+00:00"},
            "field-1", "responder")
        job = r["job"]
        self.assertIn("V-2", job["failed_valves"])
        self.assertIn("S-3", job["region"])
        self.assertEqual(job["status"], "replan")

    def test_boundary_conflict_shows_current_valves_and_diff(self):
        # A 先抢边界
        item_a = self._isolate("S-1", ["V-1", "V-2", "V-3"], actor="sup-a")
        # B 也提交同一管段的边界
        item_b = verify(self.service, make_item(self.service, "S-1", reported_at="2026-09-27T08:05:00+00:00"))
        with self.assertRaises(BoundaryConflictError) as ctx:
            self.service.act(item_b["id"], "isolate", {"valve_sequence": ["V-1", "V-2", "V-9"]},
                             "sup-b", "supervisor", item_b["version"])
        exc = ctx.exception
        self.assertEqual(exc.current_valves, ["V-1", "V-2", "V-3"])
        self.assertEqual(exc.diff["only_in_submitted"], ["V-9"])
        self.assertEqual(exc.diff["only_in_current"], ["V-3"])
        self.assertEqual(exc.diff["common"], ["V-1", "V-2"])
        self.assertIsNotNone(exc.job_id)

    def test_connection_change_recomputes_unexecuted_boundary(self):
        item = self._isolate("S-1", ["V-1", "V-2", "V-3"])
        job = item["isolation_job"]
        # 关 V-1
        r = self.service.report_valve_result(
            job["id"], {"valve_id": "V-1", "result": "closed", "observed_at": "2026-09-27T09:01:00+00:00"},
            "field-1", "responder")
        job = r["job"]
        self.assertEqual(job["closed_valves"], ["V-1"])
        # 新增一条连接：S-1 到 S-7 的新阀门 V-6
        s = self.service
        s.upsert_segment({"segment_id": "S-7", "pipeline_id": "P-1"}, "u", "dispatcher")
        s.upsert_connection({"valve_id": "V-6", "segment_a": "S-1", "segment_b": "S-7"}, "u", "dispatcher")
        # 未执行的边界失效重算：V-6 进入边界，已关的 V-1 保留
        job = self.service.get_job_for_item(item["id"])
        self.assertIn("V-6", job["boundary"])
        self.assertIn("V-1", job["closed_valves"])
        self.assertNotIn("V-6", job["closed_valves"])
        self.assertEqual(job["current_valve"], "V-2")

    def test_duplicate_receipt_recorded_once(self):
        item = self._isolate("S-1", ["V-1", "V-2", "V-3"])
        job = item["isolation_job"]
        payload = {"valve_id": "V-1", "result": "closed", "observed_at": "2026-09-27T09:01:00+00:00"}
        r1 = self.service.report_valve_result(job["id"], payload, "field-1", "responder")
        self.assertTrue(r1["receipt_recorded"])
        # 重复提交同一回执
        r2 = self.service.report_valve_result(job["id"], payload, "field-1", "responder")
        self.assertFalse(r2["receipt_recorded"])
        self.assertEqual(r1["receipt"]["id"], r2["receipt"]["id"])
        # 只记一次
        receipts = self.service.get_valve("V-1")["receipts"]
        self.assertEqual(len(receipts), 1)

    def test_receipt_endpoint_idempotent(self):
        r1 = self.service.add_valve_receipt("V-1", {"position": "closed", "observed_at": "2026-09-27T09:01:00+00:00"},
                                            "field-1", "responder")
        self.assertTrue(r1["receipt_recorded"])
        r2 = self.service.add_valve_receipt("V-1", {"position": "closed", "observed_at": "2026-09-27T09:01:00+00:00"},
                                            "field-1", "responder")
        self.assertFalse(r2["receipt_recorded"])
        self.assertEqual(r1["receipt"]["id"], r2["receipt"]["id"])

    def test_state_persists_across_restart(self):
        item = self._isolate("S-1", ["V-1", "V-2", "V-3"])
        job = item["isolation_job"]
        r = self.service.report_valve_result(
            job["id"], {"valve_id": "V-1", "result": "closed", "observed_at": "2026-09-27T09:01:00+00:00"},
            "field-1", "responder")
        self.assertEqual(r["job"]["closed_valves"], ["V-1"])

        # 模拟服务重启：新建 repo/service
        repo2 = Repository(self.tmp.name)
        service2 = Service(repo2)
        job2 = service2.get_job_for_item(item["id"])
        self.assertEqual(job2["closed_valves"], ["V-1"])
        self.assertEqual(job2["status"], "in_progress")
        # 重启后还能接着做：报告 V-2 卡死，恢复继续
        r = service2.report_valve_result(
            job2["id"], {"valve_id": "V-2", "result": "stuck", "observed_at": "2026-09-27T09:02:00+00:00"},
            "field-1", "responder")
        job2 = r["job"]
        self.assertIn("V-1", job2["closed_valves"])
        self.assertIn("S-3", job2["region"])
        self.assertEqual(job2["status"], "replan")

    def test_terminal_valve_stuck_cannot_isolate(self):
        # 给 S-1 加一个尽头阀门 V-7（segment_b 为空）
        s = self.service
        s.upsert_connection({"valve_id": "V-7", "segment_a": "S-1", "segment_b": None}, "u", "dispatcher")
        item = self._isolate("S-1", ["V-1", "V-2", "V-3", "V-7"])
        job = item["isolation_job"]
        r = self.service.report_valve_result(
            job["id"], {"valve_id": "V-7", "result": "stuck", "observed_at": "2026-09-27T09:02:00+00:00"},
            "field-1", "responder")
        job = r["job"]
        self.assertEqual(job["status"], "isolation_failed")


if __name__ == "__main__":
    unittest.main()
