import json
import sqlite3
from datetime import datetime, timezone

from .audit import audit_hash, canonical_json
from .domain import ConflictError, NotFoundError, DomainError
from .network import Graph


def now_iso():
    return datetime.now(timezone.utc).isoformat()


class Repository:
    def __init__(self, path):
        self.path = path

    def connect(self):
        conn = sqlite3.connect(self.path, timeout=30, isolation_level=None)
        conn.row_factory = sqlite3.Row
        return conn

    def initialize(self):
        conn = self.connect()
        try:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS items (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    entity_type TEXT NOT NULL,
                    stable_key TEXT NOT NULL,
                    status TEXT NOT NULL,
                    version INTEGER NOT NULL DEFAULT 1,
                    payload TEXT NOT NULL,
                    created_by TEXT NOT NULL,
                    created_role TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    UNIQUE(entity_type, stable_key)
                );
                CREATE TABLE IF NOT EXISTS sources (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    item_id INTEGER NOT NULL,
                    source_type TEXT NOT NULL,
                    external_id TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    observed_at TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    UNIQUE(item_id, source_type, external_id),
                    FOREIGN KEY(item_id) REFERENCES items(id)
                );
                CREATE TABLE IF NOT EXISTS actions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    item_id INTEGER NOT NULL,
                    action TEXT NOT NULL,
                    actor TEXT NOT NULL,
                    role TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY(item_id) REFERENCES items(id)
                );
                CREATE TABLE IF NOT EXISTS audit_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    item_id INTEGER,
                    event_type TEXT NOT NULL,
                    actor TEXT,
                    role TEXT,
                    payload TEXT NOT NULL,
                    previous_hash TEXT NOT NULL,
                    event_hash TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS segments (
                    segment_id TEXT PRIMARY KEY,
                    pipeline_id TEXT NOT NULL,
                    name TEXT,
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS valves (
                    valve_id TEXT PRIMARY KEY,
                    name TEXT,
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS connections (
                    valve_id TEXT PRIMARY KEY,
                    segment_a TEXT,
                    segment_b TEXT,
                    updated_at TEXT NOT NULL,
                    FOREIGN KEY(valve_id) REFERENCES valves(valve_id)
                );
                CREATE TABLE IF NOT EXISTS valve_receipts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    valve_id TEXT NOT NULL,
                    position TEXT NOT NULL,
                    outcome TEXT,
                    observed_at TEXT NOT NULL,
                    note TEXT,
                    payload TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    UNIQUE(valve_id, observed_at),
                    FOREIGN KEY(valve_id) REFERENCES valves(valve_id)
                );
                CREATE TABLE IF NOT EXISTS isolation_jobs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    item_id INTEGER NOT NULL UNIQUE,
                    segment_id TEXT NOT NULL,
                    pipeline_id TEXT,
                    status TEXT NOT NULL,
                    version INTEGER NOT NULL DEFAULT 1,
                    region TEXT NOT NULL,
                    boundary TEXT NOT NULL,
                    planned_sequence TEXT NOT NULL,
                    closed_valves TEXT NOT NULL,
                    failed_valves TEXT NOT NULL,
                    current_valve TEXT,
                    created_by TEXT,
                    created_role TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    FOREIGN KEY(item_id) REFERENCES items(id)
                );
                CREATE UNIQUE INDEX IF NOT EXISTS idx_isolation_jobs_active_segment
                    ON isolation_jobs(segment_id)
                    WHERE status IN ('planned','in_progress','replan');
                """
            )
        finally:
            conn.close()

    def _row_to_item(self, row):
        if row is None:
            return None
        result = dict(row)
        result["payload"] = json.loads(result["payload"])
        return result

    def _last_hash(self, conn, item_id):
        row = conn.execute(
            "SELECT event_hash FROM audit_events WHERE item_id IS ? ORDER BY id DESC LIMIT 1",
            (item_id,),
        ).fetchone()
        return row["event_hash"] if row else "GENESIS"

    def append_audit(self, conn, item_id, event_type, actor, role, payload):
        previous = self._last_hash(conn, item_id)
        event = {
            "item_id": item_id,
            "event_type": event_type,
            "actor": actor,
            "role": role,
            "payload": payload,
            "created_at": now_iso(),
        }
        event_hash = audit_hash(previous, event)
        conn.execute(
            "INSERT INTO audit_events(item_id,event_type,actor,role,payload,previous_hash,event_hash,created_at) VALUES(?,?,?,?,?,?,?,?)",
            (item_id, event_type, actor, role, canonical_json(payload), previous, event_hash, event["created_at"]),
        )

    def create_item(self, entity_type, stable_key, initial_status, payload, actor, role):
        conn = self.connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            try:
                conn.execute(
                    "INSERT INTO items(entity_type,stable_key,status,version,payload,created_by,created_role,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?)",
                    (
                        entity_type,
                        stable_key,
                        initial_status,
                        1,
                        canonical_json(payload),
                        actor,
                        role,
                        now_iso(),
                        now_iso(),
                    ),
                )
            except sqlite3.IntegrityError:
                raise ConflictError("duplicate_item", "同一业务实体已经存在")
            item_id = conn.execute("SELECT last_insert_rowid() AS id").fetchone()["id"]
            self.append_audit(conn, item_id, "created", actor, role, {"stable_key": stable_key})
            conn.execute("COMMIT")
            return self.get_item(item_id)
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        finally:
            conn.close()

    def get_item(self, item_id):
        conn = self.connect()
        try:
            row = conn.execute("SELECT * FROM items WHERE id=?", (item_id,)).fetchone()
            if row is None:
                raise NotFoundError("item_not_found", "业务实体不存在")
            return self._row_to_item(row)
        finally:
            conn.close()

    def list_items(self, status=None):
        conn = self.connect()
        try:
            if status:
                rows = conn.execute("SELECT * FROM items WHERE status=? ORDER BY id DESC", (status,)).fetchall()
            else:
                rows = conn.execute("SELECT * FROM items ORDER BY id DESC").fetchall()
            return [self._row_to_item(row) for row in rows]
        finally:
            conn.close()

    def add_source(self, item_id, source_type, external_id, payload, observed_at, actor, role):
        conn = self.connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            item = conn.execute("SELECT id FROM items WHERE id=?", (item_id,)).fetchone()
            if item is None:
                raise NotFoundError("item_not_found", "业务实体不存在")
            try:
                conn.execute(
                    "INSERT INTO sources(item_id,source_type,external_id,payload,observed_at,created_at) VALUES(?,?,?,?,?,?)",
                    (item_id, source_type, external_id, canonical_json(payload), observed_at, now_iso()),
                )
            except sqlite3.IntegrityError:
                raise ConflictError("duplicate_source", "同一来源记录已经提交")
            source_id = conn.execute("SELECT last_insert_rowid() AS id").fetchone()["id"]
            self.append_audit(
                conn,
                item_id,
                "source_recorded",
                actor,
                role,
                {"source_id": source_id, "source_type": source_type, "external_id": external_id},
            )
            conn.execute("COMMIT")
            return {"id": source_id, "item_id": item_id, "source_type": source_type, "external_id": external_id, "payload": payload, "observed_at": observed_at}
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        finally:
            conn.close()

    def list_sources(self, item_id):
        conn = self.connect()
        try:
            rows = conn.execute("SELECT * FROM sources WHERE item_id=? ORDER BY id DESC", (item_id,)).fetchall()
            result = []
            for row in rows:
                value = dict(row)
                value["payload"] = json.loads(value["payload"])
                result.append(value)
            return result
        finally:
            conn.close()

    def apply_action(self, item_id, action, actor, role, new_status, new_payload, event_payload, expected_version=None):
        conn = self.connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute("SELECT * FROM items WHERE id=?", (item_id,)).fetchone()
            if row is None:
                raise NotFoundError("item_not_found", "业务实体不存在")
            if expected_version is not None and int(expected_version) != int(row["version"]):
                raise ConflictError("version_conflict", "记录已被其他操作更新，请重新读取")
            version = int(row["version"]) + 1
            conn.execute(
                "UPDATE items SET status=?,version=?,payload=?,updated_at=? WHERE id=?",
                (new_status, version, canonical_json(new_payload), now_iso(), item_id),
            )
            conn.execute(
                "INSERT INTO actions(item_id,action,actor,role,payload,created_at) VALUES(?,?,?,?,?,?)",
                (item_id, action, actor, role, canonical_json(event_payload), now_iso()),
            )
            self.append_audit(conn, item_id, action, actor, role, event_payload)
            conn.execute("COMMIT")
            return self.get_item(item_id)
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        finally:
            conn.close()

    def apply_isolate(self, item_id, actor, role, new_status, new_payload, event_payload, expected_version, job):
        """状态更新与隔离作业创建在同一事务；抢边界失败则整单回滚。"""
        conn = self.connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute("SELECT * FROM items WHERE id=?", (item_id,)).fetchone()
            if row is None:
                raise NotFoundError("item_not_found", "业务实体不存在")
            if expected_version is not None and int(expected_version) != int(row["version"]):
                raise ConflictError("version_conflict", "记录已被其他操作更新，请重新读取")
            version = int(row["version"]) + 1
            conn.execute(
                "UPDATE items SET status=?,version=?,payload=?,updated_at=? WHERE id=?",
                (new_status, version, canonical_json(new_payload), now_iso(), item_id),
            )
            conn.execute(
                "INSERT INTO actions(item_id,action,actor,role,payload,created_at) VALUES(?,?,?,?,?,?)",
                (item_id, "isolate", actor, role, canonical_json(event_payload), now_iso()),
            )
            self.append_audit(conn, item_id, "isolate", actor, role, event_payload)
            try:
                conn.execute(
                    """INSERT INTO isolation_jobs
                       (item_id,segment_id,pipeline_id,status,version,region,boundary,
                        planned_sequence,closed_valves,failed_valves,current_valve,
                        created_by,created_role,created_at,updated_at)
                       VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (
                        job["item_id"],
                        job["segment_id"],
                        job.get("pipeline_id"),
                        job["status"],
                        job["version"],
                        canonical_json(job["region"]),
                        canonical_json(job["boundary"]),
                        canonical_json(job["planned_sequence"]),
                        canonical_json(job["closed_valves"]),
                        canonical_json(job["failed_valves"]),
                        job.get("current_valve"),
                        job.get("created_by"),
                        job.get("created_role"),
                        job["created_at"],
                        job["updated_at"],
                    ),
                )
            except sqlite3.IntegrityError:
                raise ConflictError("boundary_conflict", "该管段已有进行中的隔离作业")
            job_id = conn.execute("SELECT last_insert_rowid() AS id").fetchone()["id"]
            self.append_audit(
                conn,
                item_id,
                "isolation_planned",
                actor,
                role,
                {"job_id": job_id, "segment_id": job["segment_id"], "boundary": job["boundary"]},
            )
            conn.execute("COMMIT")
            return self.get_item(item_id)
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        finally:
            conn.close()

    def audit_trail(self, item_id):
        conn = self.connect()
        try:
            rows = conn.execute("SELECT * FROM audit_events WHERE item_id=? ORDER BY id", (item_id,)).fetchall()
            result = []
            for row in rows:
                value = dict(row)
                value["payload"] = json.loads(value["payload"])
                result.append(value)
            return result
        finally:
            conn.close()

    def state_summary(self):
        conn = self.connect()
        try:
            counts = {}
            for row in conn.execute("SELECT status, COUNT(*) AS total FROM items GROUP BY status").fetchall():
                counts[row["status"]] = row["total"]
            return {"counts": counts, "items": self.list_items()}
        finally:
            conn.close()

    # ------------------------------------------------------------------
    # 管网拓扑：管段、阀门、连接关系
    # ------------------------------------------------------------------

    def upsert_segment(self, segment_id, pipeline_id, name=None):
        conn = self.connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(
                """INSERT INTO segments(segment_id,pipeline_id,name,created_at) VALUES(?,?,?,?)
                   ON CONFLICT(segment_id) DO UPDATE SET pipeline_id=excluded.pipeline_id, name=excluded.name""",
                (segment_id, pipeline_id, name, now_iso()),
            )
            conn.execute("COMMIT")
            return {"segment_id": segment_id, "pipeline_id": pipeline_id, "name": name}
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        finally:
            conn.close()

    def upsert_valve(self, valve_id, name=None):
        conn = self.connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(
                """INSERT INTO valves(valve_id,name,created_at) VALUES(?,?,?)
                   ON CONFLICT(valve_id) DO UPDATE SET name=excluded.name""",
                (valve_id, name, now_iso()),
            )
            conn.execute("COMMIT")
            return {"valve_id": valve_id, "name": name}
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        finally:
            conn.close()

    def upsert_connection(self, valve_id, segment_a, segment_b):
        conn = self.connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(
                """INSERT INTO connections(valve_id,segment_a,segment_b,updated_at) VALUES(?,?,?,?)
                   ON CONFLICT(valve_id) DO UPDATE SET segment_a=excluded.segment_a,
                                                      segment_b=excluded.segment_b,
                                                      updated_at=excluded.updated_at""",
                (valve_id, segment_a, segment_b, now_iso()),
            )
            conn.execute("COMMIT")
            return {"valve_id": valve_id, "segment_a": segment_a, "segment_b": segment_b}
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        finally:
            conn.close()

    def list_connections(self):
        conn = self.connect()
        try:
            rows = conn.execute("SELECT valve_id, segment_a, segment_b FROM connections").fetchall()
            return [dict(row) for row in rows]
        finally:
            conn.close()

    def list_segments(self):
        conn = self.connect()
        try:
            rows = conn.execute("SELECT segment_id, pipeline_id, name FROM segments").fetchall()
            return [dict(row) for row in rows]
        finally:
            conn.close()

    def list_valves(self):
        conn = self.connect()
        try:
            rows = conn.execute("SELECT valve_id, name FROM valves").fetchall()
            return [dict(row) for row in rows]
        finally:
            conn.close()

    def load_graph(self):
        connections = [(c["valve_id"], c["segment_a"], c["segment_b"]) for c in self.list_connections()]
        return Graph(connections)

    # ------------------------------------------------------------------
    # 阀门回执
    # ------------------------------------------------------------------

    def add_valve_receipt(self, valve_id, position, outcome, observed_at, note, payload):
        conn = self.connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            cursor = conn.execute(
                """INSERT OR IGNORE INTO valve_receipts
                   (valve_id,position,outcome,observed_at,note,payload,created_at)
                   VALUES(?,?,?,?,?,?,?)""",
                (valve_id, position, outcome, observed_at, note, canonical_json(payload), now_iso()),
            )
            is_new = cursor.rowcount == 1
            row = conn.execute(
                "SELECT * FROM valve_receipts WHERE valve_id=? AND observed_at=?",
                (valve_id, observed_at),
            ).fetchone()
            conn.execute("COMMIT")
            return self._row_to_receipt(row), is_new
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        finally:
            conn.close()

    def _row_to_receipt(self, row):
        if row is None:
            return None
        value = dict(row)
        value["payload"] = json.loads(value["payload"])
        return value

    def list_valve_receipts(self, valve_id):
        conn = self.connect()
        try:
            rows = conn.execute(
                "SELECT * FROM valve_receipts WHERE valve_id=? ORDER BY observed_at DESC, id DESC",
                (valve_id,),
            ).fetchall()
            return [self._row_to_receipt(row) for row in rows]
        finally:
            conn.close()

    def latest_valve_position(self, valve_id):
        conn = self.connect()
        try:
            row = conn.execute(
                """SELECT position FROM valve_receipts
                   WHERE valve_id=? ORDER BY observed_at DESC, id DESC LIMIT 1""",
                (valve_id,),
            ).fetchone()
            return row["position"] if row else None
        finally:
            conn.close()

    # ------------------------------------------------------------------
    # 隔离作业
    # ------------------------------------------------------------------

    def _row_to_job(self, row):
        if row is None:
            return None
        value = dict(row)
        for key in ("region", "boundary", "planned_sequence", "closed_valves", "failed_valves"):
            value[key] = json.loads(value[key])
        return value

    def get_isolation_job(self, job_id):
        conn = self.connect()
        try:
            row = conn.execute("SELECT * FROM isolation_jobs WHERE id=?", (job_id,)).fetchone()
            if row is None:
                raise NotFoundError("isolation_job_not_found", "隔离作业不存在")
            return self._row_to_job(row)
        finally:
            conn.close()

    def get_active_job_for_segment(self, segment_id):
        conn = self.connect()
        try:
            row = conn.execute(
                """SELECT * FROM isolation_jobs
                   WHERE segment_id=? AND status IN ('planned','in_progress','replan')
                   ORDER BY id DESC LIMIT 1""",
                (segment_id,),
            ).fetchone()
            return self._row_to_job(row)
        finally:
            conn.close()

    def get_job_for_item(self, item_id):
        conn = self.connect()
        try:
            row = conn.execute("SELECT * FROM isolation_jobs WHERE item_id=? ORDER BY id DESC LIMIT 1", (item_id,)).fetchone()
            return self._row_to_job(row)
        finally:
            conn.close()

    def list_active_jobs(self):
        conn = self.connect()
        try:
            rows = conn.execute(
                "SELECT * FROM isolation_jobs WHERE status IN ('planned','in_progress','replan') ORDER BY id"
            ).fetchall()
            return [self._row_to_job(row) for row in rows]
        finally:
            conn.close()

    def list_isolation_jobs(self, segment_id=None, status=None):
        conn = self.connect()
        try:
            sql = "SELECT * FROM isolation_jobs WHERE 1=1"
            params = []
            if segment_id:
                sql += " AND segment_id=?"
                params.append(segment_id)
            if status:
                sql += " AND status=?"
                params.append(status)
            sql += " ORDER BY id DESC"
            rows = conn.execute(sql, params).fetchall()
            return [self._row_to_job(row) for row in rows]
        finally:
            conn.close()

    def update_isolation_job(self, job):
        conn = self.connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute("SELECT * FROM isolation_jobs WHERE id=?", (job["id"],)).fetchone()
            if row is None:
                raise NotFoundError("isolation_job_not_found", "隔离作业不存在")
            if int(row["version"]) != int(job["version"]):
                raise ConflictError("version_conflict", "隔离作业已被其他操作更新，请重新读取")
            version = int(row["version"]) + 1
            conn.execute(
                """UPDATE isolation_jobs SET status=?,version=?,region=?,boundary=?,
                   closed_valves=?,failed_valves=?,current_valve=?,updated_at=?
                   WHERE id=?""",
                (
                    job["status"],
                    version,
                    canonical_json(job["region"]),
                    canonical_json(job["boundary"]),
                    canonical_json(job["closed_valves"]),
                    canonical_json(job["failed_valves"]),
                    job.get("current_valve"),
                    now_iso(),
                    job["id"],
                ),
            )
            conn.execute("COMMIT")
            job["version"] = version
            return job
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        finally:
            conn.close()

