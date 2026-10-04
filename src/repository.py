import json
import sqlite3
from datetime import datetime, timezone

from .audit import audit_hash, canonical_json
from .domain import ConflictError, NotFoundError, DomainError


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
                CREATE TABLE IF NOT EXISTS topology_valves (
                    valve_id TEXT PRIMARY KEY,
                    kind TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS topology_segments (
                    segment_id TEXT PRIMARY KEY,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS topology_links (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    segment_id TEXT NOT NULL,
                    valve_a TEXT NOT NULL,
                    valve_b TEXT NOT NULL,
                    position INTEGER NOT NULL,
                    UNIQUE(segment_id, position),
                    UNIQUE(valve_a, valve_b)
                );
                CREATE TABLE IF NOT EXISTS valve_receipts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    valve_id TEXT NOT NULL,
                    result TEXT NOT NULL,
                    expected_position TEXT,
                    reported_position TEXT,
                    idempotency_key TEXT NOT NULL UNIQUE,
                    payload TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS isolation_jobs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    segment_id TEXT NOT NULL,
                    item_id INTEGER,
                    status TEXT NOT NULL,
                    version INTEGER NOT NULL DEFAULT 1,
                    payload TEXT NOT NULL,
                    topology_revision INTEGER NOT NULL,
                    position_revision INTEGER NOT NULL,
                    created_by TEXT NOT NULL,
                    created_role TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE UNIQUE INDEX IF NOT EXISTS idx_active_job_per_segment
                    ON isolation_jobs(segment_id)
                    WHERE status IN ('pending_close', 'paused', 'isolated');
                CREATE TABLE IF NOT EXISTS meta (
                    key TEXT PRIMARY KEY,
                    value INTEGER NOT NULL
                );
                INSERT OR IGNORE INTO meta(key, value) VALUES
                    ('topology_revision', 0),
                    ('position_revision', 0);
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
    # 管网拓扑
    # ------------------------------------------------------------------

    def _bump_revision(self, conn, key):
        conn.execute("UPDATE meta SET value = value + 1 WHERE key=?", (key,))
        row = conn.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
        return int(row["value"])

    def revisions(self):
        conn = self.connect()
        try:
            rows = {row["key"]: int(row["value"]) for row in conn.execute("SELECT key, value FROM meta").fetchall()}
            return rows.get("topology_revision", 0), rows.get("position_revision", 0)
        finally:
            conn.close()

    def upsert_valve(self, valve_id, kind, actor, role):
        conn = self.connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(
                """
                INSERT INTO topology_valves(valve_id,kind,created_at,updated_at) VALUES(?,?,?,?)
                ON CONFLICT(valve_id) DO UPDATE SET kind=excluded.kind, updated_at=excluded.updated_at
                """,
                (valve_id, kind, now_iso(), now_iso()),
            )
            conn.execute("COMMIT")
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        finally:
            conn.close()

    def put_segment(self, segment_id, sequence, links, actor, role):
        """整体替换一条管段的连接关系；连接关系变化会推进拓扑版本。"""
        conn = self.connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            for valve_id in sequence:
                if conn.execute("SELECT 1 FROM topology_valves WHERE valve_id=?", (valve_id,)).fetchone() is None:
                    raise DomainError("unknown_valve", "连接关系引用了未登记的阀门: %s" % valve_id)
            before = [
                (row["valve_a"], row["valve_b"])
                for row in conn.execute(
                    "SELECT valve_a, valve_b FROM topology_links WHERE segment_id=? ORDER BY position",
                    (segment_id,),
                ).fetchall()
            ]
            conn.execute(
                "INSERT INTO topology_segments(segment_id,created_at,updated_at) VALUES(?,?,?) "
                "ON CONFLICT(segment_id) DO UPDATE SET updated_at=excluded.updated_at",
                (segment_id, now_iso(), now_iso()),
            )
            conn.execute("DELETE FROM topology_links WHERE segment_id=?", (segment_id,))
            for position, (a, b) in enumerate(links):
                try:
                    conn.execute(
                        "INSERT INTO topology_links(segment_id,valve_a,valve_b,position) VALUES(?,?,?,?)",
                        (segment_id, a, b, position),
                    )
                except sqlite3.IntegrityError:
                    raise ConflictError("link_conflict", "阀门对 %s-%s 已属于另一条管段" % (a, b))
            changed = before != links
            revision = self._bump_revision(conn, "topology_revision") if changed else None
            self.append_audit(conn, None, "topology_updated", actor, role, {
                "segment_id": segment_id,
                "sequence": sequence,
                "changed": changed,
            })
            conn.execute("COMMIT")
            return {"segment_id": segment_id, "sequence": sequence, "changed": changed, "topology_revision": revision}
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        finally:
            conn.close()

    def load_topology(self):
        conn = self.connect()
        try:
            valves = {
                row["valve_id"]: {"kind": row["kind"]}
                for row in conn.execute("SELECT valve_id, kind FROM topology_valves").fetchall()
            }
            segments_rows = conn.execute("SELECT segment_id FROM topology_segments ORDER BY segment_id").fetchall()
            segments = {}
            links = []
            for seg_row in segments_rows:
                sid = seg_row["segment_id"]
                rows = conn.execute(
                    "SELECT valve_a, valve_b FROM topology_links WHERE segment_id=? ORDER BY position",
                    (sid,),
                ).fetchall()
                if not rows:
                    segments[sid] = []
                    continue
                sequence = [rows[0]["valve_a"]]
                for row in rows:
                    sequence.append(row["valve_b"])
                    links.append((row["valve_a"], row["valve_b"], sid))
                segments[sid] = sequence
            return valves, segments, links
        finally:
            conn.close()

    # ------------------------------------------------------------------
    # 阀门回执（幂等）
    # ------------------------------------------------------------------

    def record_receipt(self, receipt, actor, role):
        """登记回执。重复 idempotency_key 只记一次，返回已有记录。"""
        conn = self.connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            existing = conn.execute(
                "SELECT * FROM valve_receipts WHERE idempotency_key=?",
                (receipt["idempotency_key"],),
            ).fetchone()
            if existing is not None:
                conn.execute("COMMIT")
                value = dict(existing)
                value["payload"] = json.loads(value["payload"])
                value["duplicate"] = True
                value["position_revision"] = None
                return value
            conn.execute(
                """
                INSERT INTO valve_receipts(valve_id,result,expected_position,reported_position,
                                           idempotency_key,payload,created_at)
                VALUES(?,?,?,?,?,?,?)
                """,
                (
                    receipt["valve_id"],
                    receipt["result"],
                    receipt["expected_position"],
                    receipt["reported_position"],
                    receipt["idempotency_key"],
                    canonical_json(receipt),
                    now_iso(),
                ),
            )
            receipt_id = conn.execute("SELECT last_insert_rowid() AS id").fetchone()["id"]
            revision = self._bump_revision(conn, "position_revision")
            self.append_audit(conn, None, "valve_receipt", actor, role, {
                "receipt_id": receipt_id,
                "valve_id": receipt["valve_id"],
                "result": receipt["result"],
            })
            conn.execute("COMMIT")
            result = dict(receipt)
            result["id"] = receipt_id
            result["duplicate"] = False
            result["position_revision"] = revision
            return result
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        finally:
            conn.close()

    def latest_receipts(self):
        conn = self.connect()
        try:
            rows = conn.execute(
                "SELECT r.* FROM valve_receipts r JOIN ("
                "  SELECT valve_id, MAX(id) AS max_id FROM valve_receipts GROUP BY valve_id"
                ") latest ON latest.max_id = r.id"
            ).fetchall()
            return {
                row["valve_id"]: {"result": row["result"], "reported_position": row["reported_position"], "id": row["id"]}
                for row in rows
            }
        finally:
            conn.close()

    def list_receipts(self, valve_id=None):
        conn = self.connect()
        try:
            if valve_id:
                rows = conn.execute("SELECT * FROM valve_receipts WHERE valve_id=? ORDER BY id", (valve_id,)).fetchall()
            else:
                rows = conn.execute("SELECT * FROM valve_receipts ORDER BY id").fetchall()
            result = []
            for row in rows:
                value = dict(row)
                value["payload"] = json.loads(value["payload"])
                result.append(value)
            return result
        finally:
            conn.close()

    # ------------------------------------------------------------------
    # 隔离作业
    # ------------------------------------------------------------------

    JOB_ACTIVE_STATES = ("pending_close", "paused", "isolated")

    def _row_to_job(self, row):
        if row is None:
            return None
        result = dict(row)
        result["payload"] = json.loads(result["payload"])
        return result

    def create_job(self, segment_id, item_id, status, payload, actor, role, topology_revision, position_revision):
        conn = self.connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            active = conn.execute(
                "SELECT * FROM isolation_jobs WHERE segment_id=? AND status IN ('pending_close','paused','isolated')",
                (segment_id,),
            ).fetchone()
            if active is not None:
                current = self._row_to_job(active)
                raise ConflictError("active_job_exists", "管段 %s 已有进行中的隔离作业" % segment_id, {
                    "active_job_id": current["id"],
                    "active_owner": current["created_by"],
                    "active_status": current["status"],
                    "active_boundary": current["payload"].get("boundary"),
                    "submitted_boundary": payload.get("boundary"),
                })
            if item_id is not None:
                if conn.execute("SELECT 1 FROM items WHERE id=?", (item_id,)).fetchone() is None:
                    raise NotFoundError("item_not_found", "关联的泄漏事件不存在")
            conn.execute(
                """
                INSERT INTO isolation_jobs(segment_id,item_id,status,version,payload,
                    topology_revision,position_revision,created_by,created_role,created_at,updated_at)
                VALUES(?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    segment_id, item_id, status, 1, canonical_json(payload),
                    topology_revision, position_revision, actor, role, now_iso(), now_iso(),
                ),
            )
            job_id = conn.execute("SELECT last_insert_rowid() AS id").fetchone()["id"]
            self.append_audit(conn, item_id, "isolation_job_created", actor, role, {
                "job_id": job_id, "segment_id": segment_id, "boundary": payload.get("boundary"),
            })
            conn.execute("COMMIT")
            return self.get_job(job_id)
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        finally:
            conn.close()

    def get_job(self, job_id):
        conn = self.connect()
        try:
            row = conn.execute("SELECT * FROM isolation_jobs WHERE id=?", (job_id,)).fetchone()
            if row is None:
                raise NotFoundError("job_not_found", "隔离作业不存在")
            return self._row_to_job(row)
        finally:
            conn.close()

    def get_active_job(self, segment_id):
        conn = self.connect()
        try:
            row = conn.execute(
                "SELECT * FROM isolation_jobs WHERE segment_id=? AND status IN ('pending_close','paused','isolated') "
                "ORDER BY id DESC LIMIT 1",
                (segment_id,),
            ).fetchone()
            return self._row_to_job(row)
        finally:
            conn.close()

    def list_jobs(self, segment_id=None, active_only=False):
        conn = self.connect()
        try:
            sql = "SELECT * FROM isolation_jobs"
            conditions = []
            params = []
            if segment_id:
                conditions.append("segment_id=?")
                params.append(segment_id)
            if active_only:
                conditions.append("status IN ('pending_close','paused','isolated')")
            if conditions:
                sql += " WHERE " + " AND ".join(conditions)
            sql += " ORDER BY id DESC"
            return [self._row_to_job(row) for row in conn.execute(sql, params).fetchall()]
        finally:
            conn.close()

    def update_job(self, job_id, status, payload, expected_version=None,
                   topology_revision=None, position_revision=None,
                   event_type="isolation_job_updated", event_payload=None, actor=None, role=None):
        """带乐观版本的作业更新，返回更新后的作业。"""
        conn = self.connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute("SELECT * FROM isolation_jobs WHERE id=?", (job_id,)).fetchone()
            if row is None:
                raise NotFoundError("job_not_found", "隔离作业不存在")
            if expected_version is not None and int(expected_version) != int(row["version"]):
                raise ConflictError("version_conflict", "作业已被其他操作更新，请重新读取")
            version = int(row["version"]) + 1
            topo_rev = int(row["topology_revision"]) if topology_revision is None else topology_revision
            pos_rev = int(row["position_revision"]) if position_revision is None else position_revision
            conn.execute(
                "UPDATE isolation_jobs SET status=?,version=?,payload=?,topology_revision=?,position_revision=?,updated_at=? WHERE id=?",
                (status, version, canonical_json(payload), topo_rev, pos_rev, now_iso(), job_id),
            )
            event_payload = event_payload if event_payload is not None else {"job_id": job_id, "status": status}
            self.append_audit(conn, row["item_id"], event_type, actor, row["created_role"] if role is None else role, event_payload)
            conn.execute("COMMIT")
            return self.get_job(job_id)
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        finally:
            conn.close()
