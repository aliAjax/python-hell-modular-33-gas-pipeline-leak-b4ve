import json
import mimetypes
import os
from http.server import BaseHTTPRequestHandler
from urllib.parse import urlparse

from .domain import DomainError


def build_handler(service, static_dir):
    class Handler(BaseHTTPRequestHandler):
        server_version = "ModularPythonHell/1.0"

        def log_message(self, fmt, *args):
            return

        def _identity(self):
            actor = self.headers.get("X-User-Id", "").strip()
            role = self.headers.get("X-Role", "").strip()
            region = self.headers.get("X-Region", "").strip() or None
            return actor, role, region

        def _json_body(self):
            length = int(self.headers.get("Content-Length", "0") or "0")
            if not length:
                return {}
            try:
                return json.loads(self.rfile.read(length).decode("utf-8"))
            except (ValueError, UnicodeDecodeError):
                raise DomainError("invalid_json", "请求体不是有效 JSON", 400)

        def _send(self, status, value, content_type="application/json; charset=utf-8"):
            if not isinstance(value, (bytes, bytearray)):
                value = json.dumps(value, ensure_ascii=False).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(value)))
            self.end_headers()
            self.wfile.write(value)

        def _error(self, exc):
            status = getattr(exc, "status", 500)
            code = getattr(exc, "code", "internal_error")
            body = {"error": code, "message": str(exc)}
            details = getattr(exc, "details", None)
            if details:
                body["details"] = details
            self._send(status, body)

        def do_GET(self):
            try:
                path = urlparse(self.path).path
                if path == "/health":
                    return self._send(200, {"status": "ok"})
                if path == "/api/state":
                    return self._send(200, service.state())
                if path == "/api/items":
                    return self._send(200, {"items": service.list_items()})
                if path == "/api/topology":
                    return self._send(200, service.topology())
                if path == "/api/receipts":
                    return self._send(200, {"receipts": service.repository.list_receipts()})
                if path == "/api/isolation/jobs":
                    query = urlparse(self.path).query
                    params = dict(pair.split("=", 1) for pair in query.split("&") if "=" in pair)
                    jobs = service.list_jobs(segment_id=params.get("segment"),
                                             active_only=params.get("active") == "1")
                    return self._send(200, {"jobs": jobs})
                parts = [part for part in path.split("/") if part]
                if len(parts) == 3 and parts[:2] == ["api", "items"]:
                    return self._send(200, service.get_item(int(parts[2])))
                if len(parts) == 4 and parts[:2] == ["api", "items"] and parts[3] == "audit":
                    item = service.get_item(int(parts[2]))
                    return self._send(200, {"events": item["audit"]})
                if len(parts) == 4 and parts[:2] == ["api", "segments"] and parts[3] == "jobs":
                    job = service.get_active_job(parts[2])
                    if job is None:
                        return self._send(404, {"error": "no_active_job", "message": "该管段没有进行中的隔离作业"})
                    return self._send(200, job)
                if len(parts) == 4 and parts[:3] == ["api", "isolation", "jobs"]:
                    return self._send(200, service.get_job(int(parts[3])))
                if path == "/":
                    file_path = os.path.join(static_dir, "index.html")
                    with open(file_path, "rb") as handle:
                        content = handle.read()
                    return self._send(200, content, "text/html; charset=utf-8")
                return self._send(404, {"error": "not_found", "message": "接口不存在"})
            except DomainError as exc:
                return self._error(exc)
            except (ValueError, OSError) as exc:
                return self._error(DomainError("invalid_request", str(exc), 400))

        def do_POST(self):
            actor = role = region = None
            try:
                actor, role, region = self._identity()
                path = urlparse(self.path).path
                payload = self._json_body()
                parts = [part for part in path.split("/") if part]
                if parts == ["api", "items"]:
                    return self._send(201, service.create_item(payload, actor, role, region))
                if len(parts) == 4 and parts[:2] == ["api", "items"] and parts[3] == "sources":
                    return self._send(201, service.add_source(int(parts[2]), payload, actor, role, region))
                if len(parts) == 4 and parts[:2] == ["api", "items"] and parts[3] == "actions":
                    action = payload.pop("action", "")
                    if not action:
                        raise DomainError("action_required", "缺少 action", 400)
                    expected = payload.pop("expected_version", None)
                    return self._send(200, service.act(int(parts[2]), action, payload, actor, role, expected, region))
                # 拓扑
                if parts == ["api", "topology", "valves"]:
                    return self._send(201, service.register_valve(payload, actor, role))
                if len(parts) == 4 and parts[:2] == ["api", "segments"] and parts[3] == "links":
                    return self._send(200, service.put_segment(parts[2], payload, actor, role))
                # 阀门回执（幂等）
                if parts == ["api", "receipts"]:
                    return self._send(201, service.record_receipt(payload, actor, role))
                # 隔离作业
                if len(parts) == 4 and parts[:2] == ["api", "segments"] and parts[3] == "isolation-jobs":
                    expected = payload.pop("expected_version", None)
                    return self._send(201, service.create_isolation_job(
                        parts[2], payload, actor, role, expected, region))
                if len(parts) == 5 and parts[:3] == ["api", "isolation", "jobs"]:
                    job_id = int(parts[3])
                    sub = parts[4]
                    expected = payload.pop("expected_version", None)
                    if sub == "close":
                        return self._send(200, service.close_valve(job_id, payload, actor, role, expected))
                    if sub == "resume":
                        return self._send(200, service.resume_job(job_id, payload, actor, role, expected))
                    if sub == "recompute":
                        return self._send(200, service.recompute_job(job_id, payload, actor, role, expected))
                    if sub == "abandon":
                        return self._send(200, service.abandon_job(job_id, payload, actor, role, expected))
                return self._send(404, {"error": "not_found", "message": "接口不存在"})
            except DomainError as exc:
                return self._error(exc)
            except Exception as exc:
                return self._error(DomainError("internal_error", str(exc), 500))

    return Handler
