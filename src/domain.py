from datetime import datetime


class DomainError(Exception):
    def __init__(self, code, message, status=400):
        super().__init__(message)
        self.code = code
        self.status = status


class ConflictError(DomainError):
    def __init__(self, code, message, details=None):
        super().__init__(code, message, 409)
        self.details = details or {}


class NotFoundError(DomainError):
    def __init__(self, code, message):
        super().__init__(code, message, 404)


def require_text(payload, name):
    value = payload.get(name)
    if not isinstance(value, str) or not value.strip():
        raise DomainError("field_required", "%s 不能为空" % name)
    return value.strip()


def number(payload, name, minimum=None):
    value = payload.get(name)
    if isinstance(value, bool):
        raise DomainError("invalid_number", "%s 必须是数字" % name)
    try:
        value = float(value)
    except (TypeError, ValueError):
        raise DomainError("invalid_number", "%s 必须是数字" % name)
    if minimum is not None and value < minimum:
        raise DomainError("invalid_number", "%s 不能小于 %s" % (name, minimum))
    return value


def parse_timestamp(payload, name):
    value = require_text(payload, name)
    try:
        datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        raise DomainError("invalid_timestamp", "%s 必须是 ISO 时间" % name)
    return value


def normalize_create(payload):
    pipeline_id = require_text(payload, "pipeline_id")
    segment_id = require_text(payload, "segment_id")
    reported_at = parse_timestamp(payload, "reported_at")
    pressure_drop = number(payload, "pressure_drop_kpa", 0)
    sensor_ppm = number(payload, "sensor_value_ppm", 0)
    odor_reports = int(payload.get("odor_reports", 0) or 0)
    if odor_reports < 0:
        raise DomainError("invalid_odor_reports", "异味报告数不能为负数")
    reporter = require_text(payload, "reporter")
    stable_key = "%s|%s|%s" % (pipeline_id, segment_id, reported_at)
    return {
        "pipeline_id": pipeline_id,
        "segment_id": segment_id,
        "reported_at": reported_at,
        "pressure_drop_kpa": pressure_drop,
        "sensor_value_ppm": sensor_ppm,
        "odor_reports": odor_reports,
        "reporter": reporter,
        "source_comparison": [],
        "valve_sequence": [],
        "hazards_clear": False,
        "_stable_key": stable_key,
    }


def normalize_source(payload):
    source_type = require_text(payload, "source_type")
    external_id = require_text(payload, "external_id")
    observed_at = parse_timestamp(payload, "observed_at")
    result = {
        "source_type": source_type,
        "external_id": external_id,
        "observed_at": observed_at,
        "sensor_value_ppm": number(payload, "sensor_value_ppm", 0) if "sensor_value_ppm" in payload else None,
        "pressure_drop_kpa": number(payload, "pressure_drop_kpa", 0) if "pressure_drop_kpa" in payload else None,
        "odor_reports": int(payload.get("odor_reports", 0) or 0),
        "note": payload.get("note", ""),
    }
    return result


def normalize_valve(payload):
    valve_id = require_text(payload, "valve_id")
    kind = str(payload.get("kind", "gate")).strip() or "gate"
    return {"valve_id": valve_id, "kind": kind}


def normalize_segment_links(payload, segment_id):
    """校验管段连接：有序阀门链路，至少两个阀门，相邻阀门不重复。"""
    sequence = payload.get("valve_sequence")
    if not isinstance(sequence, list) or len(sequence) < 2:
        raise DomainError("valve_sequence_required", "管段连接至少需要两个阀门及顺序")
    cleaned = []
    for value in sequence:
        if not isinstance(value, str) or not value.strip():
            raise DomainError("invalid_valve_sequence", "阀门顺序格式无效")
        value = value.strip()
        if cleaned and value == cleaned[-1]:
            raise DomainError("invalid_valve_sequence", "相邻阀门不能重复: %s" % value)
        cleaned.append(value)
    links = []
    for a, b in zip(cleaned, cleaned[1:]):
        if a == b:
            raise DomainError("invalid_valve_sequence", "连接关系不能自环")
        links.append((a, b))
    return {"segment_id": segment_id, "sequence": cleaned, "links": links}


VALVE_POSITIONS = {"open", "closed"}
RECEIPT_RESULTS = {"closed", "stuck", "mismatch"}


def normalize_receipt(payload):
    valve_id = require_text(payload, "valve_id")
    result = require_text(payload, "result")
    if result not in RECEIPT_RESULTS:
        raise DomainError("invalid_receipt_result", "回执结果必须是 closed/stuck/mismatch")
    expected = payload.get("expected_position")
    if expected is not None and expected not in VALVE_POSITIONS:
        raise DomainError("invalid_valve_position", "期望阀位必须是 open/closed")
    reported_position = payload.get("reported_position")
    if reported_position is not None and reported_position not in VALVE_POSITIONS:
        raise DomainError("invalid_valve_position", "现场阀位必须是 open/closed")
    idempotency_key = require_text(payload, "idempotency_key")
    return {
        "valve_id": valve_id,
        "result": result,
        "expected_position": expected,
        "reported_position": reported_position,
        "idempotency_key": idempotency_key,
        "note": str(payload.get("note", "")),
    }
