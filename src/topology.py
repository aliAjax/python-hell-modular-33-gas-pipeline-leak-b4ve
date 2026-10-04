"""管网拓扑与隔离边界计算。

图模型：阀门是节点，管段（segment）由一条有序的阀门链路描述，
链路中相邻阀门互为邻接。隔离边界由两侧（上游/下游）的有序阀门组成，
关阀失败时沿失败阀门向目标管段之外的邻接阀门逐级扩展，已关闭的阀
门始终保留。
"""

from .domain import DomainError

UPSTREAM = "upstream"
DOWNSTREAM = "downstream"
SIDES = (UPSTREAM, DOWNSTREAM)


class Topology:
    def __init__(self, valves, segments, links):
        # valve_id -> {"kind": str}
        self.valves = dict(valves)
        # segment_id -> [valve_id, ...] 保持提交顺序
        self.segments = {sid: list(seq) for sid, seq in segments.items()}
        # (valve_a, valve_b) -> segment_id，无向
        self.links = {}
        for a, b, sid in links:
            if a not in self.valves or b not in self.valves:
                raise DomainError("unknown_valve", "连接关系引用了未登记的阀门: %s/%s" % (a, b))
            key = (min(a, b), max(a, b))
            self.links[key] = sid

    def empty(self):
        return not self.segments

    def segment(self, segment_id):
        sequence = self.segments.get(segment_id)
        if not sequence:
            raise DomainError("segment_not_found", "管段 %s 不存在或没有连接关系" % segment_id, 404)
        return sequence

    def neighbors(self, valve_id):
        """返回 [(neighbor_id, via_segment_id), ...]，按阀门编号排序保证确定性。"""
        result = []
        for (a, b), sid in self.links.items():
            if a == valve_id:
                result.append((b, sid))
            elif b == valve_id:
                result.append((a, sid))
        result.sort(key=lambda item: item[0])
        return result

    def link_segment(self, a, b):
        return self.links.get((min(a, b), max(a, b)))

    def hops_to_segment(self, valve_id, segment_id, cap=3):
        """阀门到管段任一端点的最短距离（跳数），用于边界提案合理性校验。"""
        if valve_id not in self.valves:
            return None
        endpoints = set()
        sequence = self.segments.get(segment_id, [])
        if sequence:
            endpoints.add(sequence[0])
            endpoints.add(sequence[-1])
        if valve_id in endpoints:
            return 0
        frontier = [valve_id]
        seen = {valve_id}
        distance = 0
        while frontier and distance < cap:
            distance += 1
            nxt = []
            for current in frontier:
                for neighbor, _sid in self.neighbors(current):
                    if neighbor in endpoints:
                        return distance
                    if neighbor not in seen:
                        seen.add(neighbor)
                        nxt.append(neighbor)
            frontier = nxt
        return None

    def boundary_valves(self, boundary):
        """boundary: {side: [valve,...]}，返回有序去重后的阀门列表。"""
        ordered = []
        for side in SIDES:
            for valve in boundary.get(side, []):
                if valve not in ordered:
                    ordered.append(valve)
        return ordered


def normalize_boundary(payload):
    """从请求中解析并校验边界提案。"""
    raw = payload.get("boundary")
    if not isinstance(raw, dict):
        raise DomainError("boundary_required", "需要提交 boundary（upstream/downstream 阀门列表）")
    boundary = {}
    seen = set()
    for side in SIDES:
        valves = raw.get(side, [])
        if not isinstance(valves, list) or not valves or not all(isinstance(v, str) and v.strip() for v in valves):
            raise DomainError("boundary_side_required", "边界 %s 必须是非空阀门列表" % side)
        cleaned = []
        for valve in valves:
            valve = valve.strip()
            if valve in seen:
                raise DomainError("boundary_valve_duplicated", "同一阀门不能出现在边界两侧: %s" % valve)
            seen.add(valve)
            cleaned.append(valve)
        boundary[side] = cleaned
    return boundary


def validate_proposal(topo, segment_id, boundary):
    """提案中的阀门必须已登记，且在目标管段附近（默认 3 跳内）。"""
    sequence = topo.segment(segment_id)
    if sequence[0] == sequence[-1] or len(sequence) < 2:
        raise DomainError("invalid_segment", "管段 %s 的连接关系不完整" % segment_id)
    for valve in topo.boundary_valves(boundary):
        if valve not in topo.valves:
            raise DomainError("unknown_valve", "边界阀门未登记: %s" % valve)
        hops = topo.hops_to_segment(valve, segment_id)
        if hops is None:
            raise DomainError("valve_not_adjacent", "阀门 %s 与目标管段不相邻，不能组成边界" % valve)


def expand_failed(topo, target_segment, failed_valve, used_valves, exclude_known=None):
    """失败阀门的替代选择：优先沿非目标管段向外扩展的相邻阀门。

    返回 [(valve_id, via_segment_id), ...]，按确定性顺序排列。
    管段自身方向上的邻居不能作为替代（那只会把故障点包进隔离区）。
    """
    exclude_known = exclude_known or set()
    candidates = []
    for neighbor, via_segment in topo.neighbors(failed_valve):
        if via_segment == target_segment:
            continue
        if neighbor in used_valves or neighbor in exclude_known:
            continue
        candidates.append((neighbor, via_segment))
    return candidates


def boundary_diff(current, other):
    """两个边界提案的差异：现用独有 / 对方独有（按侧比较）。"""
    diff = {}
    for side in SIDES:
        current_set = set(current.get(side, []))
        other_set = set(other.get(side, []))
        diff[side] = {
            "active_only": sorted(current_set - other_set),
            "submitted_only": sorted(other_set - current_set),
        }
    return diff
