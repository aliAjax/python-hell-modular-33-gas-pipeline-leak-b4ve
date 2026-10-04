"""燃气管网拓扑：管段通过阀门相连的无向图。

阀门有两个端口，每个端口连接一条管段（segment）或为空（None，代表管网尽头）。
隔离某条管段，就是关掉所有“跨过隔离区边界”的阀门：一端在区内、另一端在区外或为空。
"""


class Graph:
    def __init__(self, connections):
        # connections: iterable of (valve_id, segment_a, segment_b)
        self.valves = {}  # valve_id -> (segment_a, segment_b)
        self.segments = set()
        for valve_id, segment_a, segment_b in connections:
            self.valves[valve_id] = (segment_a, segment_b)
            if segment_a:
                self.segments.add(segment_a)
            if segment_b:
                self.segments.add(segment_b)

    def has_segment(self, segment_id):
        return segment_id in self.segments

    def endpoints(self, valve_id):
        return self.valves.get(valve_id, (None, None))

    def segments_of_valve(self, valve_id):
        return {ep for ep in self.valves.get(valve_id, (None, None)) if ep is not None}

    def boundary(self, region):
        """跨过隔离区边界的阀门：一端在区内，另一端在区外或为空。"""
        region_set = set(region)
        result = []
        for valve_id, (segment_a, segment_b) in self.valves.items():
            endpoints = [segment_a, segment_b]
            in_region = any(ep is not None and ep in region_set for ep in endpoints)
            outside = any(ep is None or ep not in region_set for ep in endpoints)
            if in_region and outside:
                result.append(valve_id)
        return result

    def other_side(self, valve_id, region):
        """阀门在区外那一侧的管段（用于关阀失败后扩大隔离区）；尽头阀门返回 None。"""
        region_set = set(region)
        for ep in self.valves.get(valve_id, (None, None)):
            if ep is not None and ep not in region_set:
                return ep
        return None

    def is_terminal(self, valve_id):
        segment_a, segment_b = self.valves.get(valve_id, (None, None))
        return segment_a is None or segment_b is None
