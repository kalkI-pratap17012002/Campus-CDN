from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class ChunkTask:
    chunk_index: int
    source_peer_id: str
    priority: float


class ChunkScheduler:
    def __init__(self, manifest: dict[str, Any], active_peers: list[Any], connection_pool: Any) -> None:
        self.manifest = manifest
        self.active_peers = [peer for peer in active_peers if self._is_active(peer)]
        self.connection_pool = connection_pool
        self._schedule: list[ChunkTask] = []
        self._chunk_sizes = {
            int(chunk["index"]): int(chunk.get("size", 0))
            for chunk in self.manifest.get("chunks", [])
        }
        self._peers_by_id = {
            self._peer_value(peer, "peer_id"): peer
            for peer in self.active_peers
        }

    def schedule(self, file_id: str) -> list[ChunkTask]:
        chunks = list(self.manifest.get("chunks", []))
        bandwidth_scores = self._normalized_bandwidths(self.active_peers)
        peer_loads: dict[str, int] = {peer_id: 0 for peer_id in self._peers_by_id}
        candidate_plans: list[dict[str, Any]] = []
        total_chunks = max(len(chunks), 1)

        for chunk in chunks:
            chunk_hash = str(chunk["hash"])
            chunk_index = int(chunk["index"])
            candidate_peers = [
                peer for peer in self.active_peers if chunk_hash in self._peer_value(peer, "available_chunks", [])
            ]
            rarity = len(candidate_peers)
            rarity_score = 1.0 if rarity == 0 else 1.0 / rarity
            peer_scores = {
                self._peer_value(peer, "peer_id"): (rarity_score * 0.6) + (bandwidth_scores[self._peer_value(peer, "peer_id")] * 0.4)
                for peer in candidate_peers
            }
            origin_score = (rarity_score * 0.6) if rarity == 0 else 0.0
            candidate_plans.append(
                {
                    "chunk_index": chunk_index,
                    "candidate_peers": candidate_peers,
                    "peer_scores": peer_scores,
                    "priority": max(peer_scores.values(), default=origin_score),
                }
            )

        candidate_plans.sort(key=lambda plan: plan["priority"], reverse=True)
        scheduled: list[ChunkTask] = []

        for plan in candidate_plans:
            if not plan["candidate_peers"]:
                scheduled.append(
                    ChunkTask(
                        chunk_index=plan["chunk_index"],
                        source_peer_id="origin",
                        priority=round(float(plan["priority"]), 4),
                    )
                )
                continue

            def assignment_sort_key(peer: Any) -> tuple[float, float]:
                peer_id = self._peer_value(peer, "peer_id")
                base_score = float(plan["peer_scores"][peer_id])
                load_penalty = peer_loads[peer_id] / total_chunks
                adjusted_score = base_score - (load_penalty * 0.2)
                return (adjusted_score, -peer_loads[peer_id])

            selected_peer = max(plan["candidate_peers"], key=assignment_sort_key)
            selected_peer_id = self._peer_value(selected_peer, "peer_id")
            peer_loads[selected_peer_id] += 1
            scheduled.append(
                ChunkTask(
                    chunk_index=plan["chunk_index"],
                    source_peer_id=selected_peer_id,
                    priority=round(float(plan["peer_scores"][selected_peer_id]), 4),
                )
            )

        self._schedule = scheduled
        return list(self._schedule)

    def get_schedule_stats(self) -> dict[str, float | int]:
        total_chunks = len(self._schedule)
        from_origin = sum(1 for task in self._schedule if task.source_peer_id == "origin")
        from_peers = total_chunks - from_origin
        estimated_time_seconds = round(self._estimate_total_time_seconds(), 2)
        return {
            "total_chunks": total_chunks,
            "from_peers": from_peers,
            "from_origin": from_origin,
            "estimated_time_seconds": estimated_time_seconds,
        }

    def _estimate_total_time_seconds(self) -> float:
        if not self._schedule:
            return 0.0

        peer_workloads_bits: dict[str, int] = {}
        origin_bits = 0
        active_bandwidths: list[float] = []

        for peer in self.active_peers:
            bandwidth = max(float(self._peer_value(peer, "bandwidth_mbps", 1.0)), 1.0)
            active_bandwidths.append(bandwidth)

        for task in self._schedule:
            chunk_bits = self._chunk_sizes.get(task.chunk_index, 0) * 8
            if task.source_peer_id == "origin":
                origin_bits += chunk_bits
                continue
            peer_workloads_bits[task.source_peer_id] = peer_workloads_bits.get(task.source_peer_id, 0) + chunk_bits

        source_times: list[float] = []
        for peer_id, bits in peer_workloads_bits.items():
            peer = self._peers_by_id[peer_id]
            bandwidth_mbps = max(float(self._peer_value(peer, "bandwidth_mbps", 1.0)), 1.0)
            source_times.append(bits / (bandwidth_mbps * 1_000_000))

        if origin_bits > 0:
            assumed_origin_bandwidth = max(sum(active_bandwidths) / len(active_bandwidths), 100.0) if active_bandwidths else 100.0
            origin_parallelism = max(1, int(getattr(self.connection_pool, "max_connections", 1)))
            source_times.append((origin_bits / (assumed_origin_bandwidth * 1_000_000)) / origin_parallelism)

        return max(source_times, default=0.0)

    @staticmethod
    def _normalized_bandwidths(peers: list[Any]) -> dict[str, float]:
        if not peers:
            return {}

        values = [max(float(ChunkScheduler._peer_value(peer, "bandwidth_mbps", 0.0)), 0.0) for peer in peers]
        min_value = min(values)
        max_value = max(values)
        normalized: dict[str, float] = {}

        for peer, value in zip(peers, values, strict=False):
            peer_id = ChunkScheduler._peer_value(peer, "peer_id")
            if max_value == min_value:
                normalized[peer_id] = 1.0 if max_value > 0 else 0.0
            else:
                normalized[peer_id] = (value - min_value) / (max_value - min_value)
        return normalized

    @staticmethod
    def _peer_value(peer: Any, name: str, default: Any = None) -> Any:
        if isinstance(peer, dict):
            return peer.get(name, default)
        return getattr(peer, name, default)

    @staticmethod
    def _is_active(peer: Any) -> bool:
        if isinstance(peer, dict):
            return bool(peer.get("is_active", True))
        return bool(getattr(peer, "is_active", True))
