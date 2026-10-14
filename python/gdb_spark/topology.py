from dataclasses import dataclass
from typing import List, Optional
import json
import urllib.request
import urllib.parse
import urllib.error
import logging

logger = logging.getLogger("gdb_spark.topology")


@dataclass
class GdbNodeInfo:
    node_id: int
    http_url: str
    flight_address: str
    flight_port: int
    client_flight_address: str
    client_flight_port: int
    host: str

    @property
    def flight_endpoint_uri(self) -> str:
        return f"grpc://{self.host}:{self.client_flight_port}"

    @classmethod
    def from_dict(cls, data: dict, seed_host: Optional[str] = None) -> "GdbNodeInfo":
        http_url = data.get("http_url", "http://127.0.0.1:8847")
        flight_address = data.get("flight_address") or ""
        client_flight_address = data.get("client_flight_address") or ""
        flight_port = int(data.get("flight_port", 8848))
        client_flight_port = int(data.get("client_flight_port", 8860))
        node_id = int(data.get("node_id", 1))

        host = client_flight_address
        is_loopback = host in ("127.0.0.1", "localhost", "0.0.0.0", "::1", "")

        if is_loopback and seed_host and seed_host not in ("127.0.0.1", "localhost", "0.0.0.0", "::1"):
            host = seed_host
        elif not host:
            if "//" in http_url:
                host_part = http_url.split("//")[1].split(":")[0]
                if host_part:
                    host = host_part
            if not host:
                host = seed_host or "127.0.0.1"

        return cls(
            node_id=node_id,
            http_url=http_url,
            flight_port=flight_port,
            client_flight_port=client_flight_port,
            flight_address=flight_address or host,
            client_flight_address=client_flight_address or host,
            host=host,
        )


@dataclass
class GdbClusterTopology:
    seed_url: str
    nodes: List[GdbNodeInfo]
    replication_factor: int = 1
    partitions: int = 4

    @property
    def total_nodes(self) -> int:
        return len(self.nodes)

    def target_node_for_key(self, key: int) -> GdbNodeInfo:
        if not self.nodes:
            raise RuntimeError("No nodes available in cluster topology")
        pos_key = abs(int(key))
        idx = pos_key % len(self.nodes)
        return self.nodes[idx]

    def trigger_compaction(self, timeout: float = 10.0) -> None:
        """Triggers CSR compaction across all GDB nodes after bulk load completion."""
        for node in self.nodes:
            query_url = f"{node.http_url.rstrip('/')}/query"
            try:
                payload = json.dumps({"query": "COMPACT;"}).encode("utf-8")
                req = urllib.request.Request(
                    query_url,
                    data=payload,
                    headers={"Content-Type": "application/json", "User-Agent": "gdb-spark-connector/0.5.6"},
                    method="POST",
                )
                with urllib.request.urlopen(req, timeout=timeout) as resp:
                    if resp.status < 300:
                        logger.info("Compaction triggered on GDB node %s (%s)", node.node_id, node.http_url)
            except Exception as e:
                logger.warning("Compaction call to node %s (%s) failed: %s", node.node_id, node.http_url, e)

    @classmethod
    def discover(cls, seed_url: str = "http://localhost:8847", timeout: float = 10.0) -> "GdbClusterTopology":
        clean_seed = seed_url.rstrip("/")
        seed_host = None
        try:
            parsed = urllib.parse.urlparse(clean_seed)
            if parsed.hostname:
                seed_host = parsed.hostname
        except Exception:
            seed_host = "127.0.0.1"

        cluster_url = f"{clean_seed}/cluster"
        logger.info("Discovering GDB cluster topology from %s...", cluster_url)

        try:
            req = urllib.request.Request(
                cluster_url,
                headers={"User-Agent": "gdb-spark-connector/0.5.6"},
                method="GET",
            )
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                data = json.loads(resp.read().decode("utf-8"))

            raw_nodes = data.get("ring_nodes", [])
            if raw_nodes:
                nodes = [GdbNodeInfo.from_dict(n, seed_host=seed_host) for n in raw_nodes]
            else:
                fallback_dict = {
                    "node_id": int(data.get("node_id", 1)),
                    "http_url": clean_seed,
                    "flight_address": data.get("flight_address", ""),
                    "flight_port": int(data.get("flight_port", 8848)),
                    "client_flight_address": data.get("client_flight_address", ""),
                    "client_flight_port": int(data.get("client_flight_port", 8860)),
                }
                nodes = [GdbNodeInfo.from_dict(fallback_dict, seed_host=seed_host)]

            nodes.sort(key=lambda n: n.node_id)
            rf = int(data.get("replication_factor", 1))
            parts = int(data.get("partitions", 4))

            logger.info("Discovered %d GDB ring nodes. Ports: %s", len(nodes), [n.client_flight_port for n in nodes])
            return cls(seed_url=clean_seed, nodes=nodes, replication_factor=rf, partitions=parts)

        except Exception as ex:
            logger.warning("Could not reach GDB cluster at %s (%s). Using single-node fallback.", cluster_url, ex)
            host = seed_host or "127.0.0.1"
            fallback_node = GdbNodeInfo(
                node_id=1,
                http_url=clean_seed,
                flight_address=host,
                flight_port=8848,
                client_flight_address=host,
                client_flight_port=8860,
                host=host,
            )
            return cls(seed_url=clean_seed, nodes=[fallback_node], replication_factor=1, partitions=1)
