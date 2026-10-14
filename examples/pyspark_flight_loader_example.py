#!/usr/bin/env python3
"""
PySpark / Flight Loader example for GDB Graph Database.
Generates synthetic vertices and edges, then streams directly to GDB client-flight ports.
"""

import sys
import os
import argparse
import pyarrow as pa

# Add local package to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "python")))

from gdb_spark import GdbSparkLoader, GdbOptions, GdbClusterTopology


def main():
    parser = argparse.ArgumentParser(description="GDB Spark Arrow Flight Loader Example")
    parser.add_argument("--url", default="http://localhost:8847", help="GDB Coordinator URL (default: http://localhost:8847)")
    parser.add_argument("--vertices", type=int, default=10000, help="Number of vertices to generate")
    parser.add_argument("--edges", type=int, default=50000, help="Number of edges to generate")
    args = parser.parse_args()

    print(f"[*] Discovering GDB cluster topology from {args.url}...")
    topology = GdbClusterTopology.discover(args.url)
    print(f"[+] Cluster has {topology.total_nodes} nodes:")
    for n in topology.nodes:
        print(f"    - Node {n.node_id}: HTTP {n.http_url}, Client Flight: {n.client_flight_address}:{n.client_flight_port}")

    # Generate synthetic vertices in PyArrow
    print(f"\n[*] Generating {args.vertices} vertices (User)...")
    v_ids = list(range(1, args.vertices + 1))
    v_names = [f"User_{i}" for i in v_ids]
    v_ages = [20 + (i % 60) for i in v_ids]
    v_active = [i % 2 == 0 for i in v_ids]

    v_table = pa.Table.from_arrays(
        [
            pa.array(v_ids, type=pa.int64()),
            pa.array(v_names, type=pa.string()),
            pa.array(v_ages, type=pa.int64()),
            pa.array(v_active, type=pa.bool_()),
        ],
        names=["id", "name", "age", "active"],
    )

    print("[*] Ingesting vertices into GDB via Arrow Flight...")
    v_summary = GdbSparkLoader.load_vertices(
        v_table,
        options=GdbOptions(
            url=args.url,
            label="User",
            id_col="id",
            batch_size=16384,
            compact=True,
        ),
    )
    print(f"[✓] Vertices: {v_summary.total_rows} rows in {v_summary.elapsed_seconds}s ({v_summary.rows_per_second} rows/s)")

    # Generate synthetic edges in PyArrow
    print(f"\n[*] Generating {args.edges} edges (FOLLOWS)...")
    e_src = []
    e_dst = []
    e_rank = []
    for i in range(1, args.edges + 1):
        s = (i % args.vertices) + 1
        d = ((i * 7) % args.vertices) + 1
        e_src.append(s)
        e_dst.append(d)
        e_rank.append(0)

    e_table = pa.Table.from_arrays(
        [
            pa.array(e_src, type=pa.int64()),
            pa.array(e_dst, type=pa.int64()),
            pa.array(e_rank, type=pa.int64()),
        ],
        names=["src", "dst", "rank"],
    )

    print("[*] Ingesting edges into GDB via Arrow Flight...")
    e_summary = GdbSparkLoader.load_edges(
        e_table,
        options=GdbOptions(
            url=args.url,
            edge_type="FOLLOWS",
            src_col="src",
            dst_col="dst",
            rank_col="rank",
            batch_size=16384,
            compact=True,
        ),
    )
    print(f"[✓] Edges: {e_summary.total_rows} rows in {e_summary.elapsed_seconds}s ({e_summary.rows_per_second} rows/s)")


if __name__ == "__main__":
    main()
