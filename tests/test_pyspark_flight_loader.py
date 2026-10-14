#!/usr/bin/env python3
"""
Integration test for GDB Spark Arrow Flight Loader.
Starts an in-memory mock GDB cluster (HTTP coordinator + Flight servers) to verify:
1. Driver discovery of /cluster topology
2. Ring token partitioning and routing to target client_flight_port
3. Streaming do_put Arrow RecordBatches
4. Post-ingestion compaction trigger
"""

import http.server
import json
import os
import socketserver
import sys
import threading
import time
import unittest
import pyarrow as pa
import pyarrow.flight as flight

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "python")))

from gdb_spark import GdbSparkLoader, GdbOptions, GdbClusterTopology, GdbNodeInfo


class MockFlightServer(flight.FlightServerBase):
    def __init__(self, location):
        super().__init__(location)
        self.received_tables = []
        self.received_descriptors = []

    def do_put(self, context, descriptor, reader, writer):
        self.received_descriptors.append(descriptor.command.decode("utf-8") if descriptor.command else "")
        table = reader.read_all()
        self.received_tables.append(table)


class MockGdbClusterHandler(http.server.BaseHTTPRequestHandler):
    cluster_nodes = []
    compaction_called = False

    def do_GET(self):
        if self.path == "/cluster":
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            payload = {
                "node_id": 1,
                "role": "Peer",
                "cluster_topology": "leaderless-ring",
                "replication_factor": 1,
                "partitions": 4,
                "ring_nodes": self.cluster_nodes,
            }
            self.wfile.write(json.dumps(payload).encode("utf-8"))
        else:
            self.send_response(404)
            self.end_headers()

    def do_POST(self):
        if self.path == "/query":
            length = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(length).decode("utf-8")
            if "COMPACT" in body:
                MockGdbClusterHandler.compaction_called = True
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(b'{"status": "ok"}')
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, format, *args):
        pass  # Suppress logging in test


class TestGdbSparkFlightLoader(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Start 2 Mock Flight Servers simulating 2 GDB cluster nodes
        cls.flight1 = MockFlightServer("grpc://127.0.0.1:0")
        cls.flight_port1 = cls.flight1.port
        cls.flight_thread1 = threading.Thread(target=cls.flight1.serve, daemon=True)
        cls.flight_thread1.start()

        cls.flight2 = MockFlightServer("grpc://127.0.0.1:0")
        cls.flight_port2 = cls.flight2.port
        cls.flight_thread2 = threading.Thread(target=cls.flight2.serve, daemon=True)
        cls.flight_thread2.start()

        # Setup mock cluster nodes topology
        MockGdbClusterHandler.cluster_nodes = [
            {
                "node_id": 1,
                "http_url": "http://127.0.0.1:0",
                "flight_address": "127.0.0.1",
                "flight_port": 8848,
                "client_flight_address": "127.0.0.1",
                "client_flight_port": cls.flight_port1,
            },
            {
                "node_id": 2,
                "http_url": "http://127.0.0.1:0",
                "flight_address": "127.0.0.1",
                "flight_port": 8849,
                "client_flight_address": "127.0.0.1",
                "client_flight_port": cls.flight_port2,
            },
        ]

        # Start Mock HTTP coordinator
        cls.httpd = socketserver.TCPServer(("127.0.0.1", 0), MockGdbClusterHandler)
        cls.http_port = cls.httpd.server_address[1]
        MockGdbClusterHandler.cluster_nodes[0]["http_url"] = f"http://127.0.0.1:{cls.http_port}"
        MockGdbClusterHandler.cluster_nodes[1]["http_url"] = f"http://127.0.0.1:{cls.http_port}"
        cls.http_thread = threading.Thread(target=cls.httpd.serve_forever, daemon=True)
        cls.http_thread.start()

        time.sleep(0.1)

    @classmethod
    def tearDownClass(cls):
        cls.httpd.shutdown()
        cls.flight1.shutdown()
        cls.flight2.shutdown()

    def test_01_topology_discovery(self):
        url = f"http://127.0.0.1:{self.http_port}"
        topology = GdbClusterTopology.discover(url)

        self.assertEqual(topology.total_nodes, 2)
        self.assertEqual(topology.nodes[0].node_id, 1)
        self.assertEqual(topology.nodes[0].client_flight_port, self.flight_port1)
        self.assertEqual(topology.nodes[1].node_id, 2)
        self.assertEqual(topology.nodes[1].client_flight_port, self.flight_port2)

        # Primary token test: key % 2
        self.assertEqual(topology.target_node_for_key(0).node_id, 1)
        self.assertEqual(topology.target_node_for_key(1).node_id, 2)
        self.assertEqual(topology.target_node_for_key(2).node_id, 1)

    def test_02_vertex_ingestion_flight_streaming(self):
        url = f"http://127.0.0.1:{self.http_port}"
        # Create 100 vertices: 50 even IDs (-> Node 1), 50 odd IDs (-> Node 2)
        v_ids = list(range(1, 101))
        v_names = [f"User_{i}" for i in v_ids]
        v_ages = [20 + (i % 40) for i in v_ids]

        table = pa.Table.from_arrays(
            [pa.array(v_ids), pa.array(v_names), pa.array(v_ages)],
            names=["id", "name", "age"],
        )

        summary = GdbSparkLoader.load_vertices(
            table,
            options=GdbOptions(
                url=url,
                label="Person",
                id_col="id",
                compact=True,
            ),
        )

        self.assertEqual(summary.total_rows, 100)
        self.assertEqual(summary.cluster_nodes, 2)

        # Verify that both flight servers received tables
        self.assertTrue(len(self.flight1.received_tables) >= 1)
        self.assertTrue(len(self.flight2.received_tables) >= 1)

        # Verify descriptors contained vertex label
        desc1 = json.loads(self.flight1.received_descriptors[-1])
        self.assertEqual(desc1.get("type"), "vertex")
        self.assertEqual(desc1.get("label"), "Person")

        # Verify compaction was triggered
        self.assertTrue(MockGdbClusterHandler.compaction_called)

    def test_03_edge_ingestion_flight_streaming(self):
        url = f"http://127.0.0.1:{self.http_port}"
        e_src = list(range(1, 51))
        e_dst = list(range(51, 101))

        table = pa.Table.from_arrays(
            [pa.array(e_src), pa.array(e_dst)],
            names=["src", "dst"],
        )

        summary = GdbSparkLoader.load_edges(
            table,
            options=GdbOptions(
                url=url,
                edge_type="FRIENDS_WITH",
                src_col="src",
                dst_col="dst",
                compact=False,
            ),
        )

        self.assertEqual(summary.total_rows, 50)
        desc = json.loads(self.flight1.received_descriptors[-1])
        self.assertEqual(desc.get("type"), "edge")
        self.assertEqual(desc.get("edge_type"), "FRIENDS_WITH")


if __name__ == "__main__":
    unittest.main()
