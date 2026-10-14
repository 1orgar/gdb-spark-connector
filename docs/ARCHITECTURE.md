# GDB Spark Connector: Architecture & Technical Design

## 1. High-Level Design Principles

The primary design goal of `gdb-spark-connector` is to achieve maximum ingestion throughput into **GDB** from distributed Spark clusters by avoiding the traditional bottlenecks of single-point coordinators, intermediate JSON serialization, and excessive JDBC/HTTP overhead.

```
+-------------------------------------------------------------------------+
|                              SPARK DRIVER                               |
|                                                                         |
|  1. Discovers cluster topology via HTTP GET /cluster                    |
|  2. Extracts N ring nodes & external client_flight_port endpoints       |
|  3. Plans pre-shuffle via GdbRingPartitioner (key % N)                  |
|  4. Broadcasts topology metadata to Spark Executors                     |
+-------------------------------------------------------------------------+
                                     |
                                     v
+----------------------- SPARK EXECUTOR CLUSTER --------------------------+
|                                                                         |
|  Executor Partition 0 -------- Flight DoPut -------> GDB Node 1:8860   |
|  Executor Partition 1 -------- Flight DoPut -------> GDB Node 2:8861   |
|  ...                                                                    |
|  Executor Partition N-1 ------ Flight DoPut -------> GDB Node N:886X   |
|                                                                         |
+-------------------------------------------------------------------------+
                                     |
                                     v
+-------------------------------------------------------------------------+
|                         POST-LOAD CSR COMPACTION                        |
|                                                                         |
|  Driver invokes HTTP POST /query with `COMPACT;` on all cluster nodes   |
|  Data moves from DeltaMemTable into Compressed Sparse Row (CSR)         |
+-------------------------------------------------------------------------+
```

---

## 2. Driver-Side Discovery & Topology

GDB exposes cluster metadata via `GET /cluster` on its coordinator HTTP interface (default port `8847`). The Spark Driver fetches this document during planning:

```json
{
  "node_id": 1,
  "role": "Coordinator",
  "replication_factor": 1,
  "ring_nodes": [
    {
      "node_id": 1,
      "http_url": "http://10.0.0.1:8847",
      "flight_address": "10.0.0.1",
      "flight_port": 8848,
      "client_flight_address": "10.0.0.1",
      "client_flight_port": 8860
    },
    {
      "node_id": 2,
      "http_url": "http://10.0.0.2:8847",
      "flight_address": "10.0.0.2",
      "flight_port": 8849,
      "client_flight_address": "10.0.0.2",
      "client_flight_port": 8861
    }
  ]
}
```

The driver normalizes announced IPs (resolving loopback `127.0.0.1` or `0.0.0.0` to the remote coordinator hostname when running outside the database host) and wraps this into a serializable `GdbClusterTopology` broadcast variable.

---

## 3. Ring Token Partitioning (`GdbRingPartitioner`)

In GDB's distributed architecture:
- Primary token for vertex $u$ is $u \pmod N$.
- Primary token for edge $(u, v)$ is $u \pmod N$.

To eliminate cross-executor scatter, `GdbRingPartitioner` performs a Spark shuffle stage before writing:
$$\text{Partition ID} = (\text{Key} \ \& \ \text{0x7FFFFFFFFFFFFFFFL}) \pmod N$$

When `repartitionByRing = true` (default):
- Spark partition $i$ is guaranteed to contain only records destined for GDB node $i$.
- Spark executor writes sequentially to a single gRPC endpoint (`client_flight_port`), maintaining steady TCP window sizes and zero cross-node fanout.

---

## 4. Arrow Flight Protocol & Zero-Copy Streaming

### RecordBatch Representation
Each executor chunks rows into Apache Arrow `RecordBatch`es (default: 16,384 rows). Field vectors are allocated in off-heap memory using Netty `RootAllocator`:
- `LongType` $\rightarrow$ `BigIntVector`
- `IntegerType` $\rightarrow$ `IntVector`
- `DoubleType` $\rightarrow$ `Float8Vector`
- `StringType` $\rightarrow$ `VarCharVector`
- `BooleanType` $\rightarrow$ `BitVector`

### Flight Descriptor Command
Before streaming vectors, a command descriptor is attached to the flight stream:
- **Vertices**: `{"type": "vertex", "label": "<Label>"}`
- **Edges**: `{"type": "edge", "edge_type": "<EdgeType>"}`

GDB's `GdbClientFlightService` receives these batches via `do_put`, deserializes the IPC stream directly, and inserts entities directly into the local node's `DeltaMemTable`.

---

## 5. Post-Load Compaction

Data ingested through Arrow Flight resides in memory in mutable append-only delta structures (`DeltaMemTable`). While immediately queryable, large scans perform with optimal cache locality when compacted into CSR (Compressed Sparse Row) vectors.

When `compact = true` (default), the Spark Driver executes HTTP POST queries `COMPACT;` to all discovered ring nodes upon successful write completion.
