# GDB Spark / PySpark Connector (`gdb_spark`)

High-Throughput Apache Spark / PySpark connector for **GDB Graph Database**.

Features:
- **Driver Topology Discovery**: Queries GDB cluster HTTP API (`GET /cluster`), discovering live ring nodes and dedicated `client_flight_port`s.
- **Direct Executor Arrow Flight Streaming**: Executors stream Arrow `RecordBatch`es concurrently directly into the `client-flight` ports of respective primary ring nodes (`vid % N` or `src % N`).
- **Zero-Copy & Low Latency**: Utilizes Apache Arrow Flight protocol for gRPC high-speed streaming.
- **Automatic Post-Load Compaction**: Triggers CSR compaction across all cluster nodes upon completion for immediate openCypher query readiness.

## Installation

```bash
pip install -e ./python
```

Or install with PySpark dependencies:
```bash
pip install -e "./python[spark]"
```

## Quick Start (PySpark)

```python
from pyspark.sql import SparkSession
from gdb_spark import GdbSparkLoader, GdbOptions

spark = SparkSession.builder \
    .appName("GdbIngestExample") \
    .getOrCreate()

# 1. Ingest Vertices
df_users = spark.read.parquet("hdfs:///data/users.parquet")
# Expected columns: id, name, age, etc.

summary = GdbSparkLoader.load_vertices(
    df_users,
    options=GdbOptions(
        url="http://localhost:8847",
        label="User",
        id_col="id",
        batch_size=16384,
        compact=True,
    )
)
print(f"Ingested {summary.total_rows} vertices in {summary.elapsed_seconds}s ({summary.rows_per_second} rows/s)")

# 2. Ingest Edges
df_follows = spark.read.parquet("hdfs:///data/follows.parquet")
# Expected columns: src, dst, rank, etc.

summary_edges = GdbSparkLoader.load_edges(
    df_follows,
    options=GdbOptions(
        url="http://localhost:8847",
        edge_type="FOLLOWS",
        src_col="src",
        dst_col="dst",
        batch_size=16384,
        compact=True,
    )
)
print(f"Ingested {summary_edges.total_rows} edges in {summary_edges.elapsed_seconds}s ({summary_edges.rows_per_second} rows/s)")
```
