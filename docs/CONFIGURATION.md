# GDB Spark Connector Configuration Guide

This document lists all configuration keys supported by `gdb-spark-connector` for both **Spark DataSource V2** and the programmatic **`GdbSparkLoader`** / **`GdbOptions`**.

---

## 1. Options Reference Table

| Key | Alternative / Prefix | Type | Default | Description |
|---|---|---|---|---|
| `url` | `gdb.url` | String | `http://localhost:8847` | Coordinator HTTP URL for topology discovery and compaction |
| `type` | `gdb.type` | String | `vertex` | Ingestion type: `"vertex"` or `"edge"` |
| `label` | `gdb.label` | String | `_default` | Label for vertices (e.g. `User`, `Device`) |
| `edgeType` | `gdb.edgeType` | String | `DEFAULT` | Edge type (e.g. `FOLLOWS`, `CONNECTS_TO`) |
| `idCol` | `gdb.idCol` | String | `id` | Name of the column representing Vertex ID |
| `srcCol` | `gdb.srcCol` | String | `src` | Name of the column representing Source Vertex ID |
| `dstCol` | `gdb.dstCol` | String | `dst` | Name of the column representing Target Vertex ID |
| `rankCol` | `gdb.rankCol` | String | `rank` | Optional column name representing Edge rank (default 0) |
| `batchSize` | `gdb.batchSize` | Int | `16384` | Maximum rows per Arrow `RecordBatch` |
| `repartitionByRing` | `gdb.repartitionByRing` | Boolean | `true` | Pre-shuffle Spark DataFrame by GDB token ring (`key % N`) |
| `compact` | `gdb.compact` | Boolean | `true` | Automatically trigger CSR compaction after bulk loading |
| `timeoutMs` | `gdb.timeoutMs` | Long | `30000` | Connection and request timeout in milliseconds |

---

## 2. Spark-Submit Example

When deploying to a cluster using `spark-submit`:

```bash
spark-submit \
  --class examples.SparkFlightLoaderExample \
  --master yarn \
  --deploy-mode cluster \
  --num-executors 10 \
  --executor-cores 4 \
  --executor-memory 8G \
  --jars /path/to/gdb-spark-connector-assembly-0.5.6-spark-3.5.0.jar \
  my-spark-job.jar \
  http://gdb-coordinator.internal:8847
```

---

## 3. High-Throughput Tuning Tips

- **Batch Size (`batchSize`)**:
  - Default: `16384` rows.
  - For narrow schemas (e.g. simple graph edges: `src`, `dst`, `weight`), increasing `batchSize` to `32768` or `65536` yields higher throughput.
  - For wide schemas (vertices with 20+ string/vector attributes), decreasing `batchSize` to `8192` prevents JVM off-heap memory spikes.
- **Spark Partitions (`repartitionByRing`)**:
  - Keep `repartitionByRing = true` to allow each Spark executor task to stream sequentially to a single GDB node.
  - If the dataset has millions of records and the cluster has only 3 nodes, you can repartition into a multiple of $N$ (e.g., $3 \times 4 = 12$ partitions) to allow concurrent executor tasks while preserving node affinity:
    $$(\text{key} \pmod N) + N \times (\text{task\_bucket})$$
