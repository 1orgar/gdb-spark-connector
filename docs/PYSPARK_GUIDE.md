# PySpark Guide for GDB Spark Connector

The `gdb-spark` Python package provides a lightweight, pure-Python wrapper around `pyarrow.flight` and PySpark's distributed execution engine.

---

## 1. Installation

```bash
pip install gdb-spark
```

Or install from the repository source:

```bash
git clone https://github.com/1orgar/gdb-spark-connector.git
cd gdb-spark-connector/python
pip install -e .
```

---

## 2. Basic Example: Ingesting a Graph Dataset

```python
from pyspark.sql import SparkSession
from gdb_spark import GdbSparkLoader, GdbOptions

spark = SparkSession.builder \
    .appName("GdbIngestApp") \
    .getOrCreate()

# 1. Ingest Vertices (Customers)
customers_df = spark.read.parquet("hdfs:///data/customers.parquet")

v_summary = GdbSparkLoader.load_vertices(
    customers_df,
    options=GdbOptions(
        url="http://gdb-coordinator:8847",
        label="Customer",
        id_col="customer_id",
        batch_size=16384,
        compact=True
    )
)
print(f"Loaded {v_summary.total_rows} vertices in {v_summary.elapsed_seconds}s ({v_summary.rows_per_second} rows/s)")

# 2. Ingest Edges (Transactions)
tx_df = spark.read.parquet("hdfs:///data/transactions.parquet")

e_summary = GdbSparkLoader.load_edges(
    tx_df,
    options=GdbOptions(
        url="http://gdb-coordinator:8847",
        edge_type="TRANSFERRED_TO",
        src_col="sender_id",
        dst_col="receiver_id",
        rank_col="transaction_id",
        batch_size=16384,
        compact=True
    )
)
print(f"Loaded {e_summary.total_rows} edges in {e_summary.elapsed_seconds}s ({e_summary.rows_per_second} rows/s)")
```

---

## 3. Direct PyArrow / Polars Ingestion

If running in a single-machine ETL script without a full Spark cluster, `GdbSparkLoader` also directly accepts `pyarrow.Table` or Polars/Pandas DataFrames:

```python
import polars as pl
from gdb_spark import GdbSparkLoader, GdbOptions

df = pl.read_parquet("accounts.parquet")
summary = GdbSparkLoader.load_vertices(
    df,
    options=GdbOptions(
        url="http://localhost:8847",
        label="Account",
        id_col="account_id"
    )
)
```
