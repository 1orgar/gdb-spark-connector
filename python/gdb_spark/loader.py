from dataclasses import dataclass, field
from typing import Optional, Dict, Any
import json
import time
import logging
import pyarrow as pa
import pyarrow.flight as flight

from .topology import GdbClusterTopology, GdbNodeInfo

logger = logging.getLogger("gdb_spark.loader")


@dataclass
class GdbOptions:
    url: str = "http://localhost:8847"
    entity_type: str = "vertex"  # "vertex" or "edge"
    label: Optional[str] = None
    edge_type: Optional[str] = None
    id_col: str = "id"
    src_col: str = "src"
    dst_col: str = "dst"
    rank_col: Optional[str] = None
    batch_size: int = 16384
    repartition_by_ring: bool = True
    compact: bool = True
    timeout: float = 30.0


@dataclass
class GdbLoadSummary:
    total_rows: int
    elapsed_seconds: float
    rows_per_second: float
    cluster_nodes: int
    partitions: int


class GdbSparkLoader:
    """
    High-performance Arrow Flight Bulk Loader for Spark / PySpark DataFrames.
    Streams directly into GDB client-flight ports with driver topology discovery.
    """

    @classmethod
    def load_vertices(cls, df: Any, options: Optional[GdbOptions] = None, **kwargs) -> GdbLoadSummary:
        if options is None:
            options = GdbOptions(**kwargs)
        options.entity_type = "vertex"
        return cls._load_data(df, options, is_edge=False)

    @classmethod
    def load_edges(cls, df: Any, options: Optional[GdbOptions] = None, **kwargs) -> GdbLoadSummary:
        if options is None:
            options = GdbOptions(**kwargs)
        options.entity_type = "edge"
        return cls._load_data(df, options, is_edge=True)

    @classmethod
    def _load_data(cls, df: Any, options: GdbOptions, is_edge: bool) -> GdbLoadSummary:
        start_time = time.perf_counter()

        # 1. Driver discovers cluster topology
        topology = GdbClusterTopology.discover(options.url, timeout=options.timeout)
        total_nodes = topology.total_nodes
        logger.info("GDB Driver: Discovered %d cluster nodes from %s", total_nodes, options.url)

        # 2. Prepare descriptor command
        if not is_edge:
            key_col = options.id_col
            desc_dict = {"type": "vertex", "label": options.label or "_default"}
            rename_map = {options.id_col: "id"} if options.id_col != "id" else {}
        else:
            key_col = options.src_col
            desc_dict = {"type": "edge", "edge_type": options.edge_type or "DEFAULT"}
            rename_map = {}
            if options.src_col != "src":
                rename_map[options.src_col] = "src"
            if options.dst_col != "dst":
                rename_map[options.dst_col] = "dst"
            if options.rank_col and options.rank_col != "rank":
                rename_map[options.rank_col] = "rank"

        desc_cmd = json.dumps(desc_dict).encode("utf-8")

        # 3. Handle PySpark DataFrame vs In-Memory Table
        is_pyspark = hasattr(df, "rdd") and hasattr(df, "repartition")

        if is_pyspark:
            import pyspark.sql.functions as F

            prepared_df = df
            if options.repartition_by_ring and total_nodes > 1:
                logger.info("Aligning PySpark DataFrame with %d ring nodes on '%s'...", total_nodes, key_col)
                # Compute partition expression: abs(col(key_col)) % total_nodes
                part_col = (F.abs(F.col(key_col)) % total_nodes).cast("int")
                prepared_df = df.repartition(total_nodes, part_col)

            nodes_info = [(n.host, n.client_flight_port) for n in topology.nodes]
            batch_size = options.batch_size

            def _stream_partition(part_idx, rows_iter):
                host, port = nodes_info[part_idx % len(nodes_info)]
                client = flight.FlightClient(f"grpc://{host}:{port}")
                descriptor = flight.FlightDescriptor.for_command(desc_cmd)

                rows_buffer = []
                partition_count = 0

                def _flush_buffer(buffer):
                    if not buffer:
                        return 0
                    # Convert list of Rows/dicts to PyArrow Table
                    dict_batch = {}
                    field_names = buffer[0].__fields__ if hasattr(buffer[0], "__fields__") else list(buffer[0].asDict().keys())
                    for fn in field_names:
                        target_name = rename_map.get(fn, fn)
                        dict_batch[target_name] = [r[fn] for r in buffer]

                    pa_table = pa.Table.from_pydict(dict_batch)
                    writer, reader = client.do_put(descriptor, pa_table.schema)
                    try:
                        writer.write_table(pa_table, max_chunksize=batch_size)
                    finally:
                        writer.close()
                    try:
                        reader.read()
                    except Exception:
                        pass
                    return len(buffer)

                for row in rows_iter:
                    rows_buffer.append(row)
                    if len(rows_buffer) >= batch_size:
                        partition_count += _flush_buffer(rows_buffer)
                        rows_buffer.clear()

                if rows_buffer:
                    partition_count += _flush_buffer(rows_buffer)
                    rows_buffer.clear()

                client.close()
                yield partition_count

            counts = prepared_df.rdd.mapPartitionsWithIndex(_stream_partition).collect()
            total_rows = sum(counts)
            partitions_count = len(counts)

        else:
            # Handle PyArrow Table or Polars / Pandas DataFrame directly
            if not isinstance(df, pa.Table):
                if hasattr(df, "to_arrow"):
                    pa_table = df.to_arrow()
                elif hasattr(df, "to_dict"):
                    pa_table = pa.Table.from_pandas(df)
                else:
                    raise TypeError(f"Unsupported DataFrame type: {type(df)}")
            else:
                pa_table = df

            # Rename columns if needed
            if rename_map:
                names = [rename_map.get(name, name) for name in pa_table.column_names]
                pa_table = pa_table.rename_columns(names)

            actual_key_col = rename_map.get(key_col, key_col)
            key_array = pa_table[actual_key_col]

            # Split by node index: (key % total_nodes)
            if pa.types.is_integer(key_array.type):
                node_indices = [abs(val.as_py()) % total_nodes for val in key_array]
            else:
                node_indices = [abs(hash(str(val.as_py()))) % total_nodes for val in key_array]

            total_rows = 0
            for node_idx, node in enumerate(topology.nodes):
                mask = [idx == node_idx for idx in node_indices]
                if not any(mask):
                    continue
                sub_table = pa_table.filter(pa.array(mask))
                if sub_table.num_rows == 0:
                    continue

                client = flight.FlightClient(f"grpc://{node.host}:{node.client_flight_port}")
                descriptor = flight.FlightDescriptor.for_command(desc_cmd)
                writer, reader = client.do_put(descriptor, sub_table.schema)
                try:
                    writer.write_table(sub_table, max_chunksize=options.batch_size)
                finally:
                    writer.close()
                try:
                    reader.read()
                except Exception:
                    pass
                client.close()
                total_rows += sub_table.num_rows

            partitions_count = total_nodes

        elapsed = time.perf_counter() - start_time
        rps = total_rows / elapsed if elapsed > 0 else 0.0

        logger.info("Ingested %d rows in %.3fs (%.2f rows/sec)", total_rows, elapsed, rps)

        # 4. Trigger post-load compaction if configured
        if options.compact:
            logger.info("GDB Driver: Triggering CSR compaction across cluster...")
            topology.trigger_compaction(timeout=options.timeout)

        return GdbLoadSummary(
            total_rows=total_rows,
            elapsed_seconds=round(elapsed, 4),
            rows_per_second=round(rps, 2),
            cluster_nodes=total_nodes,
            partitions=partitions_count,
        )
