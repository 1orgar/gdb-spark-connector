"""
GDB Spark Connector - High-Throughput Arrow Flight Loader for Apache Spark / PySpark.
"""

from .topology import GdbClusterTopology, GdbNodeInfo
from .loader import GdbSparkLoader, GdbOptions

__version__ = "0.5.6"

__all__ = [
    "GdbClusterTopology",
    "GdbNodeInfo",
    "GdbSparkLoader",
    "GdbOptions",
    "__version__",
]
