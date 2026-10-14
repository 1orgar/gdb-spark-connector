package org.apache.spark.sql.gdb

import org.apache.spark.Partitioner

/**
 * Custom Spark Partitioner aligning DataFrame partitions with GDB Ring Nodes.
 *
 * Each partition index i directly corresponds to GDB ring node i (primary token owner).
 *
 *   Partition = key % numNodes
 */
class GdbRingPartitioner(val numNodes: Int) extends Partitioner {
  require(numNodes > 0, s"Number of nodes must be positive, got $numNodes")

  override def numPartitions: Int = numNodes

  override def getPartition(key: Any): Int = {
    if (key == null) {
      0
    } else {
      key match {
        case l: java.lang.Long =>
          ((l.longValue() & 0x7ffffffffffffffL) % numNodes).toInt
        case i: java.lang.Integer =>
          ((i.longValue() & 0x7ffffffffffffffL) % numNodes).toInt
        case s: java.lang.Short =>
          ((s.longValue() & 0x7ffffffffffffffL) % numNodes).toInt
        case b: java.lang.Byte =>
          ((b.longValue() & 0x7ffffffffffffffL) % numNodes).toInt
        case str: String =>
          ((str.hashCode.toLong & 0x7ffffffffffffffL) % numNodes).toInt
        case other =>
          ((other.hashCode().toLong & 0x7ffffffffffffffL) % numNodes).toInt
      }
    }
  }

  override def equals(other: Any): Boolean = other match {
    case p: GdbRingPartitioner => p.numNodes == numNodes
    case _                     => false
  }

  override def hashCode(): Int = numNodes.hashCode()
}
