package org.apache.spark.sql.gdb

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class GdbClusterTopologySuite extends AnyFunSuite with Matchers {

  test("Primary token routing maps keys to correct ring node index") {
    val node1 = GdbNodeInfo(1, "http://10.0.0.1:8847", "10.0.0.1", 8848, "10.0.0.1", 8860, "10.0.0.1")
    val node2 = GdbNodeInfo(2, "http://10.0.0.2:8847", "10.0.0.2", 8848, "10.0.0.2", 8861, "10.0.0.2")
    val node3 = GdbNodeInfo(3, "http://10.0.0.3:8847", "10.0.0.3", 8848, "10.0.0.3", 8862, "10.0.0.3")

    val topology = GdbClusterTopology(
      seedUrl = "http://10.0.0.1:8847",
      nodes = Array(node1, node2, node3),
      replicationFactor = 1,
      partitions = 4
    )

    topology.totalNodes shouldEqual 3

    // key % 3
    topology.targetNodeForKey(0).nodeId shouldEqual 1
    topology.targetNodeForKey(1).nodeId shouldEqual 2
    topology.targetNodeForKey(2).nodeId shouldEqual 3
    topology.targetNodeForKey(3).nodeId shouldEqual 1
    topology.targetNodeForKey(100).nodeId shouldEqual 2 // 100 % 3 = 1 -> index 1 is node 2
  }

  test("GdbRingPartitioner correctly partitions keys across cluster nodes") {
    val partitioner = new GdbRingPartitioner(3)
    partitioner.numPartitions shouldEqual 3

    partitioner.getPartition(0L) shouldEqual 0
    partitioner.getPartition(1L) shouldEqual 1
    partitioner.getPartition(2L) shouldEqual 2
    partitioner.getPartition(3L) shouldEqual 0
    partitioner.getPartition(100L) shouldEqual 1
  }
}
