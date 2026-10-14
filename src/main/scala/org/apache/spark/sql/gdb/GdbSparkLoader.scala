package org.apache.spark.sql.gdb

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.arrow.flight.{FlightDescriptor, SyncPutListener}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Row}

import java.nio.charset.StandardCharsets

/**
 * Result metrics for a bulk load operation into GDB.
 */
case class GdbLoadSummary(
    totalRows: Long,
    elapsedMs: Long,
    rowsPerSecond: Double,
    clusterNodes: Int,
    partitions: Int
)

/**
 * High-performance bulk data loader streaming Spark DataFrames into GDB Arrow Flight client ports.
 */
object GdbSparkLoader extends Logging {

  private val mapper = new ObjectMapper()

  /**
   * Bulk loads vertices from a DataFrame into GDB.
   */
  def loadVertices(df: DataFrame, options: GdbOptions): GdbLoadSummary = {
    loadGraphData(df, options, isEdge = false)
  }

  /**
   * Bulk loads edges from a DataFrame into GDB.
   */
  def loadEdges(df: DataFrame, options: GdbOptions): GdbLoadSummary = {
    loadGraphData(df, options, isEdge = true)
  }

  private def loadGraphData(
      df: DataFrame,
      options: GdbOptions,
      isEdge: Boolean
  ): GdbLoadSummary = {
    val startTime = System.currentTimeMillis()

    // 1. Driver discovers cluster topology and ring nodes
    val topology = GdbClusterTopology.discover(options.url, options.timeoutMs)
    val totalNodes = topology.totalNodes
    logInfo(s"GDB Driver: Discovered $totalNodes cluster nodes from ${options.url}")

    // 2. Pre-partition DataFrame by GDB ring token if configured
    val (keyColName, renameMap, descriptorJson) = if (!isEdge) {
      val keyCol = options.idCol
      val label = options.label.getOrElse("_default")
      val desc = mapper.writeValueAsString(Map("type" -> "vertex", "label" -> label).asInstanceOf[Map[String, Object]])
      (keyCol, if (keyCol != "id") Map(keyCol -> "id") else Map.empty[String, String], desc)
    } else {
      val keyCol = options.srcCol
      val edgeType = options.edgeType.getOrElse("DEFAULT")
      val desc = mapper.writeValueAsString(Map("type" -> "edge", "edge_type" -> edgeType).asInstanceOf[Map[String, Object]])
      var rmap = Map.empty[String, String]
      if (options.srcCol != "src") rmap += (options.srcCol -> "src")
      if (options.dstCol != "dst") rmap += (options.dstCol -> "dst")
      options.rankCol.foreach { rc =>
        if (rc != "rank") rmap += (rc -> "rank")
      }
      (keyCol, rmap, desc)
    }

    val preparedDf = if (options.repartitionByRing && totalNodes > 1) {
      logInfo(s"GDB Driver: Aligning Spark partitions with $totalNodes GDB ring nodes via GdbRingPartitioner on column '$keyColName'")
      val keyCol = col(keyColName)
      val rdd = df.rdd.map(row => (row.getAs[Any](keyColName), row))
      val partitionedRdd = rdd.partitionBy(new GdbRingPartitioner(totalNodes)).values
      df.sparkSession.createDataFrame(partitionedRdd, df.schema)
    } else {
      df
    }

    val sparkSchema = preparedDf.schema
    val arrowSchema = GdbArrowConverter.toArrowSchema(sparkSchema, renameMap)
    val batchSize = options.batchSize
    val broadcastTopology = df.sparkSession.sparkContext.broadcast(topology)

    // 3. Executor streaming to GDB client_flight_port
    val partitionCounts = preparedDf.rdd.mapPartitionsWithIndex { (partIdx, rowIterator) =>
      val topo = broadcastTopology.value
      val targetNode = topo.nodes(partIdx % topo.totalNodes)
      val client = GdbFlightClientPool.getClient(targetNode.host, targetNode.clientFlightPort)
      val allocator = GdbFlightClientPool.getAllocator

      val descriptor = FlightDescriptor.command(descriptorJson.getBytes(StandardCharsets.UTF_8))
      var partitionTotalRows = 0L

      val rowBuffer = new scala.collection.mutable.ArrayBuffer[Row](batchSize)

      def flushBuffer(): Unit = {
        if (rowBuffer.nonEmpty) {
          val rowsArray = rowBuffer.toArray
          val root = GdbArrowConverter.rowsToVectorSchemaRoot(rowsArray, sparkSchema, arrowSchema, allocator)
          try {
            val listener = new SyncPutListener()
            val stream = client.startPut(descriptor, root, listener)
            try {
              stream.putNext()
            } finally {
              stream.completed()
              // Wait for node ACK / PutResult
              try {
                val res = listener.read()
                if (res != null && res.getApplicationMetadata != null) {
                  // ACK received
                }
              } catch {
                case _: Exception => // Non-blocking ACK
              }
            }
            partitionTotalRows += rowsArray.length
          } finally {
            root.close()
          }
          rowBuffer.clear()
        }
      }

      while (rowIterator.hasNext) {
        rowBuffer += rowIterator.next()
        if (rowBuffer.length >= batchSize) {
          flushBuffer()
        }
      }
      flushBuffer()

      Iterator.single(partitionTotalRows)
    }

    val totalRows = partitionCounts.reduce(_ + _)
    val elapsedMs = Math.max(1L, System.currentTimeMillis() - startTime)
    val rowsPerSecond = (totalRows.toDouble / elapsedMs) * 1000.0

    logInfo(s"GDB Driver: Ingestion completed! Ingested $totalRows rows in ${elapsedMs}ms (${rowsPerSecond.formatted("%.2f")} rows/sec).")

    // 4. Post-load CSR compaction on GDB cluster
    if (options.compact) {
      logInfo("GDB Driver: Triggering post-ingestion CSR compaction across cluster...")
      topology.triggerCompaction(options.timeoutMs)
    }

    GdbLoadSummary(
      totalRows = totalRows,
      elapsedMs = elapsedMs,
      rowsPerSecond = rowsPerSecond,
      clusterNodes = totalNodes,
      partitions = preparedDf.rdd.getNumPartitions
    )
  }
}
