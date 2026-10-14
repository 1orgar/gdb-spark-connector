package org.apache.spark.sql.gdb.datasource

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.arrow.flight.{FlightDescriptor, SyncPutListener}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.{DataWriter, WriterCommitMessage}
import org.apache.spark.sql.gdb.{GdbArrowConverter, GdbClusterTopology, GdbFlightClientPool, GdbOptions}
import org.apache.spark.sql.types.StructType

import java.nio.charset.StandardCharsets

/**
 * Executor partition data writer streaming InternalRows to GDB Arrow Flight client ports.
 */
class GdbDataWriter(
    partitionId: Int,
    taskId: Long,
    sparkSchema: StructType,
    options: GdbOptions,
    topology: GdbClusterTopology
) extends DataWriter[InternalRow] with Logging {

  private val mapper = new ObjectMapper()
  private val isEdge = options.entityType.equalsIgnoreCase("edge")

  private val (renameMap, descriptorJson) = if (!isEdge) {
    val label = options.label.getOrElse("_default")
    val desc = mapper.writeValueAsString(Map("type" -> "vertex", "label" -> label).asInstanceOf[Map[String, Object]])
    val rmap = if (options.idCol != "id") Map(options.idCol -> "id") else Map.empty[String, String]
    (rmap, desc)
  } else {
    val edgeType = options.edgeType.getOrElse("DEFAULT")
    val desc = mapper.writeValueAsString(Map("type" -> "edge", "edge_type" -> edgeType).asInstanceOf[Map[String, Object]])
    var rmap = Map.empty[String, String]
    if (options.srcCol != "src") rmap += (options.srcCol -> "src")
    if (options.dstCol != "dst") rmap += (options.dstCol -> "dst")
    options.rankCol.foreach { rc =>
      if (rc != "rank") rmap += (rc -> "rank")
    }
    (rmap, desc)
  }

  private val arrowSchema = GdbArrowConverter.toArrowSchema(sparkSchema, renameMap)
  private val targetNode = topology.nodes(partitionId % topology.totalNodes)
  private val client = GdbFlightClientPool.getClient(targetNode.host, targetNode.clientFlightPort)
  private val allocator = GdbFlightClientPool.getAllocator
  private val descriptor = FlightDescriptor.command(descriptorJson.getBytes(StandardCharsets.UTF_8))

  private val batchSize = options.batchSize
  private val buffer = new scala.collection.mutable.ArrayBuffer[InternalRow](batchSize)
  private var totalWritten = 0L

  override def write(record: InternalRow): Unit = {
    buffer += record.copy()
    if (buffer.length >= batchSize) {
      flush()
    }
  }

  private def flush(): Unit = {
    if (buffer.nonEmpty) {
      val rowsArray = buffer.toArray
      val root = GdbArrowConverter.internalRowsToVectorSchemaRoot(rowsArray, sparkSchema, arrowSchema, allocator)
      try {
        val listener = new SyncPutListener()
        val stream = client.startPut(descriptor, root, listener)
        try {
          stream.putNext()
        } finally {
          stream.completed()
          try {
            val res = listener.read()
            if (res != null) {
              // Node acknowledged receipt
            }
          } catch {
            case _: Exception => // Non-blocking ACK
          }
        }
        totalWritten += rowsArray.length
      } finally {
        root.close()
      }
      buffer.clear()
    }
  }

  override def commit(): WriterCommitMessage = {
    flush()
    logInfo(s"GDB Executor [Partition $partitionId / Task $taskId]: Committed $totalWritten rows to ${targetNode.flightEndpointUri}.")
    GdbWriterCommitMessage(partitionId, totalWritten)
  }

  override def abort(): Unit = {
    buffer.clear()
    logWarning(s"GDB Executor [Partition $partitionId / Task $taskId]: Aborted.")
  }

  override def close(): Unit = {
    buffer.clear()
  }
}
