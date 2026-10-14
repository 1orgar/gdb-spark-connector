package org.apache.spark.sql.gdb.datasource

import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.write.{BatchWrite, DataWriterFactory, PhysicalWriteInfo, WriterCommitMessage}
import org.apache.spark.sql.gdb.{GdbClusterTopology, GdbOptions}
import org.apache.spark.sql.types.StructType

/**
 * Commit message containing row counts from a partition writer.
 */
case class GdbWriterCommitMessage(partitionId: Int, rowsWritten: Long) extends WriterCommitMessage

/**
 * BatchWrite implementation coordinating Spark writes across executors.
 */
class GdbBatchWrite(
    sparkSchema: StructType,
    options: GdbOptions,
    topology: GdbClusterTopology
) extends BatchWrite with Logging {

  override def createBatchWriterFactory(info: PhysicalWriteInfo): DataWriterFactory = {
    new GdbDataWriterFactory(sparkSchema, options, topology)
  }

  override def commit(messages: Array[WriterCommitMessage]): Unit = {
    val totalRows = messages.collect { case m: GdbWriterCommitMessage => m.rowsWritten }.sum
    logInfo(s"GDB Driver: Successfully committed write of $totalRows rows across ${messages.length} partitions.")

    if (options.compact) {
      logInfo("GDB Driver: Triggering post-write CSR compaction...")
      topology.triggerCompaction(options.timeoutMs)
    }
  }

  override def abort(messages: Array[WriterCommitMessage]): Unit = {
    logWarning(s"GDB Driver: Write operation aborted on ${messages.length} partitions.")
  }
}
