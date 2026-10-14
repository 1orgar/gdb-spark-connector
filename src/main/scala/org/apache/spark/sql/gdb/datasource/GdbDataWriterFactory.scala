package org.apache.spark.sql.gdb.datasource

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.{DataWriter, DataWriterFactory}
import org.apache.spark.sql.gdb.{GdbClusterTopology, GdbOptions}
import org.apache.spark.sql.types.StructType

/**
 * Factory creating partition data writers on Spark Executors.
 */
class GdbDataWriterFactory(
    sparkSchema: StructType,
    options: GdbOptions,
    topology: GdbClusterTopology
) extends DataWriterFactory {

  override def createWriter(partitionId: Int, taskId: Long): DataWriter[InternalRow] = {
    new GdbDataWriter(partitionId, taskId, sparkSchema, options, topology)
  }
}
