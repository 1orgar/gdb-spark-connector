package org.apache.spark.sql.gdb.datasource

import org.apache.spark.sql.connector.catalog.{SupportsWrite, Table, TableCapability}
import org.apache.spark.sql.connector.write.{BatchWrite, LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.gdb.{GdbClusterTopology, GdbOptions}
import org.apache.spark.sql.types.StructType

import java.util
import scala.collection.JavaConverters._

/**
 * GDB Table abstraction implementing Spark SupportsWrite.
 */
class GdbTable(
    val sparkSchema: StructType,
    val options: GdbOptions
) extends Table with SupportsWrite {

  override def name(): String = s"GDB:${options.url}:${options.entityType}:${options.label.orElse(options.edgeType).getOrElse("default")}"

  override def schema(): StructType = sparkSchema

  override def capabilities(): util.Set[TableCapability] = {
    Set(
      TableCapability.BATCH_WRITE,
      TableCapability.ACCEPT_ANY_SCHEMA
    ).asJava
  }

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
    new WriteBuilder {
      override def buildForBatch(): BatchWrite = {
        val topology = GdbClusterTopology.discover(options.url, options.timeoutMs)
        new GdbBatchWrite(sparkSchema, options, topology)
      }
    }
  }
}
