package org.apache.spark.sql.gdb.datasource

import org.apache.spark.sql.connector.catalog.{Table, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.gdb.GdbOptions
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util
import scala.collection.JavaConverters._

/**
 * Standard Spark DataSource V2 Provider for format("gdb").
 */
class DefaultSource extends TableProvider with DataSourceRegister {

  override def shortName(): String = "gdb"

  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    // Schema is provided by the DataFrame being written
    new StructType()
  }

  override def getTable(
      schema: StructType,
      partitioning: Array[Transform],
      properties: util.Map[String, String]
  ): Table = {
    val scalaMap = properties.asScala.toMap
    val options = GdbOptions(scalaMap)
    new GdbTable(schema, options)
  }

  override def supportsExternalMetadata(): Boolean = true
}
