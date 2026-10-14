package org.apache.spark.sql.gdb

import org.apache.spark.internal.Logging

import java.util.Locale

/**
 * Configuration options and parser for GDB Spark Connector.
 */
case class GdbOptions(
    url: String,
    entityType: String,
    label: Option[String],
    edgeType: Option[String],
    idCol: String,
    srcCol: String,
    dstCol: String,
    rankCol: Option[String],
    batchSize: Int,
    repartitionByRing: Boolean,
    compact: Boolean,
    timeoutMs: Long
) extends Serializable

object GdbOptions extends Logging {
  val URL = "url"
  val GDB_URL = "gdb.url"

  val ENTITY_TYPE = "type"
  val GDB_ENTITY_TYPE = "gdb.type"

  val LABEL = "label"
  val GDB_LABEL = "gdb.label"

  val EDGE_TYPE = "edgeType"
  val GDB_EDGE_TYPE = "gdb.edgeType"

  val ID_COL = "idCol"
  val GDB_ID_COL = "gdb.idCol"

  val SRC_COL = "srcCol"
  val GDB_SRC_COL = "gdb.srcCol"

  val DST_COL = "dstCol"
  val GDB_DST_COL = "gdb.dstCol"

  val RANK_COL = "rankCol"
  val GDB_RANK_COL = "gdb.rankCol"

  val BATCH_SIZE = "batchSize"
  val GDB_BATCH_SIZE = "gdb.batchSize"

  val REPARTITION_BY_RING = "repartitionByRing"
  val GDB_REPARTITION_BY_RING = "gdb.repartitionByRing"

  val COMPACT = "compact"
  val GDB_COMPACT = "gdb.compact"

  val TIMEOUT_MS = "timeoutMs"
  val GDB_TIMEOUT_MS = "gdb.timeoutMs"

  val DEFAULT_URL = "http://localhost:8847"
  val DEFAULT_ID_COL = "id"
  val DEFAULT_SRC_COL = "src"
  val DEFAULT_DST_COL = "dst"
  val DEFAULT_BATCH_SIZE = 16384
  val DEFAULT_REPARTITION_BY_RING = true
  val DEFAULT_COMPACT = true
  val DEFAULT_TIMEOUT_MS = 30000L

  def apply(parameters: Map[String, String]): GdbOptions = {
    val normalized = parameters.map { case (k, v) => (k.toLowerCase(Locale.ROOT), v) }

    def getOpt(keys: String*): Option[String] = {
      keys.view.flatMap(k => normalized.get(k.toLowerCase(Locale.ROOT))).headOption
    }

    val url = getOpt(URL, GDB_URL).getOrElse(DEFAULT_URL).stripSuffix("/")
    val entityType = getOpt(ENTITY_TYPE, GDB_ENTITY_TYPE).getOrElse("vertex").toLowerCase(Locale.ROOT)
    val label = getOpt(LABEL, GDB_LABEL)
    val edgeType = getOpt(EDGE_TYPE, GDB_EDGE_TYPE)
    val idCol = getOpt(ID_COL, GDB_ID_COL).getOrElse(DEFAULT_ID_COL)
    val srcCol = getOpt(SRC_COL, GDB_SRC_COL).getOrElse(DEFAULT_SRC_COL)
    val dstCol = getOpt(DST_COL, GDB_DST_COL).getOrElse(DEFAULT_DST_COL)
    val rankCol = getOpt(RANK_COL, GDB_RANK_COL)
    val batchSize = getOpt(BATCH_SIZE, GDB_BATCH_SIZE).map(_.toInt).getOrElse(DEFAULT_BATCH_SIZE)
    val repartitionByRing = getOpt(REPARTITION_BY_RING, GDB_REPARTITION_BY_RING).map(_.toBoolean).getOrElse(DEFAULT_REPARTITION_BY_RING)
    val compact = getOpt(COMPACT, GDB_COMPACT).map(_.toBoolean).getOrElse(DEFAULT_COMPACT)
    val timeoutMs = getOpt(TIMEOUT_MS, GDB_TIMEOUT_MS).map(_.toLong).getOrElse(DEFAULT_TIMEOUT_MS)

    GdbOptions(
      url = url,
      entityType = entityType,
      label = label,
      edgeType = edgeType,
      idCol = idCol,
      srcCol = srcCol,
      dstCol = dstCol,
      rankCol = rankCol,
      batchSize = batchSize,
      repartitionByRing = repartitionByRing,
      compact = compact,
      timeoutMs = timeoutMs
    )
  }
}
