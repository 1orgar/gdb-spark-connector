package org.apache.spark.sql.gdb

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector._
import org.apache.arrow.vector.types.DateUnit
import org.apache.arrow.vector.types.FloatingPointPrecision
import org.apache.arrow.vector.types.TimeUnit
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._

import java.nio.charset.StandardCharsets
import scala.collection.JavaConverters._

/**
 * High-performance converter between Spark schemas/rows and Apache Arrow VectorSchemaRoot / RecordBatches.
 */
object GdbArrowConverter {

  /**
   * Translates Spark StructType to Arrow Schema, optionally applying column renames (e.g. custom id -> "id").
   */
  def toArrowSchema(
      sparkSchema: StructType,
      renameMap: Map[String, String] = Map.empty
  ): Schema = {
    val fields = sparkSchema.fields.map { sf =>
      val targetName = renameMap.getOrElse(sf.name, sf.name)
      val arrowType = toArrowType(sf.dataType)
      new Field(targetName, new FieldType(sf.nullable, arrowType, null), null)
    }
    new Schema(fields.toList.asJava)
  }

  def toArrowType(sparkType: DataType): ArrowType = sparkType match {
    case LongType      => new ArrowType.Int(64, true)
    case IntegerType   => new ArrowType.Int(32, true)
    case ShortType     => new ArrowType.Int(16, true)
    case ByteType      => new ArrowType.Int(8, true)
    case DoubleType    => new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)
    case FloatType     => new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)
    case StringType    => new ArrowType.Utf8()
    case BooleanType   => new ArrowType.Bool()
    case DateType      => new ArrowType.Date(DateUnit.DAY)
    case TimestampType => new ArrowType.Timestamp(TimeUnit.MILLISECOND, null)
    case other =>
      throw new UnsupportedOperationException(s"Unsupported Spark data type for Arrow Flight conversion: $other")
  }

  /**
   * Converts a slice of Spark Rows into an Arrow VectorSchemaRoot.
   */
  def rowsToVectorSchemaRoot(
      rows: Array[Row],
      sparkSchema: StructType,
      arrowSchema: Schema,
      allocator: BufferAllocator
  ): VectorSchemaRoot = {
    val root = VectorSchemaRoot.create(arrowSchema, allocator)
    val rowCount = rows.length
    root.setRowCount(rowCount)

    val fieldCount = sparkSchema.fields.length
    for (colIdx <- 0 until fieldCount) {
      val field = sparkSchema.fields(colIdx)
      val vector = root.getVector(colIdx)
      vector.setInitialCapacity(rowCount)
      vector.allocateNew()

      populateVectorFromRows(vector, field.dataType, rows, colIdx)
      vector.setValueCount(rowCount)
    }

    root
  }

  /**
   * Converts an iterator of Spark InternalRows into an Arrow VectorSchemaRoot.
   */
  def internalRowsToVectorSchemaRoot(
      rows: Array[InternalRow],
      sparkSchema: StructType,
      arrowSchema: Schema,
      allocator: BufferAllocator
  ): VectorSchemaRoot = {
    val root = VectorSchemaRoot.create(arrowSchema, allocator)
    val rowCount = rows.length
    root.setRowCount(rowCount)

    val fieldCount = sparkSchema.fields.length
    for (colIdx <- 0 until fieldCount) {
      val field = sparkSchema.fields(colIdx)
      val vector = root.getVector(colIdx)
      vector.setInitialCapacity(rowCount)
      vector.allocateNew()

      populateVectorFromInternalRows(vector, field.dataType, rows, colIdx)
      vector.setValueCount(rowCount)
    }

    root
  }

  private def populateVectorFromRows(
      vector: FieldVector,
      dataType: DataType,
      rows: Array[Row],
      colIdx: Int
  ): Unit = {
    val len = rows.length
    dataType match {
      case LongType =>
        val v = vector.asInstanceOf[BigIntVector]
        for (i <- 0 until len) {
          if (rows(i).isNullAt(colIdx)) v.setNull(i)
          else v.setSafe(i, rows(i).getLong(colIdx))
        }
      case IntegerType =>
        val v = vector.asInstanceOf[IntVector]
        for (i <- 0 until len) {
          if (rows(i).isNullAt(colIdx)) v.setNull(i)
          else v.setSafe(i, rows(i).getInt(colIdx))
        }
      case ShortType =>
        val v = vector.asInstanceOf[SmallIntVector]
        for (i <- 0 until len) {
          if (rows(i).isNullAt(colIdx)) v.setNull(i)
          else v.setSafe(i, rows(i).getShort(colIdx))
        }
      case ByteType =>
        val v = vector.asInstanceOf[TinyIntVector]
        for (i <- 0 until len) {
          if (rows(i).isNullAt(colIdx)) v.setNull(i)
          else v.setSafe(i, rows(i).getByte(colIdx))
        }
      case DoubleType =>
        val v = vector.asInstanceOf[Float8Vector]
        for (i <- 0 until len) {
          if (rows(i).isNullAt(colIdx)) v.setNull(i)
          else v.setSafe(i, rows(i).getDouble(colIdx))
        }
      case FloatType =>
        val v = vector.asInstanceOf[Float4Vector]
        for (i <- 0 until len) {
          if (rows(i).isNullAt(colIdx)) v.setNull(i)
          else v.setSafe(i, rows(i).getFloat(colIdx))
        }
      case StringType =>
        val v = vector.asInstanceOf[VarCharVector]
        for (i <- 0 until len) {
          if (rows(i).isNullAt(colIdx)) v.setNull(i)
          else {
            val bytes = rows(i).getString(colIdx).getBytes(StandardCharsets.UTF_8)
            v.setSafe(i, bytes, 0, bytes.length)
          }
        }
      case BooleanType =>
        val v = vector.asInstanceOf[BitVector]
        for (i <- 0 until len) {
          if (rows(i).isNullAt(colIdx)) v.setNull(i)
          else v.setSafe(i, if (rows(i).getBoolean(colIdx)) 1 else 0)
        }
      case other =>
        throw new UnsupportedOperationException(s"Cannot populate Arrow vector for Spark type $other")
    }
  }

  private def populateVectorFromInternalRows(
      vector: FieldVector,
      dataType: DataType,
      rows: Array[InternalRow],
      colIdx: Int
  ): Unit = {
    val len = rows.length
    dataType match {
      case LongType =>
        val v = vector.asInstanceOf[BigIntVector]
        for (i <- 0 until len) {
          if (rows(i).isNullAt(colIdx)) v.setNull(i)
          else v.setSafe(i, rows(i).getLong(colIdx))
        }
      case IntegerType =>
        val v = vector.asInstanceOf[IntVector]
        for (i <- 0 until len) {
          if (rows(i).isNullAt(colIdx)) v.setNull(i)
          else v.setSafe(i, rows(i).getInt(colIdx))
        }
      case ShortType =>
        val v = vector.asInstanceOf[SmallIntVector]
        for (i <- 0 until len) {
          if (rows(i).isNullAt(colIdx)) v.setNull(i)
          else v.setSafe(i, rows(i).getShort(colIdx))
        }
      case ByteType =>
        val v = vector.asInstanceOf[TinyIntVector]
        for (i <- 0 until len) {
          if (rows(i).isNullAt(colIdx)) v.setNull(i)
          else v.setSafe(i, rows(i).getByte(colIdx))
        }
      case DoubleType =>
        val v = vector.asInstanceOf[Float8Vector]
        for (i <- 0 until len) {
          if (rows(i).isNullAt(colIdx)) v.setNull(i)
          else v.setSafe(i, rows(i).getDouble(colIdx))
        }
      case FloatType =>
        val v = vector.asInstanceOf[Float4Vector]
        for (i <- 0 until len) {
          if (rows(i).isNullAt(colIdx)) v.setNull(i)
          else v.setSafe(i, rows(i).getFloat(colIdx))
        }
      case StringType =>
        val v = vector.asInstanceOf[VarCharVector]
        for (i <- 0 until len) {
          if (rows(i).isNullAt(colIdx)) v.setNull(i)
          else {
            val bytes = rows(i).getUTF8String(colIdx).getBytes
            v.setSafe(i, bytes, 0, bytes.length)
          }
        }
      case BooleanType =>
        val v = vector.asInstanceOf[BitVector]
        for (i <- 0 until len) {
          if (rows(i).isNullAt(colIdx)) v.setNull(i)
          else v.setSafe(i, if (rows(i).getBoolean(colIdx)) 1 else 0)
        }
      case other =>
        throw new UnsupportedOperationException(s"Cannot populate Arrow vector for InternalRow type $other")
    }
  }
}
