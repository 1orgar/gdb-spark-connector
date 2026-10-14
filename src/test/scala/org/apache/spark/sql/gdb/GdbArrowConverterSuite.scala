package org.apache.spark.sql.gdb

import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.spark.sql.types._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class GdbArrowConverterSuite extends AnyFunSuite with Matchers {

  test("Translates Spark StructType to Arrow Schema with renamed key columns") {
    val sparkSchema = StructType(Seq(
      StructField("user_id", LongType, nullable = false),
      StructField("name", StringType, nullable = true),
      StructField("score", DoubleType, nullable = false),
      StructField("active", BooleanType, nullable = false)
    ))

    val arrowSchema = GdbArrowConverter.toArrowSchema(sparkSchema, Map("user_id" -> "id"))
    arrowSchema.getFields.size() shouldEqual 4

    val idField = arrowSchema.findField("id")
    idField should not be null
    idField.getType shouldBe an[ArrowType.Int]

    val nameField = arrowSchema.findField("name")
    nameField should not be null
    nameField.getType shouldBe an[ArrowType.Utf8]

    val scoreField = arrowSchema.findField("score")
    scoreField should not be null
    scoreField.getType shouldBe an[ArrowType.FloatingPoint]
  }

  test("Translates edge schemas with src and dst columns") {
    val edgeSchema = StructType(Seq(
      StructField("source_node", LongType, nullable = false),
      StructField("target_node", LongType, nullable = false),
      StructField("weight", FloatType, nullable = true)
    ))

    val arrowSchema = GdbArrowConverter.toArrowSchema(
      edgeSchema,
      Map("source_node" -> "src", "target_node" -> "dst")
    )

    arrowSchema.findField("src") should not be null
    arrowSchema.findField("dst") should not be null
    arrowSchema.findField("weight") should not be null
  }
}
