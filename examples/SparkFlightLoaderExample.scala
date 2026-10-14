package examples

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.gdb.{GdbOptions, GdbSparkLoader}

/**
 * End-to-end example demonstrating bulk graph data loading into GDB via Apache Spark.
 */
object SparkFlightLoaderExample {

  case class User(id: Long, name: String, age: Int, active: Boolean)
  case class Follows(src: Long, dst: Long, rank: Long, weight: Double)

  def main(args: Array[String]): Unit = {
    val gdbUrl = if (args.nonEmpty) args(0) else "http://localhost:8847"

    val spark = SparkSession.builder()
      .appName("GdbSparkFlightLoaderExample")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    println(s"[*] Connecting to GDB cluster at $gdbUrl...")

    // 1. Prepare synthetic vertices dataset (10,000 users)
    val users = (1L to 10000L).map { i =>
      User(id = i, name = s"user_$i", age = (20 + (i % 50)).toInt, active = i % 2 == 0)
    }
    val usersDf = users.toDF()

    println("[*] Ingesting vertices using GdbSparkLoader (Direct Flight Streaming)...")
    val vertexSummary = GdbSparkLoader.loadVertices(
      usersDf,
      GdbOptions(
        url = gdbUrl,
        entityType = "vertex",
        label = Some("User"),
        idCol = "id",
        edgeType = None,
        srcCol = "src",
        dstCol = "dst",
        rankCol = None,
        batchSize = 16384,
        repartitionByRing = true,
        compact = true,
        timeoutMs = 30000L
      )
    )

    println(s"[+] Vertices ingested: ${vertexSummary.totalRows} in ${vertexSummary.elapsedMs}ms (${vertexSummary.rowsPerSecond.formatted("%.2f")} rows/s)")

    // 2. Prepare synthetic edges dataset (50,000 FOLLOWS connections)
    val edges = (1L until 10000L).flatMap { i =>
      Seq(
        Follows(src = i, dst = i + 1, rank = 0L, weight = 1.0),
        Follows(src = i, dst = (i * 7 % 10000L) + 1L, rank = 1L, weight = 0.5)
      )
    }
    val edgesDf = edges.toDF()

    println("[*] Ingesting edges using Spark DataSource V2 (df.write.format(\"gdb\"))...")
    edgesDf.write
      .format("gdb")
      .option("url", gdbUrl)
      .option("type", "edge")
      .option("edgeType", "FOLLOWS")
      .option("srcCol", "src")
      .option("dstCol", "dst")
      .option("rankCol", "rank")
      .option("batchSize", "16384")
      .option("compact", "true")
      .save()

    println("[+] Edges ingested successfully via DataSource V2.")

    spark.stop()
  }
}
