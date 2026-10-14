package org.apache.spark.sql.gdb

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.spark.internal.Logging

import java.io.InputStreamReader
import java.net.{HttpURLConnection, URI, URL}
import java.nio.charset.StandardCharsets

/**
 * Metadata for a single GDB cluster node.
 */
case class GdbNodeInfo(
    nodeId: Long,
    httpUrl: String,
    flightAddress: String,
    flightPort: Int,
    clientFlightAddress: String,
    clientFlightPort: Int,
    host: String
) extends Serializable {
  def flightEndpointUri: String = s"grpc://$host:$clientFlightPort"
}

/**
 * Cluster Topology snapshot discovered by Spark Driver from GDB coordinator.
 */
case class GdbClusterTopology(
    seedUrl: String,
    nodes: Array[GdbNodeInfo],
    replicationFactor: Int,
    partitions: Int
) extends Serializable with Logging {

  require(nodes.nonEmpty, "GDB cluster topology must contain at least 1 node")

  def totalNodes: Int = nodes.length

  /**
   * Primary token routing: calculates target GDB ring node for a numerical key.
   */
  def targetNodeForKey(key: Long): GdbNodeInfo = {
    val positiveKey = key & 0x7ffffffffffffffL
    val idx = (positiveKey % totalNodes).toInt
    nodes(idx)
  }

  /**
   * Primary token routing for string keys (using 64-bit Murmur/hash).
   */
  def targetNodeForStringKey(key: String): GdbNodeInfo = {
    val hash = key.hashCode.toLong
    targetNodeForKey(hash)
  }

  /**
   * Triggers compaction across all ring nodes after bulk load completion.
   */
  def triggerCompaction(timeoutMs: Long = 10000L): Unit = {
    nodes.foreach { node =>
      try {
        val queryUrl = new URL(s"${node.httpUrl.stripSuffix("/")}/query")
        val conn = queryUrl.openConnection().asInstanceOf[HttpURLConnection]
        conn.setRequestMethod("POST")
        conn.setRequestProperty("Content-Type", "application/json")
        conn.setConnectTimeout(timeoutMs.toInt)
        conn.setReadTimeout(timeoutMs.toInt)
        conn.setDoOutput(true)

        val payload = """{"query": "COMPACT;"}"""
        val os = conn.getOutputStream
        try {
          os.write(payload.getBytes(StandardCharsets.UTF_8))
          os.flush()
        } finally {
          os.close()
        }

        val code = conn.getResponseCode
        if (code >= 200 && code < 300) {
          logInfo(s"Triggered compaction on GDB node ${node.nodeId} (${node.httpUrl}) successfully.")
        } else {
          logWarning(s"Compaction request to node ${node.nodeId} returned HTTP $code.")
        }
      } catch {
        case ex: Exception =>
          logWarning(s"Failed to trigger compaction on node ${node.nodeId} (${node.httpUrl}): ${ex.getMessage}")
      }
    }
  }
}

object GdbClusterTopology extends Logging {
  private val mapper = new ObjectMapper()

  def discover(seedUrl: String, timeoutMs: Long = 10000L): GdbClusterTopology = {
    val cleanSeed = seedUrl.stripSuffix("/")
    val clusterEndpoint = s"$cleanSeed/cluster"
    val seedHost = try {
      val parsed = new URI(cleanSeed)
      val h = parsed.getHost
      if (h != null && h.nonEmpty) h else "127.0.0.1"
    } catch {
      case _: Exception => "127.0.0.1"
    }

    logInfo(s"Discovering GDB cluster topology from $clusterEndpoint...")

    try {
      val url = new URL(clusterEndpoint)
      val conn = url.openConnection().asInstanceOf[HttpURLConnection]
      conn.setRequestMethod("GET")
      conn.setRequestProperty("User-Agent", "gdb-spark-connector/0.5.6")
      conn.setConnectTimeout(timeoutMs.toInt)
      conn.setReadTimeout(timeoutMs.toInt)

      val code = conn.getResponseCode
      if (code != 200) {
        throw new RuntimeException(s"GDB /cluster returned HTTP status $code")
      }

      val is = conn.getInputStream
      val root = try {
        mapper.readTree(new InputStreamReader(is, StandardCharsets.UTF_8))
      } finally {
        is.close()
      }

      val rf = if (root.has("replication_factor")) root.get("replication_factor").asInt(1) else 1
      val parts = if (root.has("partitions")) root.get("partitions").asInt(4) else 4

      val nodesBuffer = scala.collection.mutable.ArrayBuffer[GdbNodeInfo]()

      if (root.has("ring_nodes") && root.get("ring_nodes").isArray && root.get("ring_nodes").size() > 0) {
        val ringArray = root.get("ring_nodes")
        for (i <- 0 until ringArray.size()) {
          val nodeJson = ringArray.get(i)
          val nodeId = nodeJson.path("node_id").asLong(i + 1)
          val httpUrl = nodeJson.path("http_url").asText(s"http://$seedHost:8847")
          val flightAddr = nodeJson.path("flight_address").asText("127.0.0.1")
          val flightPort = nodeJson.path("flight_port").asInt(8848)
          val clientFlightAddr = nodeJson.path("client_flight_address").asText(flightAddr)
          val clientFlightPort = nodeJson.path("client_flight_port").asInt(8860)

          val isLoopback = clientFlightAddr == "127.0.0.1" || clientFlightAddr == "localhost" || clientFlightAddr == "0.0.0.0" || clientFlightAddr.isEmpty
          val effectiveHost = if (isLoopback && seedHost != "127.0.0.1" && seedHost != "localhost" && seedHost != "0.0.0.0") {
            seedHost
          } else if (clientFlightAddr.nonEmpty && !isLoopback) {
            clientFlightAddr
          } else {
            seedHost
          }

          nodesBuffer += GdbNodeInfo(
            nodeId = nodeId,
            httpUrl = httpUrl,
            flightAddress = flightAddr,
            flightPort = flightPort,
            clientFlightAddress = clientFlightAddr,
            clientFlightPort = clientFlightPort,
            host = effectiveHost
          )
        }
      } else {
        // Single-node fallback from root json
        val nodeId = root.path("node_id").asLong(1)
        val flightAddr = root.path("flight_address").asText(seedHost)
        val flightPort = root.path("flight_port").asInt(8848)
        val clientFlightAddr = root.path("client_flight_address").asText(flightAddr)
        val clientFlightPort = root.path("client_flight_port").asInt(8860)

        nodesBuffer += GdbNodeInfo(
          nodeId = nodeId,
          httpUrl = cleanSeed,
          flightAddress = flightAddr,
          flightPort = flightPort,
          clientFlightAddress = clientFlightAddr,
          clientFlightPort = clientFlightPort,
          host = seedHost
        )
      }

      val sortedNodes = nodesBuffer.sortBy(_.nodeId).toArray
      logInfo(s"Discovered ${sortedNodes.length} GDB ring nodes. Replication factor: $rf, client flight ports: ${sortedNodes.map(_.clientFlightPort).mkString(",")}")

      GdbClusterTopology(
        seedUrl = cleanSeed,
        nodes = sortedNodes,
        replicationFactor = rf,
        partitions = parts
      )
    } catch {
      case ex: Exception =>
        logWarning(s"Could not reach GDB /cluster ($ex). Falling back to single-node configuration at $seedHost:8860.")
        val fallbackNode = GdbNodeInfo(
          nodeId = 1,
          httpUrl = cleanSeed,
          flightAddress = seedHost,
          flightPort = 8848,
          clientFlightAddress = seedHost,
          clientFlightPort = 8860,
          host = seedHost
        )
        GdbClusterTopology(
          seedUrl = cleanSeed,
          nodes = Array(fallbackNode),
          replicationFactor = 1,
          partitions = 1
        )
    }
  }
}
