package org.apache.spark.sql.gdb

import org.apache.arrow.flight.{FlightClient, Location}
import org.apache.arrow.memory.RootAllocator
import org.apache.spark.internal.Logging

import java.util.concurrent.ConcurrentHashMap

/**
 * Reusable gRPC FlightClient connection pool for Spark Executors.
 */
object GdbFlightClientPool extends Logging {
  private val allocator = new RootAllocator(Long.MaxValue)
  private val clientCache = new ConcurrentHashMap[String, FlightClient]()

  def getAllocator: RootAllocator = allocator

  /**
   * Retrieves or establishes a FlightClient connection to the target host and client_flight_port.
   */
  def getClient(host: String, port: Int): FlightClient = {
    val key = s"$host:$port"
    clientCache.computeIfAbsent(key, _ => {
      val location = Location.forGrpcInsecure(host, port)
      logInfo(s"Connecting new FlightClient to $location")
      FlightClient.builder(allocator, location).build()
    })
  }

  /**
   * Closes all cached connections and frees allocator resources.
   */
  def closeAll(): Unit = {
    val it = clientCache.values().iterator()
    while (it.hasNext) {
      try {
        it.next().close()
      } catch {
        case ex: Exception =>
          logWarning(s"Error closing FlightClient: ${ex.getMessage}")
      }
    }
    clientCache.clear()
  }
}
