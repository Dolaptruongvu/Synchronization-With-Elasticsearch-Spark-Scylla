import org.apache.spark.sql.{DataFrame, SparkSession}
import com.datastax.spark.connector._
import org.apache.spark.sql.functions._
import org.apache.log4j.{Level, Logger}
import scala.sys.process._
import java.sql.Timestamp
import java.util.UUID
import java.net.{HttpURLConnection, URL}
import org.elasticsearch.spark.sql._
import java.io._
import java.nio.file.{Files, Paths}
import scala.io.Source
import org.apache.spark.sql.types._

object Main {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
    var metricCount = 0 // Variable metricCount

    // Create SparkSession with Scylla configuration
    val spark = SparkSession.builder()
      .appName("FetchCDCDataAndSync")
      .master("local[*]") // Run locally
      .config("spark.cassandra.connection.host", "127.0.0.1") // Connect to Scylla
      .config("spark.cassandra.connection.port", "9042")
      .config("es.nodes", "localhost") // Connect to Elasticsearch
      .config("es.port", "9200")
      .getOrCreate()

    import spark.implicits._

    // Run cqlsh command to get the list of main tables
    val command = Seq(
      "docker", "exec", "b2c6b051f971",
      "cqlsh", "-e", "USE social_data; DESCRIBE TABLES;"
    )
    val result = command.!!

    // Check the raw result string
    println(s"Raw result from DESCRIBE TABLES: \n$result")

    // Split the result string into the list of main tables
    val tableNames = result.split("\\s+")
      .map(_.trim)
      .filter(_.nonEmpty)
      .toList

    // Print the list of main tables for checking
    println(s"List of main tables after splitting: "
      + s"${tableNames.mkString(", ")}")

    // Loop through each main table and synchronize CDC data
    tableNames.foreach { tableName =>
      val cdcTableName = s"${tableName}_scylla_cdc_log"
      println(s"Reading data from CDC table: $cdcTableName")

      var cdcDF: DataFrame = null

      try {
        // Read CDC data
        cdcDF = spark.read
          .format("org.apache.spark.sql.cassandra")
          .options(Map(
            "table" -> cdcTableName,
            "keyspace" -> "social_data"
          ))
          .load()

        // Check schema
        cdcDF.printSchema()

      } catch {
        case e: Exception =>
          println(s"Failed to read data from CDC table: "
            + s"$cdcTableName. Error: ${e.getMessage}")
          e.printStackTrace()
          // Continue with the next table
          return
      }

      // Continue processing if no error
      println("cdcDf with cdc$time")
      cdcDF.select("cdc$time").show()

      if (cdcDF.columns.contains("profile_uuid")) {
        println("profile_uuid found, proceeding with syncing.")

        // Read data from the entity table
        val entityDF = spark.read
          .format("org.apache.spark.sql.cassandra")
          .options(Map(
            "table" -> tableName,
            "keyspace" -> "social_data"
          ))
          .load()

        // Call the sync function and update metricCount
        metricCount += syncTableToElasticsearch(
          spark, cdcDF, entityDF,
          tableName, "social_data"
        )

      } else {
        println(s"Column 'profile_uuid' not found in "
          + s"$cdcTableName. Skipping this CDC table.")
      }
    }

    // After all loops are done, write metricCount to Elasticsearch
    val idCount = UUID.randomUUID().toString
    val lastUpdated = new Timestamp(System.currentTimeMillis())
    val metricCountDf = spark.createDataFrame(Seq(
      (idCount, metricCount, lastUpdated)
    )).toDF("id_count", "count", "lastupdated")

    // Save metricCount DataFrame to Elasticsearch
    metricCountDf.saveToEs("count_metrics", Map(
      "es.write.operation" -> "upsert",
      "es.mapping.id" -> "id_count"
    ))

    println(s"Total metrics saved with id_count: $idCount, count: "
      + s"$metricCount, lastupdated: $lastUpdated")

    // Stop SparkSession
    spark.stop()
  }

  // Function to sync DataFrame from Scylla/Cassandra to Elasticsearch
  def syncTableToElasticsearch(
      spark: SparkSession,
      cdcDfInput: DataFrame,
      entityDf: DataFrame,
      indexName: String,
      keyspace: String
  ): Int = {
    import spark.implicits._
    import java.io._
    import java.nio.file.{Files, Paths}
    import scala.io.Source
    import org.apache.spark.sql.functions._

    // Path to the file that stores last_processed_time
    val filePath = s"last_processed_time_$indexName.txt"

    // Function to read last_processed_time from file
    def readLastProcessedTime(filePath: String): Option[String] = {
      if (Files.exists(Paths.get(filePath))) {
        val lines = Source.fromFile(filePath).getLines()
        if (lines.hasNext) Some(lines.next())
        else None
      } else None
    }

    // Function to write last_processed_time to file
    def writeLastProcessedTime(
        filePath: String,
        lastProcessedTime: String
    ): Unit = {
      val file = new File(filePath)
      val bw = new BufferedWriter(new FileWriter(file))
      bw.write(lastProcessedTime)
      bw.close()
    }

    // Function to convert UUID to Timestamp
    def uuidToTimestamp(uuidStr: String): Timestamp = {
      if (uuidStr != null) {
        val uuid = UUID.fromString(uuidStr)
        // Convert timestamp from 100-ns intervals since 1582-10-15
        val timestamp = (uuid.timestamp() - 0x01b21dd213814000L) / 10000L
        new Timestamp(timestamp)
      } else null
    }

    // Register UDF
    val uuidToTimestampUDF = udf(uuidToTimestamp _)

    try {
      // Read last_processed_time from file
      val lastProcessedTime = readLastProcessedTime(filePath)

      println(s"Last processed time for table $indexName: "
        + s"$lastProcessedTime")

      // Declare variable cdcDf for modification
      var cdcDf = cdcDfInput

      // Add column cdc_timestamp using UDF
      cdcDf = cdcDf.withColumn("cdc_timestamp",
        uuidToTimestampUDF(col("cdc$time")))

      // Filter cdcDf based on lastProcessedTime if available
      if (lastProcessedTime.isDefined) {
        val lastProcessedTimestamp = Timestamp.valueOf(
          lastProcessedTime.get)
        cdcDf = cdcDf.filter(
          col("cdc_timestamp") > lastProcessedTimestamp)
      }

      // Read CDC DataFrame
      println("CDC DataFrame after filtering: ")
      cdcDf.orderBy(desc("cdc_timestamp")).show()

      // Extract new and deleted data based on cdc$operation
      val sortCdc = cdcDf.orderBy(desc("cdc_timestamp"))
      val distinctCdc = sortCdc.dropDuplicates("profile_uuid")

      // Filter for upsert operations (operation 1 or 2)
      val upsertCdc = distinctCdc
        .filter("`cdc$operation` >= 1 AND `cdc$operation` <= 2")
        .select("cdc$operation", "profile_uuid")
        .dropDuplicates("profile_uuid")

      // Filter for delete operations (operation 4)
      val deleteCdc = distinctCdc
        .filter("`cdc$operation`=4")
        .select("cdc$operation", "profile_uuid")
        .dropDuplicates("profile_uuid")

      println("Entity DataFrame: ")
      entityDf.show()

      println("Upsert Cdc DataFrame: ")
      upsertCdc.show()

      println("Delete Cdc DataFrame: ")
      deleteCdc.show()

      // Save upsert data to Elasticsearch
      upsertCdc.createOrReplaceTempView("upsert_entities")
      entityDf.createOrReplaceTempView("entities")
      val filteredEntityDf = spark.sql(
        s"""
        SELECT * FROM entities
        WHERE profile_uuid IN (
          SELECT profile_uuid FROM upsert_entities
        )
        """)

      // Display resulting DataFrame
      println(s"Filtered Entities based on upsertCdc profile_uuid:")
      filteredEntityDf.show()

      // Save data to Elasticsearch with upsert mechanism
      filteredEntityDf.saveToEs(indexName, Map(
        "es.write.operation" -> "upsert", // Use upsert
        "es.mapping.id" -> "profile_uuid" // Use profile_uuid as id
      ))

      // Handle delete data
      val deleteCdcRDD = deleteCdc.select("profile_uuid").rdd
      deleteCdcRDD.foreachPartition { partition =>
        partition.foreach { row =>
          val profile_uuid = row.getAs[String]("profile_uuid")
          val urlString = s"http://localhost:9200/$indexName/" +
            s"_doc/$profile_uuid"
          sendDeleteRequest(urlString)
        }
      }

      def sendDeleteRequest(urlString: String): Unit = {
        val urlObj = new URL(urlString)
        val connection = urlObj.openConnection()
          .asInstanceOf[HttpURLConnection]
        connection.setRequestMethod("DELETE")
        val responseCode = connection.getResponseCode
        if (responseCode == 200 || responseCode == 204) {
          println(s"Successfully deleted document with URL: $urlString")
        } else {
          println(s"Failed to delete document with URL: $urlString, "
            + s"Response Code: $responseCode")
        }
        connection.disconnect()
      }

      // Calculate updatedMetricCount
      val updatedMetricCount = filteredEntityDf.count.toInt
        + deleteCdc.count.toInt

      // Update last_processed_time into the file
      val latestCdcTimeRow = sortCdc.select("cdc_timestamp")
        .as[Timestamp].take(1).headOption

      if (latestCdcTimeRow.isDefined) {
        val latestCdcTime = latestCdcTimeRow.get.toString
        writeLastProcessedTime(filePath, latestCdcTime)
        println(s"Updated last processed time for table $indexName: "
          + s"$latestCdcTime")
      } else {
        println(s"No data processed for table $indexName, "
          + s"last processed time remains unchanged.")
      }

      // Return updatedMetricCount
      updatedMetricCount

    } catch {
      case e: Exception =>
        println(s"An error occurred while syncing table $indexName: "
          + s"${e.getMessage}")
        e.printStackTrace()
        // Return 0 if an error occurs
        0
    }
  }
}
