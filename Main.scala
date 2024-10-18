import org.apache.spark.sql.{DataFrame, SparkSession}
import com.datastax.spark.connector._
import org.apache.spark.sql.functions._
import org.apache.log4j.{Level, Logger}
import scala.sys.process._
import java.sql.Timestamp
import java.util.UUID
import org.elasticsearch.spark.sql._
import java.io._
import java.nio.file.{Files, Paths}
import scala.io.Source
import org.apache.spark.sql.types._

object Main {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
    var metricCount: Long = 0 // Metric counter, now Long

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

    // Check raw result string
    println(s"Raw result from DESCRIBE TABLES: \n$result")

    // Split the result string into the list of main tables
    val tableNames = result.split("\\s+")
      .map(_.trim)
      .filter(_.nonEmpty)
      .toList

    // Print the list of main tables for checking
    println(s"List of main tables after splitting: ${tableNames.mkString(", ")}")

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
          println(s"Failed to read data from CDC table: $cdcTableName. Error: ${e.getMessage}")
          e.printStackTrace()
          // Continue with the next table
          return
      }

      // Continue processing if no error
      println("cdcDF with cdc$time")
      cdcDF.select("cdc$time").show()

      if (cdcDF.columns.contains("profile_uuid")) {
        println("Found 'profile_uuid', proceeding with sync.")

        // Call the sync function and update metricCount
        metricCount += syncTableToElasticsearch(
          spark, cdcDF, tableName, "social_data"
        )

      } else {
        println(s"Column 'profile_uuid' not found in $cdcTableName. Skipping this CDC table.")
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

    println(s"Total metrics saved with id_count: $idCount, count: $metricCount, lastupdated: $lastUpdated")

    // Stop SparkSession
    spark.stop()
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

  // UDF to extract values from Map
  val extractValuesUDF = udf((map: Map[String, String]) => {
    if (map != null && map.nonEmpty) map.values.toArray
    else Array.empty[String]
  })

  // Function to remove null and empty fields from DataFrame
  def filterValidFields(df: DataFrame): DataFrame = {
    df.schema.fields.foldLeft(df) { (tempDf, field) =>
      field.dataType match {
        case StringType =>
          tempDf.withColumn(field.name, when(col(field.name).isNotNull && !(col(field.name) === ""), col(field.name)).otherwise(null))
        case ArrayType(_, _) =>
          tempDf.withColumn(field.name, when(size(col(field.name)) =!= 0, col(field.name)).otherwise(null))
        case _ =>
          tempDf
      }
    }
  }

  // Function to sync DataFrame from Scylla/Cassandra to Elasticsearch
  def syncTableToElasticsearch(
      spark: SparkSession,
      cdcDfInput: DataFrame,
      indexName: String,
      keyspace: String
  ): Long = {
    import spark.implicits._
    import org.apache.spark.sql.functions._
    import org.apache.spark.sql.types._

    // Path to the file that stores last_processed_time
    val filePath = s"last_processed_time_$indexName.txt"

    // Read last_processed_time from file
    val lastProcessedTime = readLastProcessedTime(filePath)

    println(s"Last processed time for table $indexName: $lastProcessedTime")

    // Register UDF
    val uuidToTimestampUDF = udf(uuidToTimestamp _)

    // Add column cdc_timestamp using UDF
    var cdcDf = cdcDfInput.withColumn("cdc_timestamp", uuidToTimestampUDF(col("cdc$time")))

    // Filter cdcDf based on lastProcessedTime if available
    if (lastProcessedTime.isDefined) {
      val lastProcessedTimestamp = Timestamp.valueOf(lastProcessedTime.get)
      cdcDf = cdcDf.filter(col("cdc_timestamp") > lastProcessedTimestamp)
    }

    // Print the DataFrame after adding column and filtering
    println(s"CDC DataFrame input for table $indexName after filtering by last processed time: ")
    cdcDf.show()

    // Filter out columns that do not start with cdc$, except for cdc$operation and cdc_timestamp
    val columnsToKeep = cdcDf.columns.filter(col => !col.startsWith("cdc$") || col == "cdc$operation" || col == "cdc_timestamp")

    // Select the columns to keep in the DataFrame
    var filteredCdcDf = cdcDf.select(columnsToKeep.head, columnsToKeep.tail: _*)

    // Print the DataFrame after filtering columns
    println("Dataframe after filtering columns")
    filteredCdcDf.show()

    // Apply UDF extractValuesUDF to columns of type Map[String, String]
    filteredCdcDf.schema.fields.foreach { field =>
      field.dataType match {
        case MapType(StringType, StringType, _) =>
          filteredCdcDf = filteredCdcDf.withColumn(field.name, extractValuesUDF(col(field.name)))
        case _ => // Do nothing if not of type Map[String, String]
      }
    }

    // Get the latest timestamp from cdc_timestamp column after filtering
    val latestTimestamp = cdcDf
      .sort(desc("cdc_timestamp"))
      .select("cdc_timestamp")
      .as[Timestamp]
      .take(1)
      .headOption

    latestTimestamp match {
      case Some(ts) => println(s"Latest timestamp: $ts")
      case None => println("No timestamp found")
    }

    // Filter records with operation = 1 or 2
    val upsertCdcDf = filteredCdcDf.filter(col("cdc$operation") === 1 || col("cdc$operation") === 2)

    // Filter records with operation = 4
    val deleteCdcDf = filteredCdcDf.filter(col("cdc$operation") === 4)

    // Find profile_uuid in upsertCdcDf with the largest cdc_timestamp
    val latestUpsertDf = upsertCdcDf
      .groupBy("profile_uuid")
      .agg(max("cdc_timestamp").as("max_timestamp_upsert"))

    // Combine deleteCdcDf with latestUpsertDf and keep records in deleteCdcDf with cdc_timestamp greater than max_timestamp_upsert
    val validDeleteDf = deleteCdcDf
      .join(latestUpsertDf, Seq("profile_uuid"), "left_outer")
      .filter($"cdc_timestamp" > $"max_timestamp_upsert" || $"max_timestamp_upsert".isNull)

    // Remove the 'cdc$operation' and 'cdc_timestamp' columns from upsertCdcDf
    val upsertData = upsertCdcDf.drop("cdc$operation", "cdc_timestamp")

    // Keep only non-null fields
    val upsertDataWithValidValues = filterValidFields(upsertData)

    println("Final upsertData")
    upsertDataWithValidValues.show()

    // Keep only 'profile_uuid' column for deleteCdcDf
    val deleteData = validDeleteDf.select("profile_uuid")
    println("Final deleteData")
    deleteData.show()

    var affectedRowCount: Long = 0 // Initialize affectedRowCount as Long

    try {
      // Perform upsert to Elasticsearch for upsertData
      affectedRowCount += upsertDataWithValidValues.count() // Count the number of rows in upsertData
      upsertDataWithValidValues.saveToEs(indexName, Map(
        "es.write.operation" -> "upsert",
        "es.mapping.id" -> "profile_uuid"   // Use profile_uuid as ID to upsert
      ))
      println("Upsert to Elasticsearch completed successfully.")

      // Perform delete in Elasticsearch for deleteData
      affectedRowCount += deleteData.count() // Count the number of rows in deleteData
      deleteData.saveToEs(indexName, Map(
        "es.write.operation" -> "delete",    // Delete data based on profile_uuid
        "es.mapping.id" -> "profile_uuid"    // Use profile_uuid as ID to delete
      ))
      println("Delete from Elasticsearch completed successfully.")

      // Write 'latestTimestamp' to file after Elasticsearch operations are successful
      latestTimestamp match {
        case Some(ts) =>
          writeLastProcessedTime(filePath, ts.toString)
          println(s"Wrote the last processed time to file: $ts")
        case None =>
          println("No timestamp to write to file")
      }

    } catch {
      case e: Exception =>
        println(s"An error occurred while interacting with Elasticsearch: ${e.getMessage}")
        e.printStackTrace() // Log detailed error information if needed
    }

    // Return the number of affected rows (upsert + delete)
    affectedRowCount
  }

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
}
