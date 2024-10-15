import org.apache.spark.sql.SparkSession
import com.datastax.spark.connector._
import org.apache.spark.sql.functions._
import org.apache.log4j.{Level, Logger}
import org.elasticsearch.spark.sql._
import java.net.HttpURLConnection
import java.net.URL
import java.util.UUID
import java.sql.Timestamp

object Main {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    // Create SparkSession with configurations for Cassandra and Elasticsearch
    val spark = SparkSession.builder()
      .appName("ScyllaElasticsearchSync")
      .master("local[*]") // Run locally
      .config("spark.cassandra.connection.host", "127.0.0.1") // Configure Cassandra connection
      .config("spark.cassandra.connection.port", "9042")
      .config("es.nodes", "localhost") // Configure Elasticsearch connection
      .config("es.port", "9200")
      .getOrCreate()

    var metricCount: Long = 0

    // Read data from Scylla/Cassandra (CDC log)
    val cdcDf = spark.read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "linkedin_entity_scylla_cdc_log", "keyspace" -> "user_data"))
      .load()

    // Read data from the `linkedin_entity` table in Cassandra
    val entityDf = spark.read 
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "linkedin_entity", "keyspace" -> "user_data"))
      .load()

    println("CDC DataFrame: ")
    cdcDf.orderBy(desc("cdc$time")).show()

    // Extract new and deleted data based on cdc$operation
    val sortCdc = cdcDf.orderBy(desc("cdc$time"))
    val distinctCdc = sortCdc.dropDuplicates("profile_uuid")

    // Filter for upsert operations (operation 1 or 2)
    val upsertCdc = distinctCdc
      .filter("`cdc$operation` >= 1 AND `cdc$operation` <= 2")
      .select("cdc$operation", "email", "fullname", "profile_uuid", "linkedin_username", "linkedin_url", "lastupdated")
      .dropDuplicates("profile_uuid")

    // Filter for delete operations (operation 4)
    val deleteCdc = distinctCdc
      .filter("`cdc$operation`=4")
      .select("cdc$operation", "profile_uuid")
      .dropDuplicates("profile_uuid")

    println("Entity DataFrame: ")
    entityDf.show()

    println("upsert Cdc Dataframe: ")
    upsertCdc.show()

    println("delete Cdc Dataframe: ")
    deleteCdc.show()

    // Save upsert data to Elasticsearch
    upsertCdc.createOrReplaceTempView("upsert_entities")
    entityDf.createOrReplaceTempView("entities")
    val filteredEntityDf = spark.sql(
      """
      SELECT * FROM entities
      WHERE profile_uuid IN (SELECT profile_uuid FROM upsert_entities)
      """)

    // Show the resulting DataFrame
    println("Filtered Entities based on upsertCdc profile_uuid:")
    filteredEntityDf.show()

    // Save data to Elasticsearch with upsert mechanism
    filteredEntityDf.saveToEs("linkedin_entity", Map(
      "es.write.operation" -> "upsert", // Enable upsert mechanism
      "es.mapping.id" -> "profile_uuid" // Use profile_uuid column as document id
    ))

    // Process delete data
    val deleteCdcRDD = deleteCdc.select("profile_uuid").rdd
    deleteCdcRDD.foreachPartition {
      partition => {
        partition.foreach { row =>
          val profile_uuid = row.getAs[String]("profile_uuid")
          val urlString = s"http://localhost:9200/linkedin_entity/_doc/$profile_uuid"
          sendDeleteRequest(urlString)
        }
      }
    }

    def sendDeleteRequest(urlString: String): Unit = {
      val urlObj = new URL(urlString)
      val connection = urlObj.openConnection().asInstanceOf[HttpURLConnection]
      connection.setRequestMethod("DELETE")
      val responseCode = connection.getResponseCode
      if (responseCode == 200 || responseCode == 204) {
        println(s"Successfully deleted document with URL: $urlString")
      } else {
        println(s"Failed to delete document with URL: $urlString, Response Code: $responseCode")
      }
      connection.disconnect()
    }

    // Generate new UUID, current timestamp, and calculate metric count
    val idCount = UUID.randomUUID().toString
    val lastUpdated = new Timestamp(System.currentTimeMillis())
    metricCount = filteredEntityDf.count + deleteCdc.count

    // Create DataFrame with metric count, id_count, and lastupdated
    val metricCountDf = spark.createDataFrame(Seq(
      (idCount, metricCount, lastUpdated) // UUID, metricCount, and lastupdated
    )).toDF("id_count", "count", "lastupdated")

    // Save the metricCount DataFrame to Elasticsearch with id_count as the identifier
    metricCountDf.saveToEs("count_metrics", Map(
      "es.write.operation" -> "upsert", // Enable upsert mechanism
      "es.mapping.id" -> "id_count" // Use 'id_count' as the document id
    ))

    // Stop the SparkSession
    spark.stop()
  }
}
