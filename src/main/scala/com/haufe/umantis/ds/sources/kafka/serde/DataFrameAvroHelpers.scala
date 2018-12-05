package com.haufe.umantis.ds.sources.kafka.serde

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.avro._
import org.apache.spark.sql.functions._

import scala.collection.JavaConverters._
import scala.collection.mutable


trait DataFrameAvroHelpers {

  def getSchema(
                 schemaRegistry: CachedSchemaRegistryClient,
                 subject: String,
                 version: String = "latest"
               )
  : String = {

    val schemaId =
      if (version == "latest") {
        schemaRegistry.getLatestSchemaMetadata(subject).getId
      } else {
        schemaRegistry.getSchemaMetadata(subject, version.toInt).getId
      }

    schemaRegistry.getByID(schemaId).toString
  }

  implicit class DataFrameWithAvroHelpers(df: DataFrame) extends Serializable {

    def from_confluent_avro(
                             inputColumn: String,
                             outputColumn: String,
                             schemaRegistryURLs: String,
                             subject: String,
                             version: String = "latest"
                           )
    : DataFrame = {
      val schemaRegistry = SchemaRegistryHelper.getSchemaRegistry(schemaRegistryURLs)

      val schema = getSchema(schemaRegistry, subject, version)

      df.withColumn(outputColumn, from_avro(col(inputColumn), schema))
    }

    def to_confluent_avro(
                           schemaRegistryURLs: String,
                           subject: String,
                           outputColumn: String,
                           inputColumns: Option[Array[String]] = None,
                           recordName: String = "topLevelRecord",
                           nameSpace: String = ""
                         )
    : DataFrame = {

      val inputColsNames = inputColumns match {
          case Some(cols) => cols
          case _ => df.columns
        }

      val inputCols = inputColsNames.map(col)

      val schemaRegistry = SchemaRegistryHelper.getSchemaRegistry(schemaRegistryURLs)

      val schema = SchemaConverters.toAvroType(
        df.select(inputCols: _*).schema,
        recordName = recordName,
        nameSpace = nameSpace
      )
      schemaRegistry.register(subject, schema)

      val dfWithAvroCol = df.withColumn(outputColumn, to_avro(struct(inputCols: _*)))

      // dropping all inputColumns
      dfWithAvroCol.drop(inputColsNames.filter(_ != outputColumn): _*)
    }
  }

}

object SchemaRegistryHelper {
  private val cacheCapacity = 256

  val avroRegistryClients: mutable.Map[String, CachedSchemaRegistryClient] =
    mutable.Map[String, CachedSchemaRegistryClient]()

  def getSchemaRegistry(schemaRegistryURLs: String): CachedSchemaRegistryClient =
    avroRegistryClients
      .getOrElseUpdate(
        schemaRegistryURLs,
        {
          // Multiple URLs for HA mode.
          val urls = schemaRegistryURLs.split(",").toList.asJava
          new CachedSchemaRegistryClient(urls, cacheCapacity)
        }
      )
}