package com.haufe.umantis.ds.sources.kafka.serde

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import org.apache.avro.Schema
import org.apache.spark.sql.avro._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame}

import scala.collection.mutable
import scala.util.Try


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

      from_confluent_avro(inputColumn, outputColumn, schema)
    }

    def from_confluent_avro(
                             inputColumn: String,
                             outputColumn: String,
                             schema: String
                           )
    : DataFrame = {

      df.withColumn(
        outputColumn,
        new Column(ConfluentAvroDataToCatalyst(col(inputColumn).expr, schema))
      )
    }

    def to_confluent_avro(
                           schemaRegistryURLs: String,
                           subject: String,
                           outputColumn: String,
                           inputColumns: Option[Array[String]] = None,
                           registerSchema: Boolean = true,
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

      val dfAvroSchema = SchemaConverters.toAvroType(
        df.select(inputCols: _*).schema,
        nullable = false,
        recordName = recordName,
        nameSpace = nameSpace
      )

      val schemaId: Int =
        Try(Some(schemaRegistry.getLatestSchemaMetadata(subject))).getOrElse(None) match {

          case Some(metadata) =>
            val registeredSchema = new Schema.Parser().parse(metadata.getSchema)

            if (registeredSchema != dfAvroSchema) {
              throw new IncompatibleSchemaException(
                s"The schema of the dataframe in input and the one registered in the" +
                  s" schema registry $schemaRegistryURLs with id ${metadata.getId} differ:\n" +
                  s"registered schema: $registeredSchema\n" +
                  s"dataframe schema : $dfAvroSchema"
              )
            }

            metadata.getId

          case _ =>
            if (registerSchema)
              schemaRegistry.register(subject, dfAvroSchema)
            else
              throw new IllegalArgumentException(
                s"Subject $subject does not exist in $schemaRegistryURLs and registerSchema=false"
              )
        }

      val dfWithAvroCol = df.withColumn(
        outputColumn,
        new Column(CatalystDataToConfluentAvro(struct(inputCols: _*).expr, schemaId))
      )

      // dropping all inputColumns
      dfWithAvroCol.drop(inputColsNames.filter(_ != outputColumn): _*)
    }
  }

}

object SchemaRegistryHelper {

  import scala.collection.JavaConverters._

  private val cacheCapacity = 2048

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
