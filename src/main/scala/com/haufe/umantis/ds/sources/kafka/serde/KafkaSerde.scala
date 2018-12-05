/**
  * This program is free software: you can redistribute it and/or modify
  * it under the terms of the GNU General Public License as published by
  * the Free Software Foundation, either version 3 of the License, or
  * (at your option) any later version.
  *
  * This program is distributed in the hope that it will be useful,
  * but WITHOUT ANY WARRANTY; without even the implied warranty of
  * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  * GNU General Public License for more details.
  *
  * You should have received a copy of the GNU General Public License
  * along with this program.  If not, see <https://www.gnu.org/licenses/>.
  */

package com.haufe.umantis.ds.sources.kafka.serde

import com.haufe.umantis.ds.sources.kafka.{KafkaTopicDataFrameHelper, TopicConf}
import com.haufe.umantis.ds.spark.{DataFrameHelpers, SparkIO}
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.streaming.Trigger

import scala.util.Try


trait KafkaAvroSerde extends DataFrameAvroHelpers {
  def conf: TopicConf

  private lazy val schemaRegistry: Option[CachedSchemaRegistryClient] =
    conf.kafkaConf.schemaRegistryURL match {
      case Some(url) => Some(SchemaRegistryHelper.getSchemaRegistry(url))
      case _ => None
    }

  lazy val keySchema: Option[String] =
    schemaRegistry match {
      case Some(sr) =>
        Try(
          Some(getSchema(sr, conf.subjectKeyName, "latest"))
        ).getOrElse(None)
      case _ => None
    }

  lazy val valueSchema: Option[String] =
    schemaRegistry match {
      case Some(sr) =>
        Try(
          Some(getSchema(sr, conf.subjectValueName, "latest"))
        ).getOrElse(None)
      case _ => None
    }

  def isKeyAvro: Boolean = {
    keySchema match {
      case Some(_) => true
      case _ => false
    }
  }

  def isValueAvro: Boolean = {
    valueSchema match {
      case Some(_) => true
      case _ => false
    }
  }
}

trait KafkaJsonSerde extends SparkIO {
  def conf: TopicConf

  def deserializeJsonInKafkaStream(
                                    df: DataFrame,
                                    inputColumn: String,
                                    outputColumn: String
                                  )
  : DataFrame = {
    // Payload is serialized using JSON, Spark cannot infer the schema.
    // To infer the schema, we read a couple of messages, write them to a JSON file,
    // infer the schema from JSON, and finally define the stream of the normal query

    val tmpFilename = s"$kafkaParquetsDir/tmp/${conf.kafkaTopic.topic}"
    val tmpFilenameCheckpoint = s"$kafkaParquetsDir/tmp/${conf.kafkaTopic.topic}_Checkpoint"
    val tmpFilenameJson = s"$kafkaParquetsDir/tmp/${conf.kafkaTopic.topic}_Json"

    // let's ensure these temp filename do not exist
    deleteFile(tmpFilename)
    deleteFile(tmpFilenameCheckpoint)
    deleteFile(tmpFilenameJson)

    // we use Structured Streaming, as to use batches we should know the Kafka offsets
    // which are not easy to get from Spark
    df
      .select(inputColumn)
      .writeStream
      .outputMode("append")
      .option("checkpointLocation", tmpFilenameCheckpoint)
      .format("parquet")
      .trigger(Trigger.Once)
      .start(tmpFilename)
      .awaitTermination()

    // Reading the parquet file and writing it again to JSON
    currentSparkSession
      .read
      .parquet(tmpFilename)
      .write
      .mode("overwrite")
      .format("text")
      .save(tmpFilenameJson)

    // Finally inferring the schema
    val jsonSchema = currentSparkSession.read.json(tmpFilenameJson).schema

    df
      .withColumn(outputColumn, from_json(col(inputColumn), jsonSchema))
  }
}


trait KafkaSerde extends KafkaAvroSerde with KafkaJsonSerde {

  implicit class SourceHelpers(df: DataFrame)
    extends DataFrameHelpers with KafkaTopicDataFrameHelper {

    def deserialize(keyColumn: String, valueColumn: String): DataFrame = {

      val dfWithDeserializedKey = keySchema match {
        case Some(_) =>
          df.from_confluent_avro(
            keyColumn,
            keyColumn,
            conf.kafkaConf.schemaRegistryURL.get,
            conf.subjectKeyName,
            "latest"
          )
        case _ =>
          deserializeJsonInKafkaStream(
            df.withColumn(keyColumn, col(keyColumn).cast("string")),
            keyColumn,
            keyColumn
          )
      }

      valueSchema match {
        case Some(_) =>
          dfWithDeserializedKey.from_confluent_avro(
            valueColumn,
            valueColumn,
            conf.kafkaConf.schemaRegistryURL.get,
            conf.subjectValueName,
            "latest"
          )
        case _ =>
          deserializeJsonInKafkaStream(
            dfWithDeserializedKey.withColumn(valueColumn, col(valueColumn).cast("string")),
            valueColumn,
            valueColumn
          )
      }
    }
  }
}
