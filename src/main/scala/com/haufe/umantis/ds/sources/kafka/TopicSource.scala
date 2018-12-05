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

package com.haufe.umantis.ds.sources.kafka

import com.haufe.umantis.ds.sources.kafka.serde.{DataFrameAvroHelpers, KafkaDeserializer}
import com.haufe.umantis.ds.spark.{DataFrameHelpers, SparkIO}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{DataStreamReader, StreamingQuery, Trigger}


abstract class TopicSource(
                            val conf: TopicConf
                          )
extends Source with SparkIO with DataFrameHelpers with DataFrameAvroHelpers
{
  println(s"Creating KafkaDataSource for topic: " +
    s"${conf.kafkaTopic.topic} saved at ${conf.filePath} and ${conf.filePathCheckpoint}")

  val kafkaSerializer = new KafkaDeserializer(conf)

  def preProcessDf(df: DataFrame): DataFrame = {
    df
  }

  // NOTE: do no use .repartition(numPartitions) here!
  // "latest"
  // "earliest"
  def getSource(startingOffset: String = "latest"): DataFrame = {
    import kafkaSerializer._
    import currentSparkSession.implicits._

    implicit class SchemaRegistryHelpersReader(dsr: DataStreamReader) {

      def maybeSetSchemaRegistryURL: DataStreamReader = {
        conf.kafkaConf.schemaRegistryURL match {
          case Some(url) => dsr.option("schema.registry.url", url)
          case _ => dsr
        }
      }
    }

    val rawDf = currentSparkSession
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", conf.kafkaConf.brokers)
      .option("failOnDataLoss", "false") // disabled because Kafka can age out data
      .maybeSetSchemaRegistryURL
      .option("startingOffsets", startingOffset)
      .option("subscribe", conf.kafkaTopic.topic)
      .load()
      .alsoPrintSchema(Some("TopicSource raw"))
      .deserialize("key", "value")
      .alsoPrintSchema(Some("TopicSource after deserialization"))

    val deserializedDf: DataFrame = {
      if (kafkaSerializer.isValueAvro) {
        // Payload serialized using Avro, Spark will infer the schema
        rawDf

      } else {
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
        rawDf
          .select("value")
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
        val valueJsonSchema = currentSparkSession.read.json(tmpFilenameJson).schema

        rawDf
          .withColumn("value", from_json($"value", valueJsonSchema))
      }
    }

    val preprocessedDf = preProcessDf(deserializedDf)
      .expand("value")

    conf.sinkConf.transformationFunction(preprocessedDf)
  }
}
