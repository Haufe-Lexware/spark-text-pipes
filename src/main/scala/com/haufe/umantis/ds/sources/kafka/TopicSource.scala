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

import com.haufe.umantis.ds.sources.kafka.serde.{DataFrameAvroHelpers, KafkaSerde}
import com.haufe.umantis.ds.spark.{DataFrameHelpers, SparkIO}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.DataStreamReader


abstract class TopicSource(val conf: TopicConf)
  extends Source
    with SparkIO
    with DataFrameHelpers
    with DataFrameAvroHelpers
    with KafkaSerde {
  println(s"Creating KafkaDataSource for topic: " +
    s"${conf.kafkaTopic.topic} saved at ${conf.filePath} and ${conf.filePathCheckpoint}")

  def preProcessDf(df: DataFrame): DataFrame = {
    df
  }

  // NOTE: do no use .repartition(numPartitions) here!
  // "latest"
  // "earliest"
  def getSource(startingOffset: String = "latest"): DataFrame = {

    implicit class SchemaRegistryHelpersReader(dsr: DataStreamReader) {

      def maybeSetSchemaRegistryURL: DataStreamReader = {
        conf.kafkaConf.schemaRegistryURL match {
          case Some(url) => dsr.option("schema.registry.url", url)
          case _ => dsr
        }
      }
    }

    val deserializedDf = currentSparkSession
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

    val preprocessedDf = preProcessDf(deserializedDf)
      .expand("value")

    conf.sinkConf.transformationFunction(preprocessedDf)
  }
}
