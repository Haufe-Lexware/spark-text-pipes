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

import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{column, from_json, struct, to_json}
import org.apache.spark.sql.types.StructType

import scala.util.Try

class TopicSourceKafkaSink(conf: TopicConf) extends TopicSourceSink(conf) {
  private var startingOffset: String = "latest"

  private var outputSchema: StructType = _

  val outputTopicName: String = conf.kafkaTopicSink match {
    case Some(tn) => tn.topic
    case _ => s"${conf.kafkaTopic.topic}.output"
  }

  val options: Map[String, String] = {
    conf.kafkaConf.schemaRegistryURL match {
      case Some(url) => Map("schema.registry.url" -> url)
      case _ => Map()
    }
  } ++ Map(
    "kafka.bootstrap.servers" -> conf.kafkaConf.brokers
  )

  /**
    * Start the processing of this topic
    */
  def start(): this.type = {

    sink match {
      case Some(s) => s.stop()
      case _ => ;
    }

    sink =
      try {
        val sourceDf = getSource("earliest") //startingOffset)

        outputSchema = sourceDf.schema

        val keyCol: Option[String] =
          if (sourceDf.columns.contains("key"))
            Some("key")
          else
            None

        val s = sourceDf
          .alsoPrintSchema(Some("TopicSourceKafkaSink before serialization"))
          .serialize(
            None,
            None,
            "value",
            Some(sourceDf.columns), // .filter(_ != "key")
            outputTopicName
          )
          .alsoPrintSchema(Some("TopicSourceKafkaSink after serialization"))
          .writeStream
          .outputMode("append")
          .option("checkpointLocation", conf.filePathCheckpoint)
          .format("kafka")
          .options(options)
          .option("topic", outputTopicName)
          .trigger(conf.kafkaTopic.trigger)
          .start()

        Some(s)

      } catch {
        case e: RestClientException =>
          e.printStackTrace()
          println(s"### TOPIC ${conf.kafkaTopic.topic} NOT FOUND IN KAFKA!!! ###")
          None
      }

    this
  }

  /**
    * Stops the processing of this topic
    */
  def stop(): this.type = {
    sink match {
      case Some(s) =>
        s.stop()
        sink = None
      case _ => ;
    }

    this
  }

  /**
    * Deletes the output parquet file of this topic
    */
  def delete(): this.type = {
    stop()
    // TODO: shall we delete the kafka topic?
    deleteDataFrame(conf.fileNameLeafCheckpoint)
    this
  }

  /**
    * Reset the processing of this topic: stop, delete, start
    */
  def reset(): this.type = {
    stop()
    delete()
    startingOffset = "earliest"
    start()
    startingOffset = "latest"
    this
  }

  /**
    * Returns a Map of the current DataFrame size. It returns "fail" if not available.
    *
    * @return The status
    */
  def status(): Map[String, String] = {
    Map[String, String](
      conf.kafkaTopic.topic -> Try(
        sink match {
          case Some(s) => s.isActive.toString
          case _ => "no sink defined"
        }
      ).getOrElse("fail")
    )
  }

  def postProcessDf(df: DataFrame): DataFrame = {
    df
  }

  def doUpdateDf(): DataFrame = {

    println("TopicSourceKafkaSink read before postprocessdf")
    val kafkaDf = currentSparkSession
      .read
      .format("kafka")

      // Trying to solve Kafka's error "This server is not the leader for that topic-partition"
      // https://stackoverflow.com/questions/47767169/kafka-this-server-is-not-the-leader-for-that-topic-partition
      // .option("kafka.retries", 100)

      .options(options)
      .option("startingOffsets", "earliest")
      .option("subscribe", outputTopicName)
      .load()
      .alsoPrintSchema(Some("TopicSourceKafkaSink just after load"))
      .alsoShow(20, 20)
      .repartition(conf.sinkConf.numPartitions)
      .deserialize(valueColumn = Some("value"), topic = outputTopicName)
      .expand("value")

    val newDataFrame = postProcessDf(kafkaDf)
//      .expand("key")
      .alsoPrintSchema(Some("TopicSourceKafkaSink after post process"))
      .alsoShow(20, 20)
      .cache()
    dataFrame = Some(newDataFrame)
    newDataFrame
  }
}
