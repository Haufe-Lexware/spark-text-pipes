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

class TopicSourceKafkaSink(
                            conf: TopicConf
                          )
  extends TopicSourceSink(conf)
{
  private var startingOffset: String = "latest"

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

  var outputSchema: StructType = _

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
        val sourceDf = getSource("earliest")//startingOffset)

        outputSchema = sourceDf.schema

        val s = sourceDf
          .select(to_json(struct(sourceDf.columns.map(column):_*)).alias("value"))
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
        case _: RestClientException =>
          // e.printStackTrace()
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
    import currentSparkSession.implicits._

    val kafkaDf = currentSparkSession
      .read
      .format("kafka")

      // Trying to solve kafka this server is not the leader for that topic partition
      // https://stackoverflow.com/questions/47767169/kafka-this-server-is-not-the-leader-for-that-topic-partition
      .option("kafka.retries", 10)

      .options(options)
      .option("startingOffsets", "earliest")
      .option("subscribe", outputTopicName)
      .load()
      .withColumn("value", $"value".cast("string"))
      .withColumn("value", from_json($"value", outputSchema))
      .expand("value")
      .repartition(conf.parquetSink.numPartitions)

    val newDataFrame = postProcessDf(kafkaDf)
      .cache()

    dataFrame = Some(newDataFrame)
    newDataFrame
  }
}
