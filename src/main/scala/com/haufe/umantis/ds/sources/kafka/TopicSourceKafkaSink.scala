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

import scala.util.Try

class TopicSourceKafkaSink(
                            conf: TopicConf
                          )
  extends TopicSource(conf)
{
  private var startingOffset: String = "latest"

  /**
    * Start the processing of this topic
    */
  def start(): this.type = {

    sink match {
      case Some(s) => s.stop()
      case _ => ;
    }

    val outputTopicName: String = conf.kafkaTopicSink match {
      case Some(tn) => tn.topic
      case _ => s"${conf.kafkaTopic.topic}.output"
    }

    val options = {
      conf.kafkaConf.schemaRegistryURL match {
        case Some(url) => Map("schema.registry.url" -> url)
        case _ => Map()
      }
    } ++ Map(
      "topic"-> outputTopicName,
      "kafka.bootstrap.servers" -> conf.kafkaConf.brokers
    )

    sink =
      try {
        val s = getSource("earliest")//startingOffset)
          .writeStream
          .outputMode("append")
          .option("checkpointLocation", conf.filePathCheckpoint)
          .format("kafka")
          .options(options)
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
    * Returns a Map of the current dataframe size. It returns "fail" if not available.
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
}