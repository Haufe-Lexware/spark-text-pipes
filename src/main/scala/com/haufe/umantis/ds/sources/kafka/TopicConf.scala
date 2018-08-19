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

import com.haufe.umantis.ds.spark.SparkIO
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf


/**
  * Kafka server configuration
  *
  * @param brokers Address and port of the kafka broker
  * @param schemaRegistryURL The URL of the schema registry
  */
case class KafkaConf(
                      brokers: String,
                      schemaRegistryURL: Option[String]
                    )

/**
  * Generic Topic description
  *
  */
abstract class TopicName() {
  def topic: String
  def payloadField: String
  def uniqueEntityKey: Option[UserDefinedFunction]
}

/**
  * Topic with user provided name
  *
  * @param topic The topic name
  * @param payloadField The name of the payloadField in Kafka
  * @param uniqueEntityKey The UDF to render the key unique for each entity
  */
class GenericTopicName(
                        val topic: String,
                        val payloadField: String,
                        val uniqueEntityKey: Option[UserDefinedFunction]
                      )
  extends TopicName


object GenericUniqueIdentityKeys {
  /**
    * We combine the key as one field.
    */
  case class EntityAndTimestamp(unique_entity: String, timestamp: Long)

  /**
    * We remove the timestamp, if present.
    *
    * Key structure:
    * [service_name].[tenant_id].[identity_id].[entity_id].[timestamp]
    *
    * A unique entity is defined by everything except the timestamp.
    *
    */
  val EntityAndTimestampFromStringKey: String => EntityAndTimestamp = {
    key: String =>
      val lastDot = key.lastIndexOf('.')
      EntityAndTimestamp(
        key.slice(0, lastDot),
        key.slice(lastDot + 1, key.length).toLong
      )
  }

  val EntityAndTimestampFromAvroKey: Row => EntityAndTimestamp = {
    key: Row => {
      val keyString = key.mkString(".")

      EntityAndTimestampFromStringKey(keyString)
    }
  }

  val EntityAndTimestampFromStringKeyUDF: UserDefinedFunction = udf{EntityAndTimestampFromStringKey}
  val EntityAndTimestampFromAvroKeyUDF: UserDefinedFunction = udf{EntityAndTimestampFromAvroKey}
}

class NoOpUniqueIdentityKeys {
  val noOp: Unit => Unit = {Unit => Unit}

  val NoOpUDF: UserDefinedFunction = udf{noOp}
}

/**
  * Configuration for the Parquet Sink of a [[TopicSourceParquetSink]]
  *
  * @param transformationFunction A transformation function to apply to the Streaming DataFrame
  * @param refreshTime The refresh time (in seconds) after which the parquet file is re-read.
  * @param numPartitions Number of partitions to use for the parquet sink
  * @param filenamePrefix An optional filename to prefix to the parquet file sink
  */
case class ParquetSinkConf(
                            transformationFunction: DataFrame => DataFrame,
                            refreshTime: Int,
                            numPartitions: Int,
                            filenamePrefix: Option[String] = None
                          )

/**
  * A configuration for a [[TopicSourceParquetSink]]
  *
  * @param kafkaConf The kafka configuration
  * @param kafkaTopic The kafka topic configuration
  * @param parquetSink The sink configuration
  */
case class TopicConf(
                      kafkaConf: KafkaConf,
                      kafkaTopic: TopicName,
                      parquetSink: ParquetSinkConf,
                      kafkaTopicSink: Option[TopicName] = None
                    )
  extends SparkIO
{
  def subjectKeyName: String = s"${kafkaTopic.topic}-key"
  def subjectValueName: String = s"${kafkaTopic.topic}-value"

  def prefix: String = parquetSink.filenamePrefix match {case None => ""; case Some(s) => s"${s}_"}
  def fileNameLeaf: String = s"$kafkaParquetsDir$prefix${kafkaTopic.topic}"
  def fileNameLeafCheckpoint: String = s"${fileNameLeaf}_Checkpoint"
  def filePath: String = filePath(fileNameLeaf)
  def filePathCheckpoint: String = filePath(fileNameLeafCheckpoint)
}