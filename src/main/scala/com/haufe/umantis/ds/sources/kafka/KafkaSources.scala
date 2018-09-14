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

import com.haufe.umantis.ds.spark.{DataFrameHelpers, SparkIO}
import org.apache.spark.sql.streaming.Trigger

import scala.collection.parallel.immutable.ParMap


/**
  * Base class to deal with collections of tables coming from Kafka.
  * (Override the val tables and domain).
  *
  * @param kafkaConf The Kafka configuration
  * @param defaultRefreshTime The refresh time (in seconds) after which the parquet file is re-read
  *                           (if not specified in the [[Table]].
  */
abstract class KafkaSources[T <: TopicSourceSink]
(
  val kafkaConf: KafkaConf,
  val defaultRefreshTime: Int = 5, /* in seconds */
  val defaultTrigger: Trigger = Trigger.ProcessingTime(0)
)
extends Source with SparkIO with SourceCollection[T]
  with DataFrameHelpers with KafkaTopicDataFrameHelper
{
  val tables: Seq[Table]

  val payLoadField = "value"

  def getTopicName(table: Table): TopicName = {
    new GenericTopicName(table.name, payLoadField, table.uniqueEntityKey, table.trigger)
  }

  def createTopic(conf: TopicConf): T

  lazy val sources: ParMap[String, T] =
    tables.map(table => {
      val input = TopicConf(
        kafkaConf,
        getTopicName(table),
        SinkConf(table.transformation, table.refreshTime.getOrElse(defaultRefreshTime), 40)
      )

      table.name -> createTopic(input)
    }).toMap.par

  /**
    * Access the table in input
    *
    * @param table The name of the table
    * @return The table
    */
  def apply(table: String): T = sources(table)

  def printTables(): Unit = {
    tables.foreach(t => println(t.name))
  }
}

abstract class TopicSourceSink(conf: TopicConf) extends TopicSource(conf) with SinkData
