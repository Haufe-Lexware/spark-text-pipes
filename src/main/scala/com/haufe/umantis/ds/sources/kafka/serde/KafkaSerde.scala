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
import com.haufe.umantis.ds.spark.DataFrameHelpers
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

import scala.util.Try


class KafkaSerde(conf: TopicConf) extends DataFrameAvroHelpers {

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
          df.withColumn(keyColumn, col(keyColumn).cast("string"))
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
          dfWithDeserializedKey.withColumn(valueColumn, col(valueColumn).cast("string"))
      }
    }
  }
}
