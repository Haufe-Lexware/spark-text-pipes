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

import com.databricks.spark.avro.ConfluentSparkAvroUtils
import com.haufe.umantis.ds.spark.DataFrameHelpers
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.col

import scala.util.Try


class KafkaDeserializer(conf: TopicConf) {

  lazy val avroUtils: Option[ConfluentSparkAvroUtils] =
    conf.kafkaConf.schemaRegistryURL match {
      case Some(url) => Some(TopicSourceParquetSink.getAvroUtils(url))
      case _ => None
    }

  private lazy val avroKeyDeserializer: Option[UserDefinedFunction] =
    avroUtils match {
      case Some(utils) =>
        Try(Some(utils.deserializerForSubject(conf.subjectKeyName))).getOrElse(None)
      case _ => None
    }

  private lazy val avroValueDeserializer: Option[UserDefinedFunction] =
    avroUtils match {
      case Some(utils) =>
        Try(Some(utils.deserializerForSubject(conf.subjectValueName))).getOrElse(None)
      case _ => None
    }

  def isKeyAvro: Boolean = {
    avroKeyDeserializer match {
      case Some(_) => true
      case _ => false
    }
  }

  def isValueAvro: Boolean = {
    avroValueDeserializer match {
      case Some(_) => true
      case _ => false
    }
  }

  implicit class SourceHelpers(df: DataFrame)
    extends DataFrameHelpers with KafkaTopicDataFrameHelper {

    def deserialize(keyColumn: String, valueColumn: String): DataFrame = {

      val dfTmp = avroKeyDeserializer match {
        case Some(des) => df.deserializeAvro(keyColumn, des)
        case _ => df.byteArrayToString(keyColumn)
      }

      avroValueDeserializer match {
        case Some(des) => dfTmp.deserializeAvro(valueColumn, des)
        case _ => dfTmp.byteArrayToString(valueColumn)
      }
    }

    /**
      * Expands a column while keeping all the other ones.
      * @param column The column to expand
      * @return The modified DataFrame
      */
    def expand(column: String): DataFrame = {
      val wantedColumns = df.columns.filter(_ != column) :+ s"$column.*"
      df.select(wantedColumns.map(col):_*)
    }

//    /**
//      * If the value is serialized with Avro, we expand it.
//      * @param column The column to expand
//      * @return The modified DataFrame
//      */
//    def expand(column: String): DataFrame = {
//      avroValueDeserializer match {
//        case Some(_) => df.selectKafkaColsAnd(column)
//        case _ => df
//      }
//    }
  }
}
