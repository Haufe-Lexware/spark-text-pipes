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

import com.haufe.umantis.ds.spark.{DataFrameHelpers, SparkSessionWrapper}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, max, when}

class TopicSourceParquetSinkEventSourcing(conf: TopicConf)
  extends TopicSourceParquetSink(conf) with TopicSourceEventSourcingTrait {

  override def preProcessDf(df: DataFrame): DataFrame =
    super[TopicSourceEventSourcingTrait].preProcessDf(df)

  override def postProcessDf(df: DataFrame): DataFrame =
    super[TopicSourceEventSourcingTrait].postProcessDf(df)
}

class TopicSourceKafkaSinkEventSourcing(conf: TopicConf)
  extends TopicSourceKafkaSink(conf) with TopicSourceEventSourcingTrait {

  override def preProcessDf(df: DataFrame): DataFrame =
    super[TopicSourceEventSourcingTrait].preProcessDf(df)

  override def postProcessDf(df: DataFrame): DataFrame =
    super[TopicSourceEventSourcingTrait].postProcessDf(df)
}


trait TopicSourceEventSourcingTrait extends SparkSessionWrapper with DataFrameHelpers {
  import currentSparkSession.implicits._

  def conf: TopicConf

  def preProcessDf(df: DataFrame): DataFrame = {
    implicit class PreprocessHelper(df: DataFrame) {
      def uniqueKeyAndTimestamp(keyField: String): DataFrame = {
        conf.kafkaTopic.uniqueEntityKey match {
          case Some(userDefinedFunction) =>
            df.withColumn("unique_key_and_timestamp", userDefinedFunction(col(keyField)))
          case _ => df
        }
      }
    }

    df
      .uniqueKeyAndTimestamp("key")
      .withColumn("unique_entity_key", $"unique_key_and_timestamp.unique_entity")
      .withColumn("producer_timestamp", $"unique_key_and_timestamp.timestamp")
      .drop("unique_key_and_timestamp")
      .withColumn("kafka_value_is_null",
        when(col(conf.kafkaTopic.payloadField).isNull, true).otherwise(false))
  }

  def postProcessDf(df: DataFrame): DataFrame = {
    df
      .alsoPrintSchema(Some("TopicSourceEventSourcingTrait before postProcessDf"))
      .alsoShow(20, 15)
      .groupBy($"unique_entity_key")
      .agg(max($"producer_timestamp") as "producer_timestamp")
      .join(df, Seq("unique_entity_key", "producer_timestamp"))
      .where($"kafka_value_is_null" === false)
      .drop("kafka_value_is_null", "unique_entity_key", "producer_timestamp")
      .alsoPrintSchema(Some("TopicSourceEventSourcingTrait after postProcessDf"))
      .alsoShow(20, 15)
  }
}


trait KafkaTopicDataFrameHelper {

  val mandatoryColumns: Seq[String] =
    Seq("key", "unique_entity_key", "producer_timestamp", "timestamp", "kafka_value_is_null")

  implicit class KafkaTopicDataFrameHelper(df: DataFrame) extends Serializable {

    @scala.annotation.varargs
    def selectKafkaColsAnd(cols: String*): DataFrame = {
      df.select((mandatoryColumns ++ cols.toSeq).map(col): _*)
    }
  }
}