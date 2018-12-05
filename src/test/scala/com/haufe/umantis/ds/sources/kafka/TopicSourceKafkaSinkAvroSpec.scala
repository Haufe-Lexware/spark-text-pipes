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

import com.haufe.umantis.ds.sources.kafka.serde.KafkaSerde
import com.haufe.umantis.ds.spark.{DataFrameHelpers, SparkIO, SparkSessionWrapper}
import com.haufe.umantis.ds.tests.SparkSpec
import org.apache.spark.sql.catalyst.ScalaReflection.schemaFor
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.scalatest.BeforeAndAfter

class TopicSourceKafkaSinkAvroSpec extends SparkSpec
  with SparkIO
  with KafkaExternalServices
  with TopicSourceKafkaSinkSpecFixture
  with BeforeAndAfter
  with KafkaSerde
{

  import currentSparkSession.implicits._

  val inputTopic = "test.kafka.sink.avro.input"
  val outputTopic = "test.kafka.sink.avro.output"
  val kafkaConf: KafkaConf = KafkaConf(kafkaBroker, Some(avroSchemaRegistry))
  val inputTopicName = new GenericTopicName(inputTopic, "value", None)
  val outputTopicName = new GenericTopicName(outputTopic, "value", None)

  val transformationFunction: DataFrame => DataFrame = {
    df =>

      val df1 = df
        .withColumn("triple", $"num" * 3)
        .select("num", "type", "triple")

      val newSchema = StructType(
        df1.schema.fields :+
          StructField("mapPartCol", schemaFor[Int].dataType, nullable = true)
      )

      import org.apache.spark.sql.catalyst.encoders.RowEncoder
      implicit val encoder: ExpressionEncoder[Row] = RowEncoder(newSchema)

      df1
        .mapPartitions(iter => {
          iter.map(r => {
            val s = r.getAs[Int]("num")

            Row.fromSeq(r.toSeq :+ s)
          })
        })
  }

  val sinkConf = SinkConf(transformationFunction, 1, 4)
  val conf = TopicConf(kafkaConf, inputTopicName, sinkConf, Some(outputTopicName))
  val ts = new TopicSourceKafkaSink(conf)

  def sleep(seconds: Int): Unit = Thread.sleep(seconds * 1000)

  "TopicSourceKafkaSink" should "get data from kafka" in {
    // ensure we start clean
    deleteTopic(inputTopic)
    deleteTopic(outputTopic)
    deleteSubject(inputTopic + "-value")
    deleteSubject(outputTopic + "-value")

    sleep(5)

    // writing to kafka batch (no streaming)
    df
      .debugPrintSchema(Some("fixture"))
      .serialize(None, None, "value", Some(df.columns), inputTopic)
      .debugPrintSchema(Some("input"))
      .debugShow()
      .write
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBroker)
      .option("topic", inputTopic)
      .save()

    // here we read from the input topic, we triple the column "num"
    // and write back to the output topic
    ts.reset()
    sleep(5)

    // let's read back the output topic using batch
    val result = ts.data
      .select("num", "type", "triple", "mapPartCol")
      .sort("num")

    val expectedResult = df
      .withColumn("triple", $"num" * 3)
      .withColumn("mapPartCol", $"num")
      .select($"num", $"type", $"triple", $"mapPartCol")
      .setNullableStateOfColumn("num", nullable = true)
      .setNullableStateOfColumn("triple", nullable = true)
      .setNullableStateOfColumn("mapPartCol", nullable = true)

    assertSmallDataFrameEquality(result, expectedResult)

    ts.stop()
    ts.delete()
    deleteTopic(inputTopic)
    deleteTopic(outputTopic)
    deleteSubject(inputTopic + "-value")
    deleteSubject(outputTopic + "-value")
  }

  after {
    deleteTopic(inputTopic)
    deleteTopic(outputTopic)
    deleteSubject(inputTopic + "-value")
    deleteSubject(outputTopic + "-value")
  }
}
