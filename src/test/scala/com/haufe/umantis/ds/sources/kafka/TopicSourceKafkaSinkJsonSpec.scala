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

import com.haufe.umantis.ds.spark.{SparkIO, SparkSessionWrapper}
import com.haufe.umantis.ds.tests.SparkSpec
import org.apache.spark.sql.catalyst.ScalaReflection.schemaFor
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.scalatest.BeforeAndAfter

class TopicSourceKafkaSinkJsonSpec extends SparkSpec
  with SparkIO with KafkaExternalServices with TopicSourceKafkaSinkSpecFixture with BeforeAndAfter {
  import currentSparkSession.implicits._

  val inputTopic = "test.kafka.sink.input"
  val outputTopic = "test.kafka.sink.output"
  val kafkaConf: KafkaConf = KafkaConf(kafkaBroker, None)
  val inputTopicName = new GenericTopicName(inputTopic, "value", None)
  val outputTopicName = new GenericTopicName(outputTopic, "value", None)

  val transformationFunction: DataFrame => DataFrame = {
    df =>

      val df1 = df
        .withColumn("triple", $"num" * 3)
        .select("num", "type", "triple")

      val newSchema = StructType(
        df1.schema.fields :+
          StructField("mapPartCol", schemaFor[Long].dataType, nullable = true)
      )

      import org.apache.spark.sql.catalyst.encoders.RowEncoder
      implicit val encoder: ExpressionEncoder[Row] = RowEncoder(newSchema)

      df1
        .mapPartitions(iter => {
          iter.map(r => {
            val s = r.getAs[Long]("num")

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

    sleep(5)

    // writing to kafka batch (no streaming)
    df
      .select(to_json(struct(df.columns.map(column):_*)).alias("value"))
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
      .select($"num".cast(LongType), $"type", $"triple".cast(LongType), $"mapPartCol".cast(LongType))
      .setNullableStateOfColumn("num", nullable = true)
      .setNullableStateOfColumn("triple", nullable = true)
      .setNullableStateOfColumn("mapPartCol", nullable = true)

    assertSmallDataFrameEquality(result, expectedResult)

    ts.stop()
    ts.delete()
    deleteTopic(inputTopic)
    deleteTopic(outputTopic)
  }

  after {
    deleteTopic(inputTopic)
    deleteTopic(outputTopic)
  }
}

trait TopicSourceKafkaSinkSpecFixture extends SparkSessionWrapper {
  import currentSparkSession.implicits._

  val df: DataFrame = Seq(
    (1, "a"),
    (2, "b"),
    (3, "a"),
    (4, "a"),
    (5, "b")
  )
    .toDF("num", "type")
}