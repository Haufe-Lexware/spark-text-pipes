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
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructType, TimestampType}

class TopicSourceKafkaSinkSpec extends SparkSpec
  with SparkIO with KafkaTest with TopicSourceKafkaSinkSpecFixture {
  import currentSparkSession.implicits._

  val inputTopic = "test.kafka.sink.input"
  val outputTopic = "test.kafka.sink.output"
  val kafkaConf: KafkaConf = KafkaConf(kafkaBroker, None)
  val inputTopicName = new GenericTopicName(inputTopic, "value", None)
  val outputTopicName = new GenericTopicName(outputTopic, "value", None)

  var payloadSchema: StructType = _
  val double: DataFrame => DataFrame = {
    df =>
      df.printSchema()

      val newDf = df
        .as("aggDf")
        .withColumn("triple", $"num" * 3)
        .withWatermark("timestamp", "6 seconds")
        .groupBy(
          window($"timestamp", "6 seconds", "3 seconds"),
          $"type"
        )
        .agg(avg($"triple").as("avgtriple"), min($"timestamp").as("timestamp"))
        .join(
          df.as("df"),
          expr(
          """
            |df.type = aggDf.type AND
            |df.timestamp >= aggDf.timestamp - interval 1 hour AND
            |df.timestamp <= aggDf.timestamp + interval 1 hour
          """.stripMargin))
        .select("type", "num", "avgtriple")

      // used later in "from_json" so we don't have to manually specify the schema
      payloadSchema = newDf.schema

      newDf
      .select(to_json(struct(newDf.columns.map(column):_*)).alias("value"))
  }

  val sinkConf = ParquetSinkConf(double, 1, 4)
  val conf = TopicConf(kafkaConf, inputTopicName, sinkConf, Some(outputTopicName))
  val ts = new TopicSourceKafkaSink(conf)

  def sleep(seconds: Int): Unit = Thread.sleep(seconds * 1000)

  "TopicSourceKafkaSink" should "get data from kafka" in {
    // ensure we start clean
    deleteTopic(inputTopic)
    deleteTopic(outputTopic)

    // writing to kafka batch (no streaming)
    df
      .select(to_json(struct(df.columns.map(column):_*)).alias("value"))
      .write
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBroker)
      .option("topic", inputTopic)
      .save()

    // here we read from the input topic, we double the column "num"
    // and write back to the output topic
    ts.reset()
    sleep(30)

    // let's read back the output topic using batch
    val result = currentSparkSession
      .read
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBroker)
      .option("subscribe", outputTopic)
      .load()
      .byteArrayToString("value")

    result.printSchema()

    result
      .withColumn("value", from_json($"value", payloadSchema))
      .expand("value")
      .show(10, 500)

//    ts.stop()
//    ts.delete()
//    deleteTopic(inputTopic)
//    deleteTopic(outputTopic)
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
  ).toDF("num", "type")
}