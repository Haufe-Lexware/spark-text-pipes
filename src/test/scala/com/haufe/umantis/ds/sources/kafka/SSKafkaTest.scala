package com.haufe.umantis.ds.sources.kafka

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructType}

import scala.util.Try

object MyKafkaTest {

  val ss: SparkSession = SparkSession.builder().getOrCreate()

  def test(mode: String): DataFrame = {

    implicit class DFHelper(df: DataFrame) {
      def expand(column: String): DataFrame = {
        val wantedColumns = df.columns.filter(_ != column) :+ s"$column.*"
        df.select(wantedColumns.map(col): _*)
      }

      def byteArrayToString(column: String): DataFrame = {
        val byteArrayToStringUDF: UserDefinedFunction =
          udf((payload: Array[Byte]) => Try(new String(payload)).getOrElse(null))

        df.withColumn(column, byteArrayToStringUDF(col(column)))
      }
    }

    import ss.implicits._

    val brokers = "kafka:9092"

    val payloadSchema: StructType = new StructType()
      .add("owner", StringType)
      .add("fruits", StringType)

    val sourceDf = Seq(
      ("Brian", "apple"),
      ("Brian", "pear"),
      ("Brian", "melon"),
      ("Brian", "avocado"),
      ("Bob", "avocado"),
      ("Bob", "apple")
    )
      .toDF("owner", "fruits")

    val inputTopic = "test.kafka.sink.input"
    val outputTopic = "test.kafka.sink.output"

    // save data on kafka (batch)
    sourceDf
      .select(to_json(struct(sourceDf.columns.map(column): _*)).alias("value"))
      .write
      .format("kafka")
      .option("kafka.bootstrap.servers", brokers)
      .option("topic", inputTopic)
      .save()

    // kafka source
    val farmDF = ss
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", brokers)
      .option("startingOffsets", "earliest")
      .option("subscribe", inputTopic)
      .load()
      .byteArrayToString("value")
      .withColumn("value", from_json($"value", payloadSchema))
      .expand("value")

    // aggregated df
    val myFarmDF = farmDF
      .withWatermark("timeStamp", "1 seconds")
      .groupBy("owner")
      .agg(collect_list(col("fruits")) as "fruitsA")

    // joined df
    val joinedDF = farmDF
      .join(myFarmDF, "owner")
      .drop("fruits")

    // stream sink
    joinedDF
      .writeStream
      .outputMode(mode)
      .option("checkpointLocation", "/data/kafka/checkpoint")
      .option("topic", outputTopic)
      .format("kafka")
      .start()

    // let's give time to process the stream
    Thread.sleep(5000)

    // let's read back the output topic using batch
    ss
      .read
      .format("kafka")
      .option("kafka.bootstrap.servers", brokers)
      .option("subscribe", outputTopic)
      .load()
      .byteArrayToString("value")
      .withColumn("value", from_json($"value", payloadSchema))
      .expand("value")
  }

  test("append")
  test("update")
}
