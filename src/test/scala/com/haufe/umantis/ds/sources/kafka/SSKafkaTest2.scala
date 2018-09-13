//package com.haufe.umantis.ds.sources.kafka

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructType}


object MyKafkaTest2 {

  val spark: SparkSession = SparkSession.builder().getOrCreate()

  def test: DataFrame = {

    implicit class DFHelper(df: DataFrame) {
      def expand(column: String): DataFrame = {
        val wantedColumns = df.columns.filter(_ != column) :+ s"$column.*"
        df.select(wantedColumns.map(col): _*)
      }

      def byteArrayToString(column: String): DataFrame = {
        val selectedCols = df.columns.filter(_ != column) :+ s"CAST($column AS STRING)"
        df.selectExpr(selectedCols: _*)
      }
    }

    import spark.implicits._

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
    val aggTopic = "test.kafka.sink.agg"
    val outputTopic = "test.kafka.sink.output"

    // save data to kafka (batch)
    sourceDf
      .select(to_json(struct(sourceDf.columns.map(column): _*)).alias("value"))
      .write
      .format("kafka")
      .option("kafka.bootstrap.servers", brokers)
      .option("topic", inputTopic)
      .save()

    // kafka source
    val farmDF = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", brokers)
      .option("startingOffsets", "earliest")
      .option("subscribe", inputTopic)
      .load()
      .byteArrayToString("value")
      .withColumn("value", from_json($"value", payloadSchema))
      .expand("value")

    farmDF.printSchema()

    // aggregated df
    val myFarmDF = farmDF
      .withWatermark("timestamp", "30 seconds")
      .groupBy(
        window($"timestamp", "30 seconds", "15 seconds"),
        $"owner"
      )
      .agg(collect_list(col("fruits")) as "fruitsA")
      .select("owner", "fruitsA", "window")

    // giving some time to process it
    Thread.sleep(10000)

    // joined df
    val joinedDF = farmDF
        .as("farmDF")
      .withWatermark("timestamp", "30 seconds")
      .join(
        myFarmDF.as("myFarmDF"),
        expr(
          """
            |farmDF.owner = myFarmDF.owner AND
            |farmDF.timestamp >= myFarmDF.window.start AND
            |farmDF.timestamp <= myFarmDF.window.end
          """.stripMargin))
      .select("farmDF.owner", "farmDF.fruits", "myFarmDF.fruitsA")

    val schema = joinedDF.schema

    // stream sink
    joinedDF
      .select(to_json(struct(joinedDF.columns.map(column): _*)).alias("value"))
      .writeStream
      .outputMode("append")
      .option("kafka.bootstrap.servers", brokers)
      .option("checkpointLocation", "/data/kafka/checkpoint")
      .option("topic", outputTopic)
      .format("kafka")
      .start()

    // let's give time to process the stream
    Thread.sleep(10000)

    // let's read back the output topic using kafka batch
    spark
      .read
      .format("kafka")
      .option("kafka.bootstrap.servers", brokers)
      .option("subscribe", outputTopic)
      .load()
      .byteArrayToString("value")
      .withColumn("value", from_json($"value", schema))
      .expand("value")
  }
}
