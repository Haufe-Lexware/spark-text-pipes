package com.haufe.umantis.ds.sources.kafka

//package com.haufe.umantis.ds.sources.kafka

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, StringType, StructType}

import scala.util.Try


object MyKafkaTest4 {

  val ss: SparkSession = SparkSession.builder().getOrCreate()

  def test: DataFrame = {

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

    val payloadSchemaA: StructType = new StructType()
      .add("owner", StringType)
      .add("fruitsA", StringType)

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

    farmDF.printSchema()

    // aggregated df
    val myFarmDF = farmDF
      .withWatermark("timestamp", "30 seconds")
      .groupBy(
//        window($"timestamp", "6 seconds"),
        $"owner"
      )
      .agg(collect_list(col("fruits")) as "fruitsA")
      .select("owner", "fruitsA")

    myFarmDF
      .select(to_json(struct(myFarmDF.columns.map(column):_*)).alias("value"))
      .writeStream
      .outputMode("update")
      .option("kafka.bootstrap.servers", brokers)
      .option("checkpointLocation", "/data/kafka/checkpointagg")
      .option("topic", aggTopic)
      .format("kafka")
      .start()

    Thread.sleep(10000)

    val aggDF = ss
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", brokers)
      .option("startingOffsets", "earliest")
      .option("subscribe", aggTopic)
      .load()
      .byteArrayToString("value")
      .withColumn("value", from_json($"value", payloadSchemaA))
      .expand("value")
      .withWatermark("timestamp", "30 seconds")

//    val latestAgg = aggDF
//      .as("myFarmDF")
//      .groupBy($"owner")
//      .agg(max($"timestamp"))

    // joined df
    val joinedDF = farmDF
      .as("farmDF")
      .join(
        aggDF.as("myFarmDF"),
        expr(
          """
            |farmDF.owner = myFarmDF.owner AND
            |farmDF.timestamp >= myFarmDF.timestamp - interval 1 hour AND
            |farmDF.timestamp <= myFarmDF.timestamp + interval 1 hour AND
            |myFarmDF.timestamp = (SELECT max(myFarmDF.timestamp) FROM myFarmDF latest WHERE latest.owner = farmDF.owner)
          """.stripMargin))
      .select("farmDF.owner", "myFarmDF.fruitsA", "farmDF.fruits")

    val schema = joinedDF.schema

    // stream sink
    joinedDF
      .select(to_json(struct(joinedDF.columns.map(column):_*)).alias("value"))
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
    ss
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
