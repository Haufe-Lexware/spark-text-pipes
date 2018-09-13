import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructType}


object Test {

  val spark: SparkSession = SparkSession.builder().getOrCreate()
  import spark.implicits._

  val brokers = "kafka:9092"

  val inputTopic = "test.kafka.sink.input"
  val aggTopic = "test.kafka.sink.agg"
  val outputTopicSelf = "test.kafka.sink.output.self"
  val outputTopicTwo = "test.kafka.sink.output.two"

  val payloadSchema: StructType = new StructType()
    .add("owner", StringType)
    .add("fruits", StringType)

  val payloadSchemaA: StructType = new StructType()
    .add("owner", StringType)
    .add("fruitsA", StringType)

  var joinedDfSchema: StructType = _

  val sourceDf: DataFrame = Seq(
    ("Brian", "apple"),
    ("Brian", "pear"),
    ("Brian", "melon"),
    ("Brian", "avocado"),
    ("Bob", "avocado"),
    ("Bob", "apple")
  )
    .toDF("owner", "fruits")

  val additionalData: DataFrame = Seq(("Bob", "grapes")).toDF("owner", "fruits")

  def saveDfToKafka(df: DataFrame): Unit = {
    df
      .select(to_json(struct(df.columns.map(column): _*)).alias("value"))
      .write
      .format("kafka")
      .option("kafka.bootstrap.servers", brokers)
      .option("topic", inputTopic)
      .save()
  }

  // save data to kafka (batch)
  saveDfToKafka(sourceDf)

  // kafka source
  val farmDF: DataFrame = spark
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

  def testSelfAggJoinFail(): Unit = {
    // aggregated df
    val myFarmDF = farmDF
      .groupBy($"owner")
      .agg(collect_list(col("fruits")) as "fruitsA")

    // joined df
    val joinedDF = farmDF
      .join(myFarmDF.as("myFarmDF"), Seq("owner"))
      .select("owner", "fruits", "myFarmDF.fruitsA")

    joinedDfSchema = joinedDF.schema

    // stream sink
    joinedDF
      .select(to_json(struct(joinedDF.columns.map(column): _*)).alias("value"))
      .writeStream
      .outputMode("append")
      .option("kafka.bootstrap.servers", brokers)
      .option("checkpointLocation", "/data/kafka/checkpointSelf")
      .option("topic", outputTopicSelf)
      .format("kafka")
      .start()

    // let's give time to process the stream
    Thread.sleep(10000)
  }

  def testSelfAggJoin(): Unit = {
    // aggregated df
    val myFarmDF = farmDF
      .withWatermark("timestamp", "30 seconds")
      .groupBy(
        window($"timestamp", "30 seconds", "15 seconds"),
        $"owner"
      )
      .agg(collect_list(col("fruits")) as "fruitsA")
      .select("owner", "fruitsA", "window")

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

    joinedDfSchema = joinedDF.schema

    // stream sink
    joinedDF
      .select(to_json(struct(joinedDF.columns.map(column): _*)).alias("value"))
      .writeStream
      .outputMode("append")
      .option("kafka.bootstrap.servers", brokers)
      .option("checkpointLocation", "/data/kafka/checkpointSelf")
      .option("topic", outputTopicSelf)
      .format("kafka")
      .start()

    // let's give time to process the stream
    Thread.sleep(10000)
  }

  def testTwoDfAggJoin(): Unit = {
    // aggregated df
    val myFarmDF = farmDF
      .withWatermark("timestamp", "30 seconds")
      .groupBy(
        $"owner"
      )
      .agg(collect_list(col("fruits")) as "fruitsA")
      .select("owner", "fruitsA")

    // save the aggregated df to kafka
    myFarmDF
      .select(to_json(struct(myFarmDF.columns.map(column):_*)).alias("value"))
      .writeStream
      .outputMode("update")
      .option("kafka.bootstrap.servers", brokers)
      .option("checkpointLocation", "/data/kafka/checkpointAgg")
      .option("topic", aggTopic)
      .format("kafka")
      .start()

    // let's give time to process the stream
    Thread.sleep(10000)

    // read the aggregated df from kafka as a stream
    val aggDF = spark
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

    // joined df
    val joinedDF = farmDF
      .as("farmDF")
      .join(
        aggDF.as("myFarmDF"),
        expr(
          """
            |farmDF.owner = myFarmDF.owner AND
            |farmDF.timestamp >= myFarmDF.timestamp - interval 1 hour AND
            |farmDF.timestamp <= myFarmDF.timestamp + interval 1 hour
          """.stripMargin))
      .select("farmDF.owner", "myFarmDF.fruitsA", "farmDF.fruits")

    joinedDfSchema = joinedDF.schema

    // stream sink
    joinedDF
      .select(to_json(struct(joinedDF.columns.map(column):_*)).alias("value"))
      .writeStream
      .outputMode("append")
      .option("kafka.bootstrap.servers", brokers)
      .option("checkpointLocation", "/data/kafka/checkpointTwo")
      .option("topic", outputTopicTwo)
      .format("kafka")
      .start()

    // let's give time to process the stream
    Thread.sleep(10000)
  }

  def data(topic: String): DataFrame = {
    // let's read back the output topic using kafka batch
    spark
      .read
      .format("kafka")
      .option("kafka.bootstrap.servers", brokers)
      .option("subscribe", topic)
      .load()
      .byteArrayToString("value")
      .withColumn("value", from_json($"value", joinedDfSchema))
      .expand("value")
  }
}
