
//package com.haufe.umantis.ds.sources.kafka

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode}
import org.apache.spark.sql.types.{StringType, StructType}

import scala.collection.mutable

case class InputRow(owner: String, fruits: String)
case class UserState(owner: String, fruits: mutable.Buffer[String])
case class HasGrapes(owner: String, var timestamp: Long, var hasGrapes: Boolean)


object SSAggJoinTest extends Serializable {
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

  def sourceDf: DataFrame = Seq(
    ("Brian", "apple"),
    ("Brian", "pear"),
    ("Brian", "melon"),
    ("Brian", "avocado"),
    ("Bob", "avocado"),
    ("Bob", "apple")
  )
    .toDF("owner", "fruits")

  def additionalData: DataFrame = Seq(("Bob", "grapes")).toDF("owner", "fruits")

  implicit class DFHelper(df: DataFrame) {
    def expand(column: String): DataFrame = {
      val wantedColumns = df.columns.filter(_ != column) :+ s"$column.*"
      df.select(wantedColumns.map(col): _*)
    }
  }

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
  def farmDF: DataFrame = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", brokers)
    .option("startingOffsets", "earliest")
    .option("subscribe", inputTopic)
    .load()
    .withColumn("value", $"value".cast("string"))
    .withColumn("value", from_json($"value", payloadSchema))
    .expand("value")

  farmDF.printSchema()

  def tfmws2(): Unit = {

    import spark.implicits._

    def updateUserStateWithEvent(state: HasGrapes, input: InputRow): HasGrapes = {
      state.hasGrapes = if (state.hasGrapes) true else input.fruits == "grapes"
      state.timestamp = System.currentTimeMillis / 1000
      state
    }

    def updateState(user: String,
                    inputs: Iterator[InputRow],
                    oldState: GroupState[HasGrapes]): Iterator[HasGrapes] = {

      val timestamp: Long = System.currentTimeMillis / 1000

      var state: HasGrapes =
        if (oldState.exists) oldState.get else HasGrapes(user, timestamp, hasGrapes = false)
      // we simply specify an old date that we can compare against and
      // immediately update based on the values in our data

      for (input <- inputs) {
        state = updateUserStateWithEvent(state, input)
        oldState.update(state)
      }
      Iterator(state)
    }

    // aggregated df
    val myFarmDF = farmDF
      .select("owner", "fruits")
      .as[InputRow]
      .groupByKey(_.owner)
      .flatMapGroupsWithState(OutputMode.Append(), GroupStateTimeout.NoTimeout())(updateState)
      .toDF("own", "time", "hasGrapes")
//      .writeStream
//      .queryName("farmdf")
//      .format("memory")
//      .outputMode("append")
//      .start()
//
//    val f = spark.sql("SELECT * FROM farmdf")
//
//    val fagg = f
//      .groupBy($"own")
//      .agg(max($"time") as "max_timestamp")
//      .withColumnRenamed("own", "o")
//      .join(f, expr("own = o AND time = max_timestamp"))

    // joined df
    val joinedDF = farmDF
      .join(myFarmDF, expr("owner = own"))
      .select($"owner", $"fruits", $"hasGrapes", $"time")

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
  }

  def tfmws(): Unit = {

    import spark.implicits._

    def updateUserStateWithEvent(state: HasGrapes, input: InputRow): HasGrapes = {
      state.hasGrapes = if (state.hasGrapes) true else input.fruits == "grapes"
      state.timestamp = System.currentTimeMillis / 1000
      state
    }

    def updateState(user: String,
                    inputs: Iterator[InputRow],
                    oldState: GroupState[HasGrapes]): Iterator[HasGrapes] = {

      val timestamp: Long = System.currentTimeMillis / 1000

      var state: HasGrapes =
        if (oldState.exists) oldState.get else HasGrapes(user, timestamp, hasGrapes = false)
      // we simply specify an old date that we can compare against and
      // immediately update based on the values in our data

      for (input <- inputs) {
        state = updateUserStateWithEvent(state, input)
        oldState.update(state)
      }
      Iterator(state)
    }

    // aggregated df
    val myFarmDF = farmDF
      .select("owner", "fruits")
      .as[InputRow]
      .groupByKey(_.owner)
      .flatMapGroupsWithState(OutputMode.Append(), GroupStateTimeout.NoTimeout())(updateState)
      .toDF("own", "timestamp", "hasGrapes")
      .writeStream
      .queryName("farmdf")
      .format("memory")
      .outputMode("append")
      .start()

    val f = spark.sql("SELECT * FROM farmdf")

    val fagg = f
      .groupBy($"own")
      .agg(max($"timestamp") as "max_timestamp")
      .withColumnRenamed("own", "o")
      .join(f, expr("own = o AND timestamp = max_timestamp"))

    // joined df
    val joinedDF = farmDF
      .join(fagg, expr("owner = own"))
      .select("owner", "fruits", "hasGrapes")

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
  }

  def testSelfFlatGroupWithState(): Unit = {

    import spark.implicits._

    def updateUserStateWithEvent(state: UserState, input: InputRow): UserState = {
      state.fruits.append(input.fruits)
      state
    }

    def updateState(user: String,
                    inputs: Iterator[InputRow],
                    oldState: GroupState[UserState]): Iterator[UserState] = {
      var state: UserState =
        if (oldState.exists) oldState.get else UserState(user, mutable.Buffer[String]())
      // we simply specify an old date that we can compare against and
      // immediately update based on the values in our data

      for (input <- inputs) {
        state = updateUserStateWithEvent(state, input)
        oldState.update(state)
      }
      Iterator(state)
    }

    // aggregated df
    val myFarmDF = farmDF
      .select("owner", "fruits")
      .as[InputRow]
      .groupByKey(_.owner)
      .flatMapGroupsWithState(OutputMode.Append(), GroupStateTimeout.NoTimeout())(updateState)
      .toDF("own", "fr")
//      .writeStream
//      .queryName("farmdf")
//      .format("memory")
//      .outputMode("append")
//      .start()
//
//    spark.sql("SELECT * FROM farmdf")

    // joined df
    val joinedDF = farmDF
      .join(myFarmDF, expr("owner = own"))
      .select("owner", "fruits", "fr")

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
  }

  def testSelfGroupWithState(): Unit = {

    import spark.implicits._

    def updateUserStateWithEvent(state: HasGrapes, input: InputRow): HasGrapes = {
      state.hasGrapes = if (state.hasGrapes) true else input.fruits == "grapes"
      state.timestamp = System.currentTimeMillis / 1000
      state
    }

    def updateAcrossEvents(user: String,
                           inputs: Iterator[InputRow],
                           oldState: GroupState[HasGrapes]): HasGrapes = {
      val timestamp: Long = System.currentTimeMillis / 1000

      var state: HasGrapes =
        if (oldState.exists) oldState.get else HasGrapes(user, timestamp, false)
      // we simply specify an old date that we can compare against and
      // immediately update based on the values in our data

      for (input <- inputs) {
        state = updateUserStateWithEvent(state, input)
        oldState.update(state)
      }
      state
    }

    // aggregated df
    val myFarmDF = farmDF
      .select("owner", "fruits")
      .as[InputRow]
      .groupByKey(_.owner)
      .mapGroupsWithState(GroupStateTimeout.NoTimeout())(updateAcrossEvents)
      .toDF("own", "hasGrapes")
      .writeStream
      .queryName("farmdf")
      .format("memory")
      .outputMode("update")
      .start()

    val f: DataFrame = spark.sql("SELECT * FROM farmdf")

    // joined df
    val joinedDF = farmDF
      .join(f, expr("owner = own"))
      .select("owner", "fruits", "hasGrapes")

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

  def testSelfAggJoin2(): Unit = {
    // aggregated df
    val myFarmDF = farmDF
      .withWatermark("timestamp", "30 seconds")
      .groupBy($"timestamp", $"owner")
      .agg(collect_list(col("fruits")) as "fruitsA")
      .select("owner", "fruitsA", "timestamp")

    // joined df
    val joinedDF = farmDF
      .as("farmDF")
      //      .withWatermark("timestamp", "30 seconds")
      .join(
      myFarmDF.as("myFarmDF"),
      expr(
        """
          |farmDF.owner = myFarmDF.owner AND
          |farmDF.timestamp >= myFarmDF.timestamp - interval 1 hour AND
          |farmDF.timestamp <= myFarmDF.timestamp + interval 1 hour
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

  def testAgg(): Unit = {
    // aggregated df
    val myFarmDF = farmDF
      .withColumn("timestamp", current_timestamp())
      .withWatermark("timestamp", "5 seconds")
      //      .groupBy($"timestamp", $"owner")
      .groupBy($"timestamp")
      .agg(collect_list(col("fruits")) as "fruitsA")

    joinedDfSchema = myFarmDF.schema

    // stream sink
    myFarmDF
      .select(to_json(struct(myFarmDF.columns.map(column): _*)).alias("value"))
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

  def testTwoDfAggJoinFail(): Unit = {
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
      .withColumn("value", $"value".cast("string"))
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

  def data(topic: String, schema: Option[StructType] = None): DataFrame = {
    // let's read back the output topic using kafka batch

    val mySchema = schema match {
      case Some(s) => s
      case _ => joinedDfSchema
    }

    spark
      .read
      .format("kafka")
      .option("kafka.bootstrap.servers", brokers)
      .option("subscribe", topic)
      .load()
      .withColumn("value", $"value".cast("string"))
      .withColumn("value", from_json($"value", mySchema))
      .expand("value")
  }
}
