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


import com.haufe.umantis.ds.nlp.{ColnamesText, DsPipeline, DsPipelineInput, StandardPipeline}
import com.haufe.umantis.ds.spark.{DataFrameHelpers, SparkIO, SparkSessionWrapper}
import com.haufe.umantis.ds.tests.SparkSpec
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.avro._
import org.scalatest.BeforeAndAfter
import org.scalatest.Matchers._
import kafka.admin.AdminUtils
import kafka.utils.ZkUtils
import kafka.zk.{AdminZkClient, KafkaZkClient}
import org.apache.kafka.clients.admin.{AdminClient, KafkaAdminClient}

import scala.sys.process._

trait KafkaTest extends SparkIO with TopicSourceEventSourcingSpecFixture {

  val kafkaPythonUtilitiesPath = s"${appsRoot}scripts/py-kafka-avro-console"

  def sendEvents(keyschema: String, schema: String, topic: String, events: String): String = {
    val stream: java.io.InputStream =
      new java.io.ByteArrayInputStream(
        events.getBytes(java.nio.charset.StandardCharsets.UTF_8.name))

    val command = Seq(
      "/usr/bin/python3",
      s"$kafkaPythonUtilitiesPath/kafka_avro_producer.py",
      s"--brokers $kafkaBroker",
      s"--registry $avroSchemaRegistry",
      s"--keyschema $keyschema",
      s"--schema $schema",
      s"--topic $topic"
    )
      .mkString(" ")

    command #< stream !!
  }

  def deleteTopic(topic: String): Unit = {
    val command = Seq(
      "/usr/bin/python3",
      s"$kafkaPythonUtilitiesPath/kafka_delete_topic.py",
      s"--brokers $kafkaBroker",
      s"--zookeeper $zookeeper",
      s"--topic $topic"
    )
      .mkString(" ")

    println(command)

    command !!
  }
}

trait TopicSourceEventSourcingSpec
  extends SparkSpec
    with SparkIO with TopicSourceEventSourcingSpecFixture {
  import currentSparkSession.implicits._

  currentSparkSession.sparkContext.setLogLevel("WARN")

//  val kafkaZkClient: KafkaZkClient = {
//    import org.apache.kafka.common.utils.Time
//    KafkaZkClient(
//      zookeeper,
//      isSecure=false,
//      sessionTimeoutMs=200000,
//      connectionTimeoutMs=15000,
//      maxInFlightRequests=10,
//      time=Time.SYSTEM,
//      metricGroup="myGroup",
//      metricType="myType")
//  }

//  val adminZkClient: AdminZkClient = AdminZkClient(kafkaZkClient)

//  val a = org.apache.kafka.clients.admin.AdminClient.create()

  val adminClient: AdminClient = {
    val props = new java.util.Properties()
    props.setProperty("bootstrap.servers", kafkaBroker)
    org.apache.kafka.clients.admin.AdminClient.create(props)
  }


  /** Delete a Kafka topic and wait until it is propagated to the whole cluster */
  def deleteTopic(topic: String): Unit = {
//    kafkaZkClient.deleteTopic
//    val partitions = zkUtils.getPartitionsForTopics(Seq(topic))(topic).size
//    AdminUtils.deleteTopic(zkUtils, topic)
    import collection.JavaConverters._
    adminClient.deleteTopics(List(topic).asJavaCollection).all().get()
//    kafka.zk.AdminZkClient
//    verifyTopicDeletionWithRetries(zkUtils, topic, partitions, List(this.server))
  }

  def topic: String

  def kafkaConf: KafkaConf = KafkaConf(kafkaBroker, Some(avroSchemaRegistry))
  def topicName =
    new GenericTopicName(
      topic, "value", Some(GenericUniqueIdentityKeys.EntityAndTimestampFromAvroKeyUDF))
//  def identity: DataFrame => DataFrame = {df => df}
  def transformationFunction: DataFrame => DataFrame = {df =>
    df
      .transformWithPipeline(
        DsPipeline(
          DsPipelineInput(
            ColnamesText("f1"),
            StandardPipeline.TextDataPreprocessing
          )
        ).pipeline
      )
  }
  def sinkConf = SinkConf(transformationFunction, refreshTime = 1 /* seconds */, numPartitions = 4)
  def conf = TopicConf(kafkaConf, topicName, sinkConf)
  def ts: TopicSourceSink

  def sendEvents(events: String): Unit = {
    println($"sending events to ${topicName.topic}:\n$events")
//    sendEvents(keyschema, schema, topic, events)
    toDF(events)
      .write
      .format("kafka")
//      .option("schema.registry.url", avroSchemaRegistry)
      .option("kafka.bootstrap.servers", kafkaBroker)
      .option("topic", topicName.topic)
      .save()

    currentSparkSession
      .read
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBroker)
      .option("topic", topicName.topic)
      .load()
      .show()
  }

  def sleep(seconds: Int): Unit = Thread.sleep(seconds * 1000)

  def currentDf: DataFrame = ts.data.sort($"f1")
  def show(): Unit = {
    currentDf.columns.foreach(c => println(c))
//    currentDf.select("key").show()
    currentDf.show(20, 100)
  }
  def values: DataFrame = currentDf.select("f1", "f2")

  def doTest(): Unit = {

    // ensure the topic does not exist
    deleteTopic(topic)

//    toDF(createABC)

    // entity creation
    sendEvents(createABC)
    sleep(2)
    ts.reset()
    show()
    assertSmallDataFrameEquality(values, df1)

    // entity modification
    sendEvents(modifyA)
    sleep(2)
    show()
    assertSmallDataFrameEquality(values, df2)

    // entity modification and deletion
    sendEvents(modifyBdeleteC)
    sleep(2)
    show()
    assertSmallDataFrameEquality(values, df3)

    // entity deletion and creation of new ones
    sendEvents(deleteABcreateDEF)
    sleep(2)
    show()
    assertSmallDataFrameEquality(values, df4)

    // now we delete the topic in kafka and add new entities
    // TopicSource should act by resetting its state (including the associated parquet file)
    // NOTE: disabled because data can be aged out by kafka and we cannot fail every time it
    // happens.

    //    println(
    //      "NOTE by Nicola Bova: An exception should be printed on screen, " +
    //      "as the Kafka topic has just ben deleted and Sparks complains about it." +
    //      "TopicSource, however, should be able to reset its state and continue to work.\n")
    //    deleteTopic(topic)
    //    sendEvents(createABC)
    //    sleep(2)
    //    show()
    //    assertSmallDataFrameEquality(values, df1)

    // cleanup
    ts.stop()
    deleteTopic(topic)
  }
}

class TopicSourceParquetSinkEventSourcingSpec extends TopicSourceEventSourcingSpec {
  override val topic = "test.event.sourcing.parquetsink"
  override val ts = new TopicSourceParquetSinkEventSourcing(conf)

  "TopicSourceParquetSinkEventSourcing" should
    "get data from kafka, including updates, deletes, and resets." in { doTest() }
}

class TopicSourceKafkaSinkEventSourcingSpec extends TopicSourceEventSourcingSpec
  with BeforeAndAfter {

  override val topic = "test.event.sourcing.kafkasink"
  override val ts = new TopicSourceKafkaSinkEventSourcing(conf)

  "TopicSourceKafkaSinkEventSourcing" should
    "get data from kafka, including updates, deletes, and resets." in { doTest() }

  after {
    deleteTopic(ts.outputTopicName)
  }
}


trait TopicSourceEventSourcingSpecFixture extends SparkSessionWrapper with DataFrameHelpers {
  import currentSparkSession.implicits._

  def fixture(data: Seq[(String, Int)]): DataFrame =
    data
      .map{ case (s, i) => (Some(s"I am entity $s"), Some(i))} // Some() so columns are set nullable
      .toDF("f1", "f2")
      .sort($"f1")

  val df1: DataFrame = fixture(Seq(("A", 3), ("B", 4), ("C", 5)))
  val df2: DataFrame = fixture(Seq(("A", 10), ("B", 4), ("C", 5)))
  val df3: DataFrame = fixture(Seq(("A", 10), ("B", 11)))
  val df4: DataFrame = fixture(Seq(("D", 7), ("E", 8), ("F", 9)))

  val keyschema = """{"name":"key","type":"record","fields":[{"name":"service_name","type":"string"},{"name":"tenant_id","type":"string"},{"name":"identity_id","type":"string"},{"name":"entity_id","type":"string"},{"name":"timestamp","type":"long","logicalType":"timestamp-millis"}]}"""
  val schema = """{"type":"record","name":"myrecord","fields":[{"name":"f1","type":"string"},{"name":"f2","type":"int"}]}"""

  def toDF(strDf: String): DataFrame = {
    strDf
      .split('\n').toSeq
      .map(_.split('|'))
      .map { case Array(f1, f2) => (f1, f2) }
      .toDF("key", "value")
//      .expand_json("key")
//      .expand_json("value")
//      .alsoShow()
//      .withColumn("key", to_avro($"key"))
//      .withColumn("value", to_avro($"value"))
//      .alsoShow()
  }

  val createABC: String =
    """
      |{"service_name": "test", "tenant_id": "1000", "identity_id": "4024fea2-2c12-4343-bc20-9340d3add001", "entity_id": "37700", "timestamp": 1525710609}|{"f1": "I am entity A","f2": 3}
      |{"service_name": "test", "tenant_id": "1000", "identity_id": "4024fea2-2c12-4343-bc20-9340d3add001", "entity_id": "37777", "timestamp": 1525720609}|{"f1": "I am entity B","f2": 4}
      |{"service_name": "test", "tenant_id": "1000", "identity_id": "4024fea2-2c12-4343-bc20-9340d3add001", "entity_id": "37788", "timestamp": 1525730609}|{"f1": "I am entity C","f2": 5}
    """.stripMargin.trim

  val modifyA: String =
    """
      |{"service_name": "test", "tenant_id": "1000", "identity_id": "4024fea2-2c12-4343-bc20-9340d3add001", "entity_id": "37700", "timestamp": 1525740609}|{"f1": "I am entity A","f2": 10}
    """.stripMargin.trim

  val modifyBdeleteC: String =
    """
      |{"service_name": "test", "tenant_id": "1000", "identity_id": "4024fea2-2c12-4343-bc20-9340d3add001", "entity_id": "37777", "timestamp": 1525750609}|{"f1": "I am entity B","f2": 11}
      |{"service_name": "test", "tenant_id": "1000", "identity_id": "4024fea2-2c12-4343-bc20-9340d3add001", "entity_id": "37788", "timestamp": 1525760609}|null
    """.stripMargin.trim

  val deleteABcreateDEF: String =
    """
      |{"service_name": "test", "tenant_id": "1000", "identity_id": "4024fea2-2c12-4343-bc20-9340d3add001", "entity_id": "37700", "timestamp": 1525770609}|null
      |{"service_name": "test", "tenant_id": "1000", "identity_id": "4024fea2-2c12-4343-bc20-9340d3add001", "entity_id": "37777", "timestamp": 1525780609}|null
      |{"service_name": "test", "tenant_id": "1000", "identity_id": "4024fea2-2c12-4343-bc20-9340d3add001", "entity_id": "38700", "timestamp": 1525790609}|{"f1": "I am entity D","f2": 7}
      |{"service_name": "test", "tenant_id": "1000", "identity_id": "4024fea2-2c12-4343-bc20-9340d3add001", "entity_id": "38777", "timestamp": 1525800609}|{"f1": "I am entity E","f2": 8}
      |{"service_name": "test", "tenant_id": "1000", "identity_id": "4024fea2-2c12-4343-bc20-9340d3add001", "entity_id": "38788", "timestamp": 1525810609}|{"f1": "I am entity F","f2": 9}
    """.stripMargin.trim
}
