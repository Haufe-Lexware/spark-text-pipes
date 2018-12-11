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

import com.haufe.umantis.ds.sources.kafka.serde.{DataFrameAvroHelpers, DataFrameJsonHelpers}
import com.haufe.umantis.ds.spark.SparkIO
import com.haufe.umantis.ds.tests.SparkSpec
import org.scalatest.Matchers._


class AvroSerdeSpec
  extends SparkSpec
    with SparkIO
    with DataFrameAvroHelpers
    with DataFrameJsonHelpers
    with KafkaExternalServices
{

  import currentSparkSession.implicits._

  currentSparkSession.sparkContext.setLogLevel("WARN")

  val testData: String =
    """
      |{"service_name": "test", "tenant_id": "1000", "identity_id": "4024fea2-2c12-4343-bc20-9340d3add001", "entity_id": "37700", "timestamp": 1525710609}|{"f1": "I am entity A","f2": 3}
      |{"service_name": "test", "tenant_id": "1000", "identity_id": "4024fea2-2c12-4343-bc20-9340d3add001", "entity_id": "37777", "timestamp": 1525720609}|{"f1": "I am entity B","f2": 4}
      |{"service_name": "test", "tenant_id": "1000", "identity_id": "4024fea2-2c12-4343-bc20-9340d3add001", "entity_id": "37788", "timestamp": 1525730609}|{"f1": "I am entity C","f2": 5}
    """.stripMargin.trim

  val topic = "test.avro.serde"

  "to_avro/from_avro" should "give correct results" in {
    import org.apache.spark.sql.avro.{SchemaConverters, from_avro, to_avro}
    import org.apache.spark.sql.DataFrame

    val input1 = Seq("foo", "bar", "baz").toDF("key")
    val input2 = input1.sqlContext.createDataFrame(input1.rdd, input1.schema)

    def test_avro(df: DataFrame): Unit = {
      println("input df:")
      df.printSchema()
      df.show()

      val keySchema = SchemaConverters.toAvroType(df.schema).toString
      println(s"avro schema: $keySchema")

      val avroDf = df
        .select(to_avro($"key") as "key")

      println("avro serialized:")
      avroDf.printSchema()
      avroDf.show()

      val output = avroDf
        .select(from_avro($"key", keySchema) as "key")
        .select("key.*")

      println("avro deserialized:")
      output.printSchema()
      output.show()
    }

    println("############### testing .toDF()")
    test_avro(input1)
    println("############### testing .createDataFrame()")
    test_avro(input2)
  }

  "DataFrameAvroHelpers" should
    "serialize and deserialize Avro, register and retrieve schemas from the Schema Registry." in {

    deleteSubject(topic + "-key")
    deleteSubject(topic + "-value")

    val df = testData
      .split('\n').toSeq
      .map(_.split('|'))
      .map { case Array(f1, f2) => (f1, f2) }
      .toDF("key", "value")
      .fromInferredJson("key")
      .expand("key")
      .fromInferredJson("value")
      .expand("value")
      .debugPrintSchema()
      .debugShow()

    val dfToAvroAndBack = df.sqlContext.createDataFrame(df.rdd, df.schema)
      .to_confluent_avro(
        avroSchemaRegistry,
        topic + "-key",
        "key",
        Some(Array("entity_id", "identity_id", "service_name", "tenant_id", "timestamp")),
        "TestKey",
        "com.jaumo"
      )
      .to_confluent_avro(
        avroSchemaRegistry,
        topic + "-value",
        "value",
        Some(Array("f1", "f2")),
        "TestValue",
        "com.jaumo"
      )
      .debugPrintSchema(Some("Avro Serialized"))
      .debugShow()
      .from_confluent_avro(
        "key",
        "key",
        avroSchemaRegistry,
        topic + "-key"
      )
      .from_confluent_avro(
        "value",
        "value",
        avroSchemaRegistry,
        topic + "-value"
      )
      .expand("key")
      .expand("value")
      .debugPrintSchema(Some("Avro Deserialized"))
      .debugShow()

    assertSmallDataFrameEquality(dfToAvroAndBack, df)
  }

  "DataFrameAvroHelpers" should
    "serialize and deserialize Avro, register and retrieve schemas from the Schema Registry" +
      "also when writing to/from Kafka." in {

    deleteSubject(topic + "-key")
    deleteSubject(topic + "-value")
    deleteTopic(topic)

    val df = testData
      .split('\n').toSeq
      .map(_.split('|'))
      .map { case Array(f1, f2) => (f1, f2) }
      .toDF("key", "value")
      .fromInferredJson("key")
      .expand("key")
      .fromInferredJson("value")
      .expand("value")
      .debugPrintSchema()
      .debugShow()

    df
      .to_confluent_avro(
        avroSchemaRegistry,
        topic + "-key",
        "key",
        Some(Array("entity_id", "identity_id", "service_name", "tenant_id", "timestamp")),
        "TestKey",
        "com.jaumo"
      )
      .to_confluent_avro(
        avroSchemaRegistry,
        topic + "-value",
        "value",
        Some(Array("f1", "f2")),
        "TestValue",
        "com.jaumo"
      )
      .debugPrintSchema(Some("Avro Serialized"))
      .debugShow()
      .write
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBroker)
      .option("topic", topic)
      .save()


    val dfToAvroAndBack = currentSparkSession
      .read
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBroker)
      .option("subscribe", topic)
      .option("startingOffsets", "earliest")
      .option("endingOffsets", "latest")
      .load()
      .from_confluent_avro(
        "key",
        "key",
        avroSchemaRegistry,
        topic + "-key"
      )
      .from_confluent_avro(
        "value",
        "value",
        avroSchemaRegistry,
        topic + "-value"
      )
      .select("key", "value")
      .expand("key")
      .expand("value")
      .sort($"f1")
      .debugPrintSchema(Some("Avro Deserialized"))
      .debugShow()

    assertSmallDataFrameEquality(dfToAvroAndBack, df)
  }
}
