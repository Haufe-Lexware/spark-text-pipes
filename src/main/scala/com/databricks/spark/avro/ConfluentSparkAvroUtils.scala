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

// This code has been adapted from
// https://github.com/Tubular/confluent-spark-avro

/*
 * We have to use "com.databricks.spark.avro" because "SchemaConverters.toSqlType"
 * is only visible in the scope of the package.
 */
package com.databricks.spark.avro

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import org.apache.avro.Schema
import org.apache.commons.logging.LogFactory
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.DataType

import scala.collection.JavaConverters._
import scalaz.Memo
import play.api.libs.json.Json

import scala.util.Try

/**
  * The class is used to generate deserialization UDFs for Avro messages in Kafka
  * powered by Schema Registry.
  *
  * Usages:
  *   val utils = new ConfluentSparkAvroUtils("http://schema-registry.my.company.com:8081")
  *   val sparkUDF = utils.deserializerForSubject("my-awesome-subject-in-schema-registry")
  *
  * Notes:
  *   All attributes that have to be created from scratch on the executor side
  *   are marked with "@transient lazy".
  */
class ConfluentSparkAvroUtils(schemaRegistryURLs: String) extends Serializable {

  @transient private lazy val logger = LogFactory.getLog(this.getClass)

  @transient private lazy val schemaRegistry = {
    // Multiple URLs for HA mode.
    val urls = schemaRegistryURLs.split(",").toList.asJava
    // Store up to 128 schemas.
    val cacheCapacity = 128

    logger.info("Connecting to schema registry server at " + schemaRegistryURLs)

    new CachedSchemaRegistryClient(urls, cacheCapacity)
  }

  @transient private lazy val deserializer = new KafkaAvroDeserializer(schemaRegistry)

  def deserializerForSubject(subject: String, version: String): UserDefinedFunction = {
    udf(
      (payload: Array[Byte]) => {
        val data: Array[Byte] = payload

        val avroSchema = avroSchemaForSubject(subject, version)
        val dataType = dataTypeForSubject(subject, version)

        val obj = Try(deserializer.deserialize("", data, avroSchema)).getOrElse(null)
        val toSqlRow = sqlConverter(avroSchema, dataType)

        toSqlRow(obj).asInstanceOf[GenericRow]
      },
      dataTypeForSubject(subject, version)
    )
  }

  def deserializerForSubject(subject: String): UserDefinedFunction =
    deserializerForSubject(subject, "latest")

  def getAvroSchemaForSubject(subject: String, version: String = "latest"): Schema = {
    avroSchemaForSubject(subject, version)
  }

  def getAvroSchemaForSubjectPretty(subject: String, version: String = "latest"): String = {
    val schema = avroSchemaForSubject(subject, version).toString()
    Json.prettyPrint(Json.parse(schema))
  }

  @transient private lazy val avroSchemaForSubject: ((String, String)) => Schema =
    Memo.mutableHashMapMemo {
      subjectAndVersion =>
        var schemaId: Int = -1

        if (subjectAndVersion._2 == "latest") {
          schemaId = schemaRegistry
            .getLatestSchemaMetadata(subjectAndVersion._1)
            .getId
        } else {
          schemaId = schemaRegistry
            .getSchemaMetadata(subjectAndVersion._1, subjectAndVersion._2.toInt)
            .getId
        }

        logger.info("Resolving avro schema for subject " + subjectAndVersion._1 +
          " with version " + subjectAndVersion._2)

        schemaRegistry.getByID(schemaId)
    }

  @transient private lazy val dataTypeForSubject: ((String, String)) => DataType =
    Memo.mutableHashMapMemo {
      subjectAndVersion =>
        SchemaConverters
          .toSqlType(avroSchemaForSubject(subjectAndVersion._1, subjectAndVersion._2)).dataType
    }

  @transient private lazy val sqlConverter: ((Schema, DataType)) => Function[AnyRef, AnyRef] =
    Memo.mutableHashMapMemo {
      schemaAndType =>
        SchemaConverters.createConverterToSQL(schemaAndType._1, schemaAndType._2)
    }
}