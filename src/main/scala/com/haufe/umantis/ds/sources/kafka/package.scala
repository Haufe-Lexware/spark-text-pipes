//package com.haufe.umantis.ds.sources
//
//import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
//import org.apache.spark.sql.avro.{AvroDataToCatalyst, CatalystDataToAvro, SchemaConverters}
//import org.apache.spark.sql.{Column, DataFrame}
//
//import scala.collection.JavaConverters._
//import scala.collection.mutable
//
//
//package object kafka {
//  private val cacheCapacity = 256
//
//  val avroRegistryClients: mutable.Map[String, CachedSchemaRegistryClient] =
//    mutable.Map[String, CachedSchemaRegistryClient]()
//
//  def getSchemaRegistry(schemaRegistryURLs: String): CachedSchemaRegistryClient =
//    avroRegistryClients
//      .getOrElseUpdate(
//        schemaRegistryURLs,
//        {
//          // Multiple URLs for HA mode.
//          val urls = schemaRegistryURLs.split(",").toList.asJava
//          new CachedSchemaRegistryClient(urls, cacheCapacity)
//        }
//      )
//
//  def from_confluent_avro(
//                           data: Column,
//                           schemaRegistryURLs: String,
//                           subject: String,
//                           version: String = "latest"
//                         )
//  : Column = {
//
//    val schemaRegistry = getSchemaRegistry(schemaRegistryURLs)
//
//    val schemaId =
//      if (version == "latest") {
//        schemaRegistry.getLatestSchemaMetadata(subject).getId
//      } else {
//        schemaRegistry.getSchemaMetadata(subject, version.toInt).getId
//      }
//
//    val schema = schemaRegistry.getByID(schemaId).toString
//
//    new Column(AvroDataToCatalyst(data.expr, schema))
//  }
//
//  def to_confluent_avro(
//                         df: DataFrame,
//                         data: Column,
//                         schemaRegistryURLs: String,
//                         subject: String
//                       )
//  : Column = {
//
//    val schemaRegistry = getSchemaRegistry(schemaRegistryURLs)
//    val schema = SchemaConverters.toAvroType(df.select(data).schema)
//    schemaRegistry.register(subject, schema)
//
//    new Column(CatalystDataToAvro(data.expr))
//  }
//}
