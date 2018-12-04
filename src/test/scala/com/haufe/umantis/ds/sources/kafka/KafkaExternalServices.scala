package com.haufe.umantis.ds.sources.kafka

import com.haufe.umantis.ds.spark.SparkIO
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException
import org.apache.kafka.clients.admin.AdminClient

import scala.util.Try

trait KafkaExternalServices extends SparkIO {
  val schemaRegistryClient =
    new CachedSchemaRegistryClient(avroSchemaRegistry, 256)

  val adminClient: AdminClient = {
    val props = new java.util.Properties()
    props.setProperty("bootstrap.servers", kafkaBroker)
    org.apache.kafka.clients.admin.AdminClient.create(props)
  }

  /** Delete a Kafka topic and wait until it is propagated to the whole cluster */
  def deleteTopic(topic: String): Unit = {
    import collection.JavaConverters._
    Try(adminClient.deleteTopics(List(topic).asJavaCollection).all().get())
  }

  def deleteSubject(subject: String): Unit = {
    try {
      val versions = schemaRegistryClient.deleteSubject(subject)
      println(s"$subject versions deleted $versions")
    } catch {
      case e: RestClientException => e.getMessage()
    }
  }
}
