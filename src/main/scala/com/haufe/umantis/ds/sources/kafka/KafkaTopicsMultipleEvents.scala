package com.haufe.umantis.ds.sources.kafka

import java.nio.ByteBuffer

import com.haufe.umantis.ds.sources.kafka.serde.{DataFrameAvroHelpers, SchemaRegistryHelper}
import com.haufe.umantis.ds.spark.{DataFrameHelpers, SparkIO}
import io.confluent.kafka.schemaregistry.client.SchemaMetadata
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.streaming.{StreamingQuery, StreamingQueryListener, Trigger}
import org.apache.spark.sql.streaming.StreamingQueryListener.{QueryProgressEvent, QueryStartedEvent, QueryTerminatedEvent}

case class EventMetadata(latestVersion: Int, latestSchema: String, schemaIDs: List[Int])

case class KafkaHdfsBridge(
                            topic: String,
                            event: String,
                            filePath: String,
                            checkpointFilePath: String,
                            streamingQuery: StreamingQuery
                          )


class KafkaTopicsMultipleEvents(
                                 val schemaRegistryURL: String,
                                 val kafkaURL: String
                               )
  extends SparkIO
    with DataFrameHelpers with DataFrameAvroHelpers {

  import currentSparkSession.implicits._

  import scala.collection.JavaConverters._

  type Topic = String
  type Event = String

  val schemas: Map[Topic, Map[Event, EventMetadata]] = {
    val schemaRegistry = SchemaRegistryHelper.getSchemaRegistry(schemaRegistryURL)

    schemaRegistry
      .getAllSubjects.asScala
      .filter(_.startsWith("domain."))
      .flatMap(subject =>
        subject.split("-") match {
          case Array(topic, event) => Some(topic -> event)
          case _ => None
        }
      )
      .groupBy { case (topic, _) => topic }
      .map { case (topic: String, list: List[(String, String)]) =>
        topic -> list.map { case (_, event) => event }
      }
      .map { case (topic, events) =>
        topic -> events
          .flatMap(event => {
            if (event.endsWith(".DefaultKey") || event.endsWith(".Key")) {
              None
            } else {
              val subject = Array(topic, event).mkString("-")
              val versions = schemaRegistry.getAllVersions(subject).asScala
              Some(
                event -> versions
                  .map(version => {
                    schemaRegistry.getSchemaMetadata(subject, version)
                  }).toList
              )
            }
          })
          .map { case (event, schemaMetadatas: List[SchemaMetadata]) =>
            event -> schemaMetadatas
              .foldLeft(EventMetadata(-1, "", List())) {
                case (eventMetadata: EventMetadata, schemaMetadata: SchemaMetadata) =>
                  schemaMetadata.getVersion match {
                    case version if version > eventMetadata.latestVersion => EventMetadata(
                      schemaMetadata.getVersion,
                      schemaMetadata.getSchema,
                      eventMetadata.schemaIDs :+ schemaMetadata.getId
                    )
                    case _ => EventMetadata(
                      eventMetadata.latestVersion,
                      eventMetadata.latestSchema,
                      eventMetadata.schemaIDs :+ schemaMetadata.getId
                    )
                  }
              }
          }
          .toMap
      }
  }

  val schemaID: UserDefinedFunction =
    udf((value: Array[Byte]) => ByteBuffer.wrap(value.slice(1, 5)).getInt)

  def getRaw(topic: String): DataFrame = {
    currentSparkSession
      .read
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaURL)
      .option("startingOffsets", "earliest")
      .option("subscribe", topic)
      .load()
      .filter("value is not null")
      .withColumn("schemaID", schemaID(col("value")))
      .select("schemaID", "value")
      .cache()
  }

  def getDFs(topic: String, rawDf: Option[DataFrame] = None): Map[Event, DataFrame] = {
    val raw = rawDf match {
      case Some(df) => df
      case _ => getRaw(topic)
    }

    val dfs = schemas(topic)
      .map { case (event, metadata: EventMetadata) =>
        val eventName = event.split('.').last
        eventName -> raw
          .filter($"schemaID".isin(metadata.schemaIDs: _*))
          .from_confluent_avro("value", "value", metadata.latestSchema)
          .select("value.*")
          .cache()
      }

    if (rawDf.isEmpty) raw.unpersist()

    dfs
  }

  def getSource(topic: String): DataFrame = {
    currentSparkSession
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaURL)
      .option("startingOffsets", "earliest")
      .option("subscribe", topic)
      .load()
      .filter("value is not null")
      .withColumn("schemaID", schemaID(col("value")))
      .select("schemaID", "value")
  }

  def proc(topic: String, hdfsBase: String, intervalMs: Int = 0): Map[Event, KafkaHdfsBridge] = {
    val source = getSource(topic)

    schemas(topic)
      .map { case (event, metadata: EventMetadata) =>

        val eventName = event.split('.').last

        val filePath = s"$hdfsBase/$topic/$eventName"
        val checkpointFilePath = s"$filePath-CHECKPOINT"

        val query: StreamingQuery = source
          .filter($"schemaID".isin(metadata.schemaIDs: _*))
          .from_confluent_avro("value", "value", metadata.latestSchema)
          .select("value.*")
          .writeStream
          .outputMode("append")
          .option("checkpointLocation", checkpointFilePath)
          .format("parquet")
          .trigger(Trigger.ProcessingTime(1))
          .start(filePath)

        event -> KafkaHdfsBridge(
          topic = topic,
          event = eventName,
          filePath = filePath,
          checkpointFilePath = checkpointFilePath,
          streamingQuery = query
        )
      }
  }

  def installListeners(): Unit = {
    currentSparkSession.streams.addListener(new StreamingQueryListener() {
      override def onQueryStarted(queryStarted: QueryStartedEvent): Unit = {
        println("Query started: " + queryStarted.id)
      }
      override def onQueryTerminated(
                                      queryTerminated: QueryTerminatedEvent): Unit = {
        println("Query terminated: " + queryTerminated.id)
      }
      override def onQueryProgress(queryProgress: QueryProgressEvent): Unit = {
        println("Query made progress: " + queryProgress.progress)
      }
    })
  }
}
