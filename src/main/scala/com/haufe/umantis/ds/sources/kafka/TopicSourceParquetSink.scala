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

import com.databricks.spark.avro.ConfluentSparkAvroUtils
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException
import org.apache.spark.sql.streaming.StreamingQueryException
import org.apache.spark.sql.{AnalysisException, DataFrame}

import scala.collection.mutable
import scala.util.Try


/** A Kafka Data Source.
  *
  * It creates a Streaming DataFrame (using Spark Structured Streaming) from
  * a Kafka topic. It deserializes the topic value using an instance of
  * [[com.databricks.spark.avro.ConfluentSparkAvroUtils]].
  * Then, it applies a transformation function to the streaming DataFrame. Finally, a
  * Then, it creates a StreamingQuery to sink the Streaming DataFrame to a parquet file.
  * This class provides a method to always return a fresh copy of the processed data.
  * This copy is refreshed every refreshTime seconds.
  *
  */
class TopicSourceParquetSink(
                             conf: TopicConf
                            )
extends TopicSource(conf)
{
  var dataFrame: Option[DataFrame] = None

  var lastTimestamp: Long = 0

  var lastSinkTimestamp: String = null

  private var startingOffset: String = "latest"

  private var readOnly: Boolean = false

  def isReadOnly: Boolean = readOnly

  def readOnly(value: Boolean): this.type = {
    readOnly = value
    this
  }

  private def checkParquetOnly(f: () => Unit): this.type = {
    if (isReadOnly) {
      log("This call has no effect if readOnly is set!")
    } else {
      f()
    }
    this
  }

  /**
    * Start the processing of this topic
    */
  def start(): this.type = {
    checkParquetOnly(() => {

      sink match {
        case Some(s) => s.stop()
        case _ => ;
      }

      sink =
        try {
          val s = getSource("earliest")//startingOffset)
            .writeStream
            .outputMode("append")
            .option("checkpointLocation", conf.filePathCheckpoint)
            .format("parquet")
            .trigger(conf.kafkaTopic.trigger)
            .start(conf.filePath)

          Some(s)
        } catch {
          case _: RestClientException =>
            // e.printStackTrace()
            log(s"TOPIC NOT FOUND IN KAFKA!")
            None
        }

    })
  }

  /**
    * Stops the processing of this topic
    */
  def stop(): this.type = {
    checkParquetOnly(() => {

      sink match {
        case Some(s) =>
          try {
            s.stop()
            s.awaitTermination()
          } catch {
            case _: StreamingQueryException =>
          }

          sink = None
          dataFrame = None
        case _ => ;
      }

    })
  }

  /**
    * Deletes the output parquet file of this topic
    */
  def delete(): this.type = {
    checkParquetOnly(() => {

      stop()
      deleteDataFrame(conf.fileNameLeaf)
      deleteDataFrame(conf.fileNameLeafCheckpoint)

    })
  }

  /**
    * Reset the processing of this topic: stop, delete, start
    */
  def reset(): this.type = {
    checkParquetOnly(() => {

      stop()
      delete()
      startingOffset = "earliest"
      start()
      startingOffset = "latest"

    })
  }

  /**
    * Returns a Map of the current DataFrame size. It returns "fail" if not available.
    * @return The status
    */
  def status(): Map[String, String] = {
    Map[String, String](
      conf.kafkaTopic.topic -> Try(data.count().toString).getOrElse("fail")
    )
  }

  def postProcessParquet(df: DataFrame): DataFrame = {
    df
  }

  /**
    * @return An always fresh copy of the data in DataFrame format
    */
  def freshData: DataFrame = {
    lastTimestamp = 0
    data
  }

  /**
    * @return A fresh or cached copy of the data in DataFrame format
    */
  def data: DataFrame = {

    def getTimestamp: Long = System.currentTimeMillis / 1000

    def doUpdateDf(): DataFrame = {
      val fname = s"$dataRoot${conf.fileNameLeaf}.parquet"

      val diskDf = currentSparkSession
        .sql(s"select * from parquet.`$fname`")
        .toDF()
        .repartition(conf.parquetSink.numPartitions)

      val newDataFrame = postProcessParquet(diskDf)
        .cache()

      dataFrame = Some(newDataFrame)
      newDataFrame
    }

    def updateDf(): DataFrame = {
      log("Updating DataFrame")
//      Thread.sleep(100)

      sink match {
        case Some(_) =>
          (1 to 30).foreach(retryNr => {
            log(s"Reading Parquet try # $retryNr")

            try{
              return doUpdateDf()
            } catch {
              case _: AnalysisException =>
                Thread.sleep(1000)
            }
          })
        case _ =>
          // in case isReadOnly == true
          return doUpdateDf()
      }

      throw new KafkaTopicNotAvailableException(
        s"Kafka topic ${conf.kafkaTopic.topic} not ready!")
    }

    val sinkTimestamp = sink match {
      case Some(s) =>
        log("Sink defined")
        if (! s.isActive) {
          // This means that the sink exists but it's not active.
          // Therefore, some sort of error occurred (e.g. kafka topic was reset).
          // Thus, we reset the TopicSource.
          log("Sink reset")
//          Thread.sleep(100)
          reset()
        }
        Option(s.lastProgress).map(_.timestamp).getOrElse(lastSinkTimestamp)
      case _ =>
        log("No sink")
        lastSinkTimestamp
    }

    dataFrame match {
      case None =>
        log("No DataFrame")

        if (! isReadOnly)
          sink match {
            case Some(s) =>
            case _ =>
              start()
          }
        lastTimestamp = getTimestamp
        lastSinkTimestamp = sinkTimestamp
        updateDf()

      case Some(df) =>
        log("DataFrame defined")

        val now = getTimestamp
        log(s"Difference in timestamps: ${now - lastTimestamp}" )
        if (lastSinkTimestamp == sinkTimestamp  /* nothing come in kafka topic */
            || now - lastTimestamp < conf.parquetSink.refreshTime /* in seconds */)
          // The df is fresh enough to be served
          df
        else {
          lastTimestamp = now
          updateDf()
        }
    }
  }

  /**
    * @return Pretty String describing the Avro schema of the associated topic.
    */
  def schema: Option[String] = {
    kafkaSerializer.avroUtils match {
      case Some(utils) =>
        Try(Some(utils.getAvroSchemaForSubjectPretty(conf.subjectValueName))).getOrElse(None)
      case _ => None
    }
  }

  def log(message: String): Unit = {
    println(s"${conf.kafkaTopic.topic}: $message")
  }
}

/**
  * Factory for [[TopicSourceParquetSink]].
  */
object TopicSourceParquetSink {

  val avroRegistries: mutable.Map[String, ConfluentSparkAvroUtils] =
    mutable.Map[String, ConfluentSparkAvroUtils]()

  def getAvroUtils(schemaRegistryURL: String): ConfluentSparkAvroUtils = {
    avroRegistries
      .getOrElseUpdate(
        schemaRegistryURL,
        new ConfluentSparkAvroUtils(schemaRegistryURL)
      )
  }
}



/**
  * Exception thrown if a Kafka topic is not available.
  * @param message The message of the exception.
  */
class KafkaTopicNotAvailableException(message: String) extends Exception(message) {

  def this(message: String, cause: Throwable) {
    this(message)
    initCause(cause)
  }

  def this(cause: Throwable) {
    this(Option(cause).map(_.toString).orNull, cause)
  }

  def this() {
    this(null: String)
  }
}
