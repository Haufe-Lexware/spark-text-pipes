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

import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType
import play.api.libs.json.Json

import scala.util.Try


/** A Kafka Data Source.
  *
  * It creates a Streaming DataFrame (using Spark Structured Streaming) from
  * a Kafka topic. It deserializes the topic value.
  * Then, it applies a transformation function to the streaming DataFrame. Finally, a
  * Then, it creates a StreamingQuery to sink the Streaming DataFrame to a parquet file.
  * This class provides a method to always return a fresh copy of the processed data.
  * This copy is refreshed every refreshTime seconds.
  *
  */
class TopicSourceParquetSink(
                             conf: TopicConf
                            )
extends TopicSourceSink(conf)
{
  private var startingOffset: String = "latest"

  var outputSchema: StructType = _

  private def checkParquetOnly(f: () => Unit): this.type = {
    if (isReadOnly) {
      log("This call has no effect if readOnly is set!")
    } else {
      f()
    }
    this
  }

  var sinkProcessingThread: Option[Thread] = None

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
          val sourceDf = getSource("earliest")//startingOffset)

          outputSchema = sourceDf.schema

          val s = sourceDf
            .writeStream
            .outputMode("append")
            .option("checkpointLocation", conf.filePathCheckpoint)
            .format("parquet")
            .trigger(conf.kafkaTopic.trigger)
            .start(conf.filePath)

          val t = new Thread {
            override def run(): Unit = s.awaitTermination()
          }
          t.start()
          sinkProcessingThread = Some(t)

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
          s.stop()
          sinkProcessingThread match {
            case Some(t) =>
              println("waiting")
              try {
                while (t.isAlive) {
                  t.wait()
                  Thread.sleep(10)
                }
              } catch {
                case _: IllegalMonitorStateException => ;
              }

            case _ =>
              println("not waiting")
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

  def postProcessDf(df: DataFrame): DataFrame = {
    df
  }

  def doUpdateDf(): DataFrame = {
    val fname = s"$dataRoot${conf.fileNameLeaf}.parquet"

    val diskDf = if (conf.sinkConf.useSqlToRead) {
      currentSparkSession
        .sql(s"select * from parquet.`$fname`")
        .toDF()
    } else {
      val df = currentSparkSession
        .read
        .schema(outputSchema)
        .format("parquet")
        .load(fname)

      // if the df is empty (because no data has been written yet)
      // we want to trigger an exception (caught in SinkData.updateDf()
      // so that we can retry to read the df
      df.head()

      df
    }
      .repartition(conf.sinkConf.numPartitions)

    val newDataFrame = postProcessDf(diskDf)
      .cache()

    dataFrame = Some(newDataFrame)
    newDataFrame
  }

  /**
    * @return Pretty String describing the Avro schema of the associated topic.
    */
  def schema: Option[String] = {
    kafkaSerializer.schemaRegistry match {
      case Some(schemaRegistry) =>
        Try(Some(Json.prettyPrint(Json.parse(
          kafkaSerializer.getSchema(schemaRegistry, conf.subjectValueName, "latest")
        )))).getOrElse(None)
      case _ => None
    }
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
