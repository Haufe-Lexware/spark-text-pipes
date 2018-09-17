package com.haufe.umantis.ds.sources.kafka

import org.apache.spark.sql.{AnalysisException, DataFrame}
import org.apache.spark.sql.streaming.StreamingQuery


/**
  * This trait provides logic to get fresh data in DataFrame format
  */
trait SinkData extends Source {
  var dataFrame: Option[DataFrame] = None

  var lastTimestamp: Long = 0

  var sink: Option[StreamingQuery] = None

  val conf: TopicConf

  def log(message: String): Unit = {
    println(s"${conf.kafkaTopic.topic}: $message")
  }

  private var readOnly: Boolean = false

  def isReadOnly: Boolean = readOnly

  def readOnly(value: Boolean): this.type = {
    readOnly = value
    this
  }

  /**
    * Reads the DataFrame from the persistent source (Parquet, Kafka, etc.)
    * @return A fresh copy of the DataFrame
    */
  def doUpdateDf(): DataFrame

  /**
    * @return A fresh or cached copy of the data in DataFrame format
    */
  def data: DataFrame = {

    def getTimestamp: Long = System.currentTimeMillis / 1000

    def updateDf(): DataFrame = {
      log("Updating DataFrame")
      //      Thread.sleep(100)

      sink match {
        case Some(_) =>
          (1 to 30).foreach(retryNr => {
            log(s"Reading Fresh Data Try # $retryNr")

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

    sink match {
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
      case _ =>
        log("No sink")
    }

    dataFrame match {
      case None =>
        log("No DataFrame")
        //        Thread.sleep(100)

        if (! isReadOnly)
          sink match {
            case Some(s) =>
            // sink is defined but df is not available, so we process some data
            // s.processAllAvailable()

            case _ =>
              // sink is not defined. we first create it;
              start()
            // then check it out again
            //              sink match {
            //                case Some(s) => s.processAllAvailable() // defined, all good
            //                case _ => ; // not defined, kafka topic does not exist, updateDf will fail
            //              }
          }
        lastTimestamp = getTimestamp
        updateDf()

      case Some(df) =>
        log("DataFrame defined")
        //        Thread.sleep(100)

        val now = getTimestamp
        log(s"Difference in timestamps: ${now - lastTimestamp}" )
        if (now - lastTimestamp < conf.sinkConf.refreshTime /* in seconds */)
        // The df is fresh enough to be served
          df
        else {
          lastTimestamp = now
          updateDf()
        }
    }
  }

  /**
    * @return An always fresh copy of the data in DataFrame format
    */
  def freshData: DataFrame = {
    lastTimestamp = 0
    data
  }
}
