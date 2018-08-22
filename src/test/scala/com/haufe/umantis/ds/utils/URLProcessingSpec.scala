package com.haufe.umantis.ds.utils

import com.haufe.umantis.ds.nlp.{ColnamesURL, DsPipeline, DsPipelineInput, StandardPipeline}
import com.haufe.umantis.ds.spark.{DataFrameHelpers, SparkSessionWrapper}
import com.haufe.umantis.ds.tests.SparkSpec
import org.apache.spark.sql.DataFrame


class URLProcessingSpec extends SparkSpec with URLProcessingSpecFixture with DataFrameHelpers {

  "URLValidator and URLExpander" should "validate and expand URLs" in {

    val pipeline = DsPipeline(DsPipelineInput(
      cols,
      StandardPipeline.URLProcessing
    )).pipeline

    val expandedDF = urls.transformWithPipeline(pipeline)

    assertSmallDataFrameEquality(expandedDF, expectedExpandedUrls)
  }
}


trait URLProcessingSpecFixture extends SparkSessionWrapper {
  import currentSparkSession.implicits._

  val cols = ColnamesURL("urls")

  val urls: DataFrame = Seq(
    (Seq("http://▀▄▀▄▀▄▀▄▀▄▀▄▀▄▀aiLemi", "https://bit.ly/1dNVPAW", "http://www.google.com/"))
  )
    .toDF(cols.urls)

  val expectedExpandedUrls: DataFrame = Seq(
    (
      (Seq("http://▀▄▀▄▀▄▀▄▀▄▀▄▀▄▀aiLemi", "https://bit.ly/1dNVPAW", "http://www.google.com/")),
      (Seq("https://bit.ly/1dNVPAW", "http://www.google.com/")),
      (Seq("http://www.google.com/", "http://www.google.com/"))
    )
  )
    .toDF(cols.urls, cols.validURLs, cols.expandedURLs)
}
