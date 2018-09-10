package com.haufe.umantis.ds.utils

import com.haufe.umantis.ds.nlp.{ColnamesURL, DsPipeline, DsPipelineInput, StandardPipeline}
import com.haufe.umantis.ds.spark.{DataFrameHelpers, SparkSessionWrapper}
import com.haufe.umantis.ds.tests.SparkSpec
import org.apache.spark.sql.DataFrame


class URLProcessingSpec extends SparkSpec with URLProcessingSpecFixture with DataFrameHelpers {

  "URLProcessing" should "detect, validate and expand URLs" in {

    val pipeline = DsPipeline(DsPipelineInput(
      cols,
      StandardPipeline.URLProcessing
    )).pipeline

    val expandedDF = urls.transformWithPipeline(pipeline)
//    expandedDF.show(10, 200)
//    expectedExpandedUrls.show(10, 200)

    assertSmallDataFrameEquality(expandedDF, expectedExpandedUrls)
  }
}


trait URLProcessingSpecFixture extends SparkSessionWrapper with DataFrameHelpers {
  import currentSparkSession.implicits._

  val cols = ColnamesURL("urls")

  val urls: DataFrame = Seq(
    "http://▀▄▀▄▀▄▀▄▀▄▀▄▀▄▀aiLemi https://bit.ly/1dNVPAW http://www.google.com/"
  )
    .toDF(cols.text)

  val expectedExpandedUrls: DataFrame = Seq(
    (
      "http://▀▄▀▄▀▄▀▄▀▄▀▄▀▄▀aiLemi https://bit.ly/1dNVPAW http://www.google.com/",
      "http://▀▄▀▄▀▄▀▄▀▄▀▄▀▄▀aiLemi https://bit.ly/1dNVPAW http://www.google.com/",
      Seq("https://bit.ly/1dNVPAW", "http://www.google.com/"),
      Seq(CheckedURL("https://bit.ly/1dNVPAW", "http://www.google.com/", true, 1),
        CheckedURL("http://www.google.com/", "http://www.google.com/", true, 0))
    )
  )
    .toDF(cols.text, cols.textAllLatin, cols.urls, cols.expandedURLs)
    .setNullableStateOfColumn(cols.expandedURLs, false)
}
