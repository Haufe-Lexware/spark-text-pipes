package com.haufe.umantis.ds.nlp

import com.haufe.umantis.ds.tests.SparkSpec
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.functions.lit
import org.scalatest._
import Matchers._


class LinearWeigherSpec extends SparkSpec {
  val c1 = ColnamesText("c1")
  val c2 = ColnamesText("c2")
  val columns  = ColnamesAggregated("Score",Array(c1,c2))
  val lw = DsPipeline.getLinearWeigher(columns)

  "LinearWeigher transformer" should "dont count null value" in {
    import currentSparkSession.implicits._
    val data:DataFrame = Seq((10.0f,null))
      .toDF(c1.score,c2.score)
      .withColumn(columns.linearWeights,lit(Literal.create(Array(0.5f,0.1f))))
    lw.transform(data).select(columns.outputCol).head().getFloat(0) shouldEqual(5.0f +- 0.1f)
  }

  "LinearWeigher transformer" should "compute" in {
    import currentSparkSession.implicits._
    val data:DataFrame = Seq((10.0f,5.0f))
      .toDF(c1.score,c2.score)
      .withColumn(columns.linearWeights,lit(Literal.create(Array(0.5f,0.1f))))
    lw.transform(data).select(columns.outputCol).head().getFloat(0) shouldEqual((10.0*0.5+5.0*0.1)/0.6f +- 0.1f)
  }


}
