package com.haufe.umantis.ds.nlp

import com.haufe.umantis.ds.tests.SparkSpec
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.functions._
import org.scalatest._
import Matchers._
import org.apache.spark.sql.catalyst.ScalaReflection.schemaFor


class LinearWeigherSpec extends SparkSpec {
  val c1 = ColnamesText("c1")
  val c2 = ColnamesText("c2")
  val columns = ColnamesAggregated("Score", Array(c1, c2))
  val lw: LinearWeigher = DsPipeline.getLinearWeigher(columns)

  "LinearWeigher transformer" should "dont count null value" in {
    import currentSparkSession.implicits._
    val data: DataFrame = Seq((10.0f))
      .toDF(c1.score)
      .withColumn(c2.score, lit(Literal.create(null, schemaFor[Float].dataType)))
      .withColumn(columns.linearWeights, lit(Literal.create(Array(0.5f, 0.1f))))
        .withColumn("array",array(col(c1.score),col(c2.score)))
    lw.transform(data).select(columns.outputCol).head().getFloat(0) shouldEqual (10f)
  }
  it should "compute with 0 value" in {
    import currentSparkSession.implicits._
    val data: DataFrame = Seq((10.0f, 0.0f))
      .toDF(c1.score, c2.score)
      .withColumn(columns.linearWeights, lit(Literal.create(Array(0.5f, 0.1f))))
    lw.transform(data).select(columns.outputCol).head().getFloat(0) shouldEqual ((10.0f * 0.5f ) / 0.6f)
  }

  it  should "compute" in {
    import currentSparkSession.implicits._
    val data: DataFrame = Seq((100.0f, 50.0f))
      .toDF(c1.score, c2.score)
      .withColumn(columns.linearWeights, lit(Literal.create(Array(0.5f, 0.1f))))
    lw.transform(data).select(columns.outputCol).head().getFloat(0) shouldEqual ((100.0f * 0.5f + 50.0f * 0.1f) / 0.6f)
  }


}
