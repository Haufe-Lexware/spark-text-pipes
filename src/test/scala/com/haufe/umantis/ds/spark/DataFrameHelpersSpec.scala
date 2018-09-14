package com.haufe.umantis.ds.spark

import com.haufe.umantis.ds.tests.SparkSpec
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

import scala.collection.mutable
import org.scalatest._
import Matchers._
import PartialFunctionValues._
import org.apache.spark.sql.catalyst.expressions.Literal

class DataFrameHelpersSpec extends SparkSpec {
  import currentSparkSession.implicits._

  val dfArray: DataFrame = Seq((Seq("a", "b"), Seq("c", "d"))).toDF("c1", "c2")
//  +----------------+
//  |            data|
//  +----------------+
//  |[[a, b], [c, d]]|
//  +----------------+
//
//  root
//  |-- data: struct (nullable = false)
//  |    |-- c1: array (nullable = true)
//  |    |    |-- element: string (containsNull = true)
//  |    |-- c2: array (nullable = true)
//  |    |    |-- element: string (containsNull = true)


  val dfInts: DataFrame = Seq((Some(4), "b"), (None, "d")).toDF("c1", "c2")
//  +----+---+
//  |  c1| c2|
//  +----+---+
//  |   4|  b|
//  |null|  d|
//  +----+---+
//
//  root
//  |-- c1: integer (nullable = true)
//  |-- c2: string (nullable = true)

  "DataFrameHelpers" must "flatten schema" in {
    val flattened = dfArray
      .select(struct($"c1", $"c2").as("data"))
      .flatten()

    assertSmallDataFrameEquality(flattened, dfArray)
  }

  it must "replace a column with its expansion" in {
    val expanded = dfArray
      .select(
        struct("c1", "c2").as("data"),
        $"c1".as("a"),
        $"c2".as("b")
      )
      .expand("data")

    assertSmallDataFrameEquality(
      expanded,
      dfArray.select($"c1", $"c2", $"c1".as("a"), $"c2".as("b"))
    )
  }

  it must "sanitize column names and replace column names given a Map" in {
    val dfWithBadColNames = dfArray
      .select($"c1", $"c1".as("c1.c1"), $"c2".as("c2.c2"))

    val inverseReplacements = mutable.Map[String, String]()
    val dfWithGoodColNames = dfWithBadColNames
      .sanitizeColumnNames("_", Some(inverseReplacements))

    inverseReplacements.valueAt("c1") should equal ("c1")
    inverseReplacements.valueAt("c1_c1") should equal ("c1.c1")
    inverseReplacements.valueAt("c2_c2") should equal ("c2.c2")

    dfWithGoodColNames.columns shouldBe Array("c1", "c1_c1", "c2_c2")

    val inverseReplacedCols = dfWithGoodColNames
      .withColumn("c3", $"c1") // let's add a new column
      .drop("c2_c2") // let's drop one of the replaced columns
      .renameCols(inverseReplacements)
      .columns

    inverseReplacedCols shouldBe Array("c1", "c1.c1", "c3")
  }

  it must "merge array columns" in {
    val merged = dfArray.mergeArrayColumns("merged", Seq("c1", "c2"), dropColumns = true)
    assertSmallDataFrameEquality(merged, Seq(Seq("a", "b", "c", "d")).toDF("merged"))


    val mergedAll = dfArray.mergeArrayColumns("merged", Seq("c1", "c2"))
    val expected = dfArray
      .withColumn("merged", lit(Literal.create(Seq("a", "b", "c", "d"))))
      .setNullableStateOfColumn("merged", nullable = true)
    assertSmallDataFrameEquality(mergedAll, expected)
  }
}
