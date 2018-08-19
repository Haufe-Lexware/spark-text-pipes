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

package com.haufe.umantis.ds.nlp

import com.haufe.umantis.ds.spark.{DataFrameHelpers, SparkSessionWrapper}
import com.haufe.umantis.ds.tests.SparkSpec
import org.apache.spark.sql.DataFrame

class ColumnsAggregatorSpec extends SparkSpec
  with ColumnsAggregatorSpecFixture with Serializable {

  "ColumnsAggregator" must "aggregate columns correctly" in {

    val result = inputdf
      .transformWithPipeline(
        new DsPipeline(Seq(
          DsPipelineInput(
            Seq(colsAggregatedMax),
            Seq(Stg.Max)
          ),
          DsPipelineInput(
            Seq(colsAggregatedMin),
            Seq(Stg.Min)
          ),
          DsPipelineInput(
            Seq(colsAggregatedSum),
            Seq(Stg.Sum)
          ),
          DsPipelineInput(
            Seq(colsAggregatedMean),
            Seq(Stg.Mean)
          )
        )).pipeline
      )

    result.show(false)

    assertSmallDataFrameEquality(
      result.select("max", "min", "sum", "mean"),
      expectedResult
    )
  }
}

trait ColumnsAggregatorSpecFixture extends SparkSessionWrapper with DataFrameHelpers {
  import currentSparkSession.implicits._

  val cols: Array[ColnamesText] = (1 to 3).map(x => ColnamesText(s"col$x")).toArray

  val inputdf: DataFrame = Seq(
    (1f, 2f, 3f),
    (7f, 12f, 33f)
  )
    .toDF(cols.map(_.score):_*)

  val colsAggregatedMax = ColnamesAggregated("max", cols)
  val colsAggregatedMin = ColnamesAggregated("min", cols)
  val colsAggregatedSum = ColnamesAggregated("sum", cols)
  val colsAggregatedMean = ColnamesAggregated("mean", cols)

  val expectedResult: DataFrame = Seq(
    (3f, 1f, 6f, 2f),
    (33f, 7f, 52f, 17.333334f)
  )
    .toDF("max", "min", "sum", "mean")
}
