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

import com.haufe.umantis.ds.spark.{SparkSessionWrapper, UnaryUDFTransformer}
import com.haufe.umantis.ds.tests.SparkSpec
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.functions.{col, udf}
import org.scalatest.Matchers._

class NormalizedBagOfWordsTransformerSpec extends SparkSpec
with NormalizedBagOfWordsTransformerSpecFixture {
  import currentSparkSession.implicits._

  val normalizer: UnaryUDFTransformer[Seq[String], Seq[(String, Int)]] =
    DsPipeline.getNormalizedBagOfWordsTransformer(cols)

  "NormalizedBagOfWordsTransformer" should "create nBOW" in {
    val f = udf {d: Seq[GenericRowWithSchema] =>
      d.map(p => (p.getString(0), p.getInt(1))).sortWith(_._1 < _._1)}

    val result = normalizer
      .transform(inputDf)
      .withColumn(cols.nBOW, f(col(cols.nBOW)))

    val expected = Seq((sentence, expectedResult))
      .toDF(cols.tokens, cols.nBOW)

    assertSmallDataFrameEquality(result, expected)
  }

  it should "transform map" in {
    val input = Seq(Map(cols.tokens -> sentence))

    val result = normalizer.transform(input)
    val expected = Seq(Map(
      cols.tokens -> sentence,
      cols.nBOW -> expectedResult
    ))

    result.map(
      _.map {
        case (s, n: Seq[(String, Int)]) if s == cols.nBOW => (s, n.sortWith(_._1 < _._1))
        case p => p
      }
    ) shouldBe expected
  }
}

trait NormalizedBagOfWordsTransformerSpecFixture extends SparkSessionWrapper {
  import currentSparkSession.implicits._

  val cols = new ColnamesText("text")

  val sentence: Seq[String] =
    "president greet press chicago president go chicago".split(" ")

  val expectedResult =
    Seq(("chicago", 2), ("go", 1), ("greet", 1), ("president", 2), ("press", 1))

  val inputDf: DataFrame = Seq(sentence).toDF(cols.tokens)
}
