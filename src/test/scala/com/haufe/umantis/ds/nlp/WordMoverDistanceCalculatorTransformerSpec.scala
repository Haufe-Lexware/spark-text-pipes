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

import com.haufe.umantis.ds.tests.SparkSpec
import com.haufe.umantis.ds.wmd._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.functions._
import org.scalatest.Matchers._

class WordMoverDistanceCalculatorTransformerSpec extends SparkSpec {

  import currentSparkSession.implicits._

  val colsVarying = new ColnamesText("varying_text")
  val colsFixed = new ColnamesText("fixed_text")

  val similarityCols = ColnamesTextSimilarity(colsFixed, colsVarying)

  def cbow(sentence: String): Seq[(String, Int)] = {
    sentence
      .split(" ")
      .groupBy(identity)
      .map{ case (word, coll) => word -> coll.length }
      .toSeq
  }

  val input: DataFrame = Seq(
    (1, cbow("obama speaks media illinois"), "en"),
    (2, cbow("band gave concert japan"), "en"),
    (3, cbow("obama speaks illinois"), "en"))
      .toDF("id", colsVarying.nBOW, colsVarying.language)

  val wmd: WordMoverDistanceCalculatorTransformer = DsPipeline.getWordMoverDistance(similarityCols)

  "WordMoverDistance transformer" should "provide correct transformations" in {
    val inputDf = input
      .withColumn(similarityCols.baseText.language, lit("en"))
      .withColumn(similarityCols.baseText.nBOW,
        lit(Literal.create(cbow("president greets press chicago"))))
      .repartition(1)

    def doTest(name: String): DataFrame = {
      println(name)
      val res = wmd.transform(inputDf)
//        .select("id", cols.documentDistance)
        .sort(similarityCols.similarity)

      res.show(20, 200)
      res
    }

    wmd.setCalculationType(CosineDistance)
    doTest("Cosine")

    wmd.setCalculationType(CosineDistanceNormalized)
    doTest("CosineNormalized")

    wmd.setCalculationType(TanimotoDistance)
    doTest("Tanimoto")

    wmd.setCalculationType(EuclideanDistance)
    doTest("Euclidean")

    wmd.setCalculationType(EuclideanDistanceNormalized)
    val transformedDf = doTest("EuclideanNormalized")

    transformedDf.show(20, 200)

    val res = transformedDf
      .select("id")
      .collect()
      .map(_.getInt(0))

    //result based on paper http://proceedings.mlr.press/v37/kusnerb15.pdf
    res shouldBe Seq(1,3,2)
  }
}
