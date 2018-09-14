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
import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.functions.{monotonically_increasing_id, _}
import org.scalatest.Matchers._

class TextDataScoringCentroidSpec extends SparkSpec
  with TextDataScoringCentroidSpecFixture with Serializable {

  "TextDataScoringCentroid" must "score vectors well" in {

    def getVector(word: String, language: String): DenseVector = {
      model.getVector(language, word) match {
        case Some(vec) => new DenseVector(vec.map(_.toDouble))
        case _ => new DenseVector(Array.fill[Double](300)(0))
      }
    }

    def getResult(df: DataFrame, position: Int): String = {
      df.take(position + 1)(position).getAs[Seq[String]](textCols.cleanWords).head
    }

    val pipeline: PipelineExtended =
      DsPipeline(DsPipelineInput(
        similarityCols,
        StandardPipeline.TextDataScoringCentroid
      )).pipeline

    val lorry = getVector("lorry", "en")

    val deskInGerman = getVector("schreibtisch", "de")

    // testing for query word == "lorry"
    val dfLorry = df
      .withColumn(similarityCols.baseText.vector, lit(Literal.create(lorry)))
      .withColumn(similarityCols.baseText.language, lit("en"))

    val resultForLorry = dfLorry.transformWithPipeline(pipeline)
      .sort(col(similarityCols.similarity).desc)

    resultForLorry.show(20, 50)

    val mostSimilarToLorry = getResult(resultForLorry, 0)
    mostSimilarToLorry shouldBe "truck"

    val leastSimilarToLorry = getResult(resultForLorry, resultForLorry.count.toInt - 1)
    leastSimilarToLorry shouldBe "desk"

    // testing for query word == "Schreibtisch" (desk in German)
    val dfDesk = df
      .withColumn(similarityCols.baseText.vector, lit(Literal.create(deskInGerman)))
      .withColumn(similarityCols.baseText.language, lit("de"))

    val resultForDesk = dfDesk.transformWithPipeline(pipeline)
      .sort(col(similarityCols.similarity).desc)

    resultForDesk.show(20, 50)

    val mostSimilarToDesk = getResult(resultForDesk, 0)
    mostSimilarToDesk shouldBe "desk"
  }
}

trait TextDataScoringCentroidSpecFixture extends SparkSessionWrapper with DataFrameHelpers {
  import currentSparkSession.implicits._

  val baseCol = ColnamesText("base")
  val textCols = ColnamesText("text")

  val similarityCols = ColnamesTextSimilarity(baseCol, textCols)

  val inputdf: DataFrame = Seq(
    ("en", Seq("car")),
    ("de", Seq("auto")),
    ("en", Seq("truck")),
    ("en", Seq("desk"))
  )
    .toDF(textCols.language, textCols.cleanWords)
    .withColumn("id", monotonically_increasing_id)
    .withColumn("id", $"id".cast("string"))

  val model: EmbeddingsModel = DsPipeline.getEmbeddingsModel(textCols)

  val df: DataFrame = model.transform(inputdf)
}
