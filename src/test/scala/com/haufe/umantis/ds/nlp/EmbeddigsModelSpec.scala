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

import com.haufe.umantis.ds.spark.SparkSessionWrapper
import com.haufe.umantis.ds.tests.SparkSpec
import org.apache.spark.sql.DataFrame
import org.scalatest._
import Matchers._
import org.apache.spark.ml.linalg.Vector

case class SynonymResult(word: String, similarity: Float)
case class TransformResult(text__language: String, text__words: String, text__vector: Vector)

/**
  * @note This test relies on an EXTERNAL CONNECTION with the EmbeddingsDict server.
  *       The computer you run this tests on MUST be able to resolve "embdict" correctly
  *       and the server MUST resolve "embdict" to itself.
  */
class EmbeddingsModelSpec extends SparkSpec with EmbeddingsModelSpecFixture{
  import currentSparkSession.implicits._

  val model: EmbeddingsModel = DsPipeline.getEmbeddingsModel(cols)

  def query(word: String, language: String = "en"): Option[Array[Float]] = {
    model.getVector(language, word)
  }

  def syn(word: String, language: String): DataFrame = {
    model.findSynonyms(language, word, 1)
  }

  def syn(vec: Option[Array[Float]], language: String): DataFrame = {
    vec match {
      case Some(array) => model.findSynonyms(language, array, 1)
      case _ => currentSparkSession.createDataFrame(Seq())
    }
  }

  def first(df: DataFrame): String = {
    df.as[SynonymResult].collect().head.word
  }

  "EmbeddingsModel" must "return a vector when queried with a word" in {
    query("dog") shouldBe defined
  }

  it must "return None when queried with a number" in {
    query("123") shouldBe None
  }

  it must "provide correct analogies in DataFrame format" in {
    val result = model.analogy(
      "en", positive = Array("king", "woman"), negative = Array("man"), 1)
    first(result) shouldBe "queen"
  }

  it must "provide correct multi language synonyms in DataFrame format" in {
    // most similar word to English "plumber" in German should be klempner
    val plumber = query("plumber")
    val plumberInGerman = first(syn(plumber, "de"))
    plumberInGerman shouldBe "klempner"

    // most similar word to German "klempner" in English should be plumber
    val klempner = query("klempner", "de")
    val klempnerInEnglish = first(syn(klempner, "en"))
    klempnerInEnglish shouldBe "plumber"
  }

  it must "calculate a vector representation for array of words in DataFrame" in {
    val results = model.transform(df).as[TransformResult].collect()
    results.foreach(_.text__vector.size shouldBe model.getVectorSize)

    // transforming [king, woman], collecting as TransformResult
    val kingWomanResult = model.transform(queenDf).as[TransformResult].collect().head

    // finding synonyms words for the resulting vector
    val kingWomanSynonyms =
      model.findSynonyms("en", kingWomanResult.text__vector, 10)
        .as[SynonymResult]
        .collect()
        .map(_.word)

    // asserting that "queen" is in the synonyms
    kingWomanSynonyms should contain ("queen")
  }
}

trait EmbeddingsModelSpecFixture extends SparkSessionWrapper {
  import currentSparkSession.implicits._

  val cols: ColnamesText = ColnamesText("text")

  val df: DataFrame = Seq(
    ("en", Seq("dog", "cat", "mouse")),
    ("en", Seq("chinchilla", "shark", "bear")),
    ("de", Seq("auto", "hund"))
  )
    .toDF(cols.language, cols.cleanWords)

  val queenDf: DataFrame = Seq(
    ("en", Seq("king", "woman"))
  )
    .toDF(cols.language, cols.cleanWords)
}
