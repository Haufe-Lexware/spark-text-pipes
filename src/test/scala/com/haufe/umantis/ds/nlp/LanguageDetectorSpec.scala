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


class LanguageDetectorSpec extends SparkSpec with LanguageDetectorSpecFixture {

  val ld: LanguageDetector = DsPipeline.getLanguageDetector(cols)
    .setSupportedLanguages(Array("en", "de", "fr", "it", "es"))

  "LanguageDetector" must "detect all languages in Dataframe" in {
    val result = ld.transform(text).drop(cols.text)

    assertSmallDataFrameEquality(languages, result)
  }

  it must "detect all languages in Seq[Map[String, Any]]" in {
    val resultMap = ld.transform(text.toSeqOfMaps)
    val expected = mergeDataframes(Array(text, languages)).toSeqOfMaps

    resultMap shouldBe expected
  }

  it must "not fail if the String is empty" in {
    val result = ld.transform(emptyDf).drop(cols.text)
    result.head().getAs[String](cols.language) shouldBe ld.getDefaultLanguage
  }

  it must "not fail if the String is null" in {
    val result = ld.transform(nullDf).drop(cols.text)
    result.head().getAs[String](cols.language) shouldBe ld.getDefaultLanguage
  }

  it must "not fail if the String is huge" in {
    Seq(10, 100, 1000, 10000, 100000, 1000000, 10000000).map { size =>
      // println(s"trying $size")
      val result = ld.transform(hugeStringDf(size)).drop(cols.text)
      result.head().getAs[String](cols.language) shouldBe ld.getDefaultLanguage
    }
  }

  it must "not fail if the dataset is large" in {
    Seq(
      ld.transform(hugeDf(11)).drop(cols.text),
      ld.transform(hugeDf(11)).drop(cols.text)
    )
      .par
      .foreach(_.head().getAs[String](cols.language) should not be empty)
  }
}

trait LanguageDetectorSpecFixture extends SparkSessionWrapper {
  import currentSparkSession.implicits._

  val cols = ColnamesText("text")

  val text: DataFrame = Seq(
    "My name is John and I speak English.",
    "Mein Name ist John und ich spreche Deutsch.",
    "Je m'appelle John et je parle français.",
    "Mi chiamo John e parlo italiano.",
    "Mi nombre es John y hablo espańol."
  )
    .toDF(cols.text)

  val emptyDf: DataFrame = Seq("").toDF(cols.text)

  val nullDf: DataFrame = Seq(null.asInstanceOf[String]).toDF(cols.text)

  def hugeStringDf(size: Int): DataFrame =
    Seq("test " * size)
      .toDF(cols.text)

  /**
    * Size of the resulting dataframe = size(input) * pow(2, folds)
    * @param folds Exponent
    * @return a copy of text with many copies of text
    */
  def hugeDf(folds: Int): DataFrame = {
    (1 until folds)
      .foldLeft(text) { (acc, _) => acc.union(acc) }
      .repartition(currentSparkSession.sparkContext.defaultParallelism)
      .cache()
  }

  val languages: DataFrame = Seq(
    "en",
    "de",
    "fr",
    "it",
    "es"
  )
    .toDF(cols.language)
}
