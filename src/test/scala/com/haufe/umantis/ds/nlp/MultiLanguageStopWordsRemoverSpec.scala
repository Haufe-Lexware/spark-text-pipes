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


class MultiLanguageStopWordsRemoverSpec extends SparkSpec
  with MultiLanguageStopWordsRemoverSpecFixture {

  val stopWordsMap: Map[String, Set[String]] =
    MultiLanguageStopWordsRemover.loadDefaultStopWordsMap(Array("de", "en", "it"))

  val remover: MultiLanguageStopWordsRemover = DsPipeline.getStopWordsRemover(cols)
    .setStopWordsMap(stopWordsMap)


  "MultiLanguageStopWordsRemover" must "remove all stop words (multi language) in Dataframe" in {
    val result = remover.transform(df).drop(cols.tokens)

    assertSmallDataFrameEquality(expected, result)
  }

  it must "remove all stop words (multi language) in Seq[Map[String, Any]]" in {
    val resultMap = remover.transform(df.toSeqOfMaps)
    val expectedResult = mergeDataframes(Array(df, expected)).toSeqOfMaps

    resultMap shouldBe expectedResult
  }
}

trait MultiLanguageStopWordsRemoverSpecFixture extends SparkSessionWrapper {
  import currentSparkSession.implicits._

  val cols = ColnamesText("text")

  val df: DataFrame = Seq(
    ("en", Seq("the", "me", "alle", "andere")),
    ("de", Seq("the", "me", "alle", "andere")),
    ("it", Seq("the", "me", "il", "gli")),
    ("da", Seq("the", "me", "alle", "andere")) // test default language
  )
    .toDF(cols.language, cols.tokens)

  val expected: DataFrame = Seq(
    ("en", Seq("alle", "andere")),
    ("de", Seq("the", "me")),
    ("it", Seq("the", "me")),
    ("da", Seq("alle", "andere")) // test default language
  )
    .toDF(cols.language, cols.cleanWords)
}
