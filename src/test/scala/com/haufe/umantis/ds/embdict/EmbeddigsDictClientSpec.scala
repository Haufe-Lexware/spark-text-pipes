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

package com.haufe.umantis.ds.embdict

import com.haufe.umantis.ds.embdict.messages.VectorResponseWithIndex
import com.haufe.umantis.ds.nlp.ColnamesText
import com.haufe.umantis.ds.spark.{DataFrameHelpers, SparkSessionWrapper}
import com.haufe.umantis.ds.tests.SparkSpec
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit
import org.scalatest.Matchers._
import org.scalatest._

/**
  * @note This test relies on an EXTERNAL CONNECTION with the EmbeddingsDict server.
  *       The computer you run this tests on MUST be able to resolve "embdict" correctly
  *       and the server MUST resolve "embdict" to itself.
  */
class EmbeddingsDictClientSpec extends SparkSpec with EmbeddingsDictClientSpecFixture {
  val embdict = EmbeddingsDictClient(cacheName = "tmpcache")

  def query(word: String, language: String = "en", serialDictImpl: Boolean = false)
  : Option[Array[Float]] = {
    // skip cache
    embdict.queryRemote(language, word, serialDictImpl)
  }

  def queryWithIndex(word: String, language: String = "en", serialDictImpl: Boolean = false)
  : VectorResponseWithIndex = {
    // skip cache
    embdict.queryRemoteWithIndex(language, word, serialDictImpl)
  }

  "EmbeddingsDictClient" must "return a vector when queried with a word" in {
    query("dog") shouldBe defined
  }

  it must "return None when queried with a number" in {
    query("123") shouldBe None
  }

  it must "return a vector and its corresponding index when queried with a word (with index)" in {
    val res = queryWithIndex("dog")
    res.index shouldBe defined
    res.vector shouldBe defined
  }

  it must "return None for both vector and index when queried with a number (with index)" in {
    val res = queryWithIndex("456")
    res.index shouldBe None
    res.vector shouldBe None
  }

  it must "provide correct analogies" in {
    val resultsQueen = embdict.analogy(
      "en", positive = Array("king", "woman"), negative = Array("man"), 1)
    resultsQueen should contain key "queen"

    val resultsBerlin = embdict.analogy(
      "en", positive = Array("paris", "germany"), negative = Array("france"), 1)
    resultsBerlin should contain key "berlin"
  }

  it must "provide correct analogies if negatives are empty" in {
    val results = embdict.analogy(
      "en", positive = Array("king", "woman"), negative = Array(), topN = 1)

    // the expected result is still "queen" because
    // "king" and "woman" are filtered in analogy calculations
    results should contain key "queen"
  }

  it must "provide correct multi language synonyms" in {
    // most similar word to English "plumber" in German should be klempner
    query("plumber") match {
      case Some(plumber) =>
        val klempner = embdict.findSynonyms("de", plumber, 1)
        klempner should contain key "klempner"
      case None => fail()
    }

    // most similar word to German "klempner" in English should be plumber
    query("klempner", "de") match {
      case Some(klempner) =>
        val plumber = embdict.findSynonyms("en", klempner, 1)
        plumber should contain key "plumber"
      case None => fail()
    }
  }

  it must "cache results internally" in {
    val ed = EmbeddingsDictClient(cacheName = "tmpcache")
//    ed.getCacheSize.foreach {case (lang, cacheSize) => println(s"$lang: $cacheSize")}

    ed.getCacheSize.foreach {case (lang, cacheSize) => cacheSize shouldBe 0}

    ed.query("en", "dog")
    ed.cache("en") should contain key "dog"

    ed.query("de", "hund")
    ed.cache("de") should contain key "hund"
  }

  it must "persist and delete cache values when so instructed" in {
    val ed = EmbeddingsDictClient(cacheName = "tmpcache")
    ed.query("en", "dog")
    ed.query("de", "hund")

    ed.saveCache("tmpcache")

    val ed2 = EmbeddingsDictClient(cacheName = "tmpcache")
    ed.cache("en") should contain key "dog"
    ed.cache("de") should contain key "hund"

    ed2.clearCache()
    ed2.saveCache("tmpcache")

    val ed3 = EmbeddingsDictClient(cacheName = "tmpcache")
    ed3.getCacheSize.foreach {case (lang, cacheSize) => cacheSize shouldBe 0}
  }

  it must "fill cache using all words in a list of DataFrames" in {
    val input: Map[DataFrame, Array[ColnamesText]] = Seq(
      df1 -> Array(animals1, animals2),
      df2 -> Array(animals3, animals4)
    ).toMap

    val ed = EmbeddingsDictClient(cacheName = "tmpcache")
    ed.getCacheSize.foreach {case (lang, cacheSize) => cacheSize shouldBe 0}

    EmbeddingsDictClient
      .fillEmbeddingsCache(input, clearCache = true, embDict = ed, providedCacheName = "tmpcache")

    val ed2 = EmbeddingsDictClient(cacheName = "tmpcache")
    (words1 ++ words2 ++ words3
      ++ words4.flatMap(x => if (x != null) x else Seq()))
      .toSet
      .foreach{word: String => ed2.cache("en") should contain key word}

    // cleanup
    ed2.clearCache()
    ed2.saveCache("tmpcache")
  }
}

trait EmbeddingsDictClientSpecFixture extends SparkSessionWrapper {
  import currentSparkSession.implicits._

  val animals1: ColnamesText = ColnamesText("animals1")
  val animals2: ColnamesText = ColnamesText("animals2")
  val animals3: ColnamesText = ColnamesText("animals3")
  val animals4: ColnamesText = ColnamesText("animals4")

  val words1 = Seq("dog", "cat", "mouse")
  val words2 = Seq("donkey", "horse", "cow")
  val words3 = Seq("chinchilla", "shark", "bear")
  val words4 = Seq(null, Array("dog", "wolf"), null)

  val col1: DataFrame = words1.map(Array(_)).toDF(animals1.cleanWords)
  val col2: DataFrame = words2.map(Array(_)).toDF(animals2.cleanWords)
  val col3: DataFrame = words3.map(Array(_)).toDF(animals3.cleanWords)
  val col4: DataFrame = words4.toDF(animals4.cleanWords)

  val df1: DataFrame = DataFrameHelpers
    .mergeDataframes(Array(col1, col2))
    .withColumn(animals1.language, lit("en"))
    .withColumn(animals2.language, lit("en"))
  val df2: DataFrame = DataFrameHelpers
    .mergeDataframes(Array(col3, col4))
    .withColumn(animals3.language, lit("en"))
    .withColumn(animals4.language, lit("en"))
}
