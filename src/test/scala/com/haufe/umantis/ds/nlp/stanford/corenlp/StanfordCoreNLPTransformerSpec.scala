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

package com.haufe.umantis.ds.nlp.stanford.corenlp

import com.haufe.umantis.ds.nlp.{ColnamesText, DsPipeline}
import com.haufe.umantis.ds.spark.SparkSessionWrapper
import com.haufe.umantis.ds.tests.SparkSpec
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.scalatest.Matchers
import org.scalatest.tagobjects.Slow


class StanfordCoreNLPTransformerSpec extends SparkSpec
  with StanfordCoreNLPFixture with Matchers {

  val coreNLP: StanfordCoreNLPTransformer = DsPipeline.getStanfordCoreNLP(cols)

  val colsSecond = ColnamesText("sec")

  def time(name: String, block: => Unit): Long = {
    val t0 = System.nanoTime()
    val
    result: Unit = block // call-by-name
    val t1 = System.nanoTime()
    println(s"$name: " + (t1 - t0) / 1000000000f + "s")
    t1 - t0
  }

  "StanfordCoreNLPTransformer" must "create columns lemma, pos, ner, and tokens " +
    "and also ensure that the second transformation runs quicker" taggedAs Slow in {
    df.persist()
    expected.persist()

    val t1 = time("First transformation", {
      val result = coreNLP.transform(df).drop(cols.clean)

      assertSmallDataFrameEquality(result, expected)
    })

    val t2 = time("Second transformation", {
      val result = coreNLP.transform(df).drop(cols.clean)

      assertSmallDataFrameEquality(expected, result)
    })
    assert(t2 < t1, "No initialization needs to happen")

    val t3 = time("Two transformers transforms", {
      val r = df.withColumn(colsSecond.language, col(cols.language))
        .withColumn(colsSecond.clean, col(cols.clean))
      val result = DsPipeline
        .getStanfordCoreNLP(colsSecond)
        .transform(coreNLP.transform(r))
        .select(cols.language, cols.corenlpTokens, cols.pos, cols.lemma, cols.ner,
          colsSecond.language, colsSecond.corenlpTokens, colsSecond.pos,
          colsSecond.lemma, colsSecond.ner)

      val expect = expected.withColumn(colsSecond.language, col(cols.language))
        .withColumn(colsSecond.corenlpTokens, col(cols.corenlpTokens))
        .withColumn(colsSecond.pos, col(cols.pos))
        .withColumn(colsSecond.lemma, col(cols.lemma))
        .withColumn(colsSecond.ner, col(cols.ner))
        .select(cols.language, cols.corenlpTokens, cols.pos, cols.lemma, cols.ner,
          colsSecond.language, colsSecond.corenlpTokens, colsSecond.pos,
          colsSecond.lemma, colsSecond.ner)
      assertSmallDataFrameEquality(result, expect)
    })

    assert(t3 < t1, "No initialization needs to happen")
  }

  it must "support transformation of Map" taggedAs Slow in {
    val input = Seq(
      Map(cols.language -> "en",
        cols.clean -> "Today is nice day."),
      Map(cols.language -> "de",
        cols.clean -> "Heute ist ein schöner Tag."))

    val result = coreNLP.transform(input)
    val expectedResult = Seq(
      Map(cols.language -> "en",
        cols.clean -> "Today is nice day.",
        cols.corenlpTokens -> Seq("Today", "is", "nice", "day", "."),
        cols.pos -> Seq("NN", "VBZ", "JJ", "NN", "."),
        cols.ner -> Seq("DATE", "O", "O", "DURATION", "O"),
        cols.lemma -> Seq("today", "be", "nice", "day", ".")
      ),
      Map(cols.language -> "de",
        cols.clean -> "Heute ist ein schöner Tag.",
        cols.corenlpTokens -> Seq("Heute", "ist", "ein", "schöner", "Tag", "."),
        cols.pos -> Seq("ADV", "VAFIN", "ART", "ADJA", "NN", "$."),
        cols.ner -> Seq("O", "O", "O", "O", "O", "O"),
        cols.lemma -> null)
    )

    result shouldEqual expectedResult
  }
}


trait StanfordCoreNLPFixture extends SparkSessionWrapper {

  import currentSparkSession.implicits._

  val cols = ColnamesText("text")

  val df: DataFrame = Seq(
    ("en", "The Senior System Center Operation Manager (SCOM) Engineer is responsible for " +
      "managing and maintaining the global enterprise SCOM platform."),
    ("de", "Wenn Sie sich beruflich und fachlich weiterentwickeln, in einem " +
      "dynamischen und erfolgsorientiertem Umfeld arbeiten und mit den neusten " +
      "Technologien in Kontakt kommen möchten."),
    ("da", "The Senior System Center Operation Manager (SCOM) Engineer is responsible for " +
      "managing and maintaining the global enterprise SCOM platform.") // test default language
  )
    .toDF(cols.language, cols.clean)

  val expected: DataFrame = Seq(
    ("en",
      Seq("The", "Senior", "System", "Center", "Operation", "Manager", "-LRB-", "SCOM",
        "-RRB-", "Engineer", "is", "responsible", "for", "managing", "and", "maintaining", "the",
        "global", "enterprise", "SCOM", "platform", "."), Seq("DT", "NNP", "NNP", "NNP", "NNP",
      "NNP", "-LRB-", "NNP", "-RRB-", "NNP", "VBZ", "JJ", "IN", "VBG", "CC", "VBG", "DT", "JJ",
      "NN", "NN", "NN", "."), Seq("the", "Senior", "System", "Center", "Operation", "Manager",
      "-lrb-", "SCOM", "-rrb-", "Engineer", "be", "responsible", "for", "manage", "and",
      "maintain", "the", "global", "enterprise", "scom", "platform", "."),
      Seq("O", "MISC", "MISC", "MISC", "MISC", "TITLE", "O", "ORGANIZATION", "O", "TITLE",
        "O", "O", "O", "O", "O", "O", "O", "O", "O", "O", "O", "O")),
    ("de",
      Seq("Wenn", "Sie", "sich", "beruflich", "und", "fachlich", "weiterentwickeln", ",",
        "in", "einem", "dynamischen", "und", "erfolgsorientiertem", "Umfeld", "arbeiten",
        "und", "mit", "den", "neusten", "Technologien", "in", "Kontakt", "kommen", "möchten", "."),
      Seq("KOUS", "PPER", "PRF", "ADJD", "KON", "ADJD", "VVINF", "$,", "APPR", "ART", "ADJA",
        "KON", "ADJA", "NN", "VVFIN", "KON", "APPR", "ART", "ADJA", "NN", "APPR", "NN", "VVINF",
        "VMFIN", "$."),
      null,
      Seq("O", "O", "O", "O", "O", "O", "O", "O", "O", "O", "O", "O", "O", "O", "O", "O", "O",
        "O", "O", "O", "O", "O", "O", "O", "O")),
    ("da",
      Seq("The", "Senior", "System", "Center", "Operation", "Manager", "-LRB-", "SCOM", "-RRB-",
        "Engineer", "is", "responsible", "for", "managing", "and", "maintaining", "the",
        "global", "enterprise", "SCOM", "platform", "."),
      Seq("DT", "NNP", "NNP", "NNP", "NNP", "NNP", "-LRB-", "NNP", "-RRB-", "NNP", "VBZ",
        "JJ", "IN", "VBG", "CC", "VBG", "DT", "JJ", "NN", "NN", "NN", "."),
      Seq("the", "Senior", "System", "Center", "Operation", "Manager", "-lrb-", "SCOM",
        "-rrb-", "Engineer", "be", "responsible", "for", "manage", "and", "maintain", "the",
        "global", "enterprise", "scom", "platform", "."),
      Seq("O", "MISC", "MISC", "MISC", "MISC", "TITLE", "O", "ORGANIZATION", "O", "TITLE",
        "O", "O", "O", "O", "O", "O", "O", "O", "O", "O", "O", "O")) // test default language
  )
    .toDF(cols.language, cols.corenlpTokens, cols.pos, cols.lemma, cols.ner)
}
