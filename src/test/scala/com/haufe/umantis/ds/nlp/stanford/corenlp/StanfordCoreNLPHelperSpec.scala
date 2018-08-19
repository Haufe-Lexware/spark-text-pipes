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

import org.scalatest.tagobjects.Slow
import org.scalatest.{FlatSpec, Matchers}

class StanfordCoreNLPHelperSpec extends FlatSpec with Matchers {

  "Annotate with token" should "generate annotation" taggedAs Slow in {
    val result = StanfordCoreNLPHelper.local().tokenize("en").apply("Today is nice day.")
    result shouldEqual Seq("Today", "is", "nice", "day", ".")
  }

  "Annotate with lemma" should "generate annotation" taggedAs Slow in {
    val result = StanfordCoreNLPHelper.local().lemma("en").apply("Today is nice day.")
    result shouldEqual Seq("today", "be", "nice", "day", ".")
  }

  "Annotate with pos" should "generate annotation" taggedAs Slow in {
    val result = StanfordCoreNLPHelper.local().pos("en").apply("Today is nice day.")
    result shouldEqual Seq("NN", "VBZ", "JJ", "NN", ".")
  }

  "Annotate with ner" should "generate annotation" taggedAs Slow in {
    val result = StanfordCoreNLPHelper.local().ner("en").apply("Today is nice day.")
    result shouldEqual Seq("DATE", "O", "O", "DURATION", "O")
  }

  "Annotate with sentiment" should "generate annotation" taggedAs Slow in {
    val result = StanfordCoreNLPHelper.local().sentiment("en").apply("Today is nice day.")
    result shouldEqual Seq("Positive")
  }

  /** TODO  enable french in configuration
    * "French annotation " should "work" taggedAs Slow in {
    * val resultFr = StanfordCoreNLPHelper.local().pos("fr")("Aujourd'hui c'était une belle
    * journée.")
    * val resultEn = StanfordCoreNLPHelper.local().pos("en")("Aujourd'hui c'était une belle
    * journée.")
    *
    * val resultDe = StanfordCoreNLPHelper.local().pos("de")("Aujourd'hui c'était une belle
    * journée.")
    * *
    * println(resultFr)
    * println(resultEn)
    * println(resultDe)
    * }
    * */
  "Annotate with cleanxml" should "generate annotation" taggedAs Slow in {
    val result = StanfordCoreNLPHelper.local().cleanxml("en").apply(
      "<xml>Today is <b>nice</b> day.</xml>")
    result shouldEqual Seq("Today", "is", "nice", "day", ".")
  }

  "Annotate with deparse" should "generate annotation" taggedAs Slow in {
    val result = StanfordCoreNLPHelper.local().depparse("en")("Today is nice day.")
    result shouldEqual Seq(
      """<dependencies style="typed">
        |  <dep type="nsubj">
        |    <governor idx="4">day</governor>
        |    <dependent idx="1">Today</dependent>
        |  </dep>
        |  <dep type="cop">
        |    <governor idx="4">day</governor>
        |    <dependent idx="2">is</dependent>
        |  </dep>
        |  <dep type="amod">
        |    <governor idx="4">day</governor>
        |    <dependent idx="3">nice</dependent>
        |  </dep>
        |  <dep type="punct">
        |    <governor idx="4">day</governor>
        |    <dependent idx="5">.</dependent>
        |  </dep>
        |</dependencies>
        |""".stripMargin)
  }
}
