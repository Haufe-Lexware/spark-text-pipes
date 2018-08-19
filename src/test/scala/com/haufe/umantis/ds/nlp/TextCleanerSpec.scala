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


class TextCleanerSpec extends SparkSpec with TextCleanerSpecFixture {

  "TextCleaner" must "clean all text and expand acronyms in text" in {
    val tc = new TextCleaner()

    (dirtySentences, cleanSentences)
      .zipped
      .foreach((dirty, clean) => {
        // we want this to fail in case of error, here
        val maybeClean = tc.getSubstitutions(dirty)
          .foldLeft(dirty)((res, subsList) => tc.applySubstitutionsUnsafe(res, subsList))
        maybeClean shouldBe clean
      })
  }

  it must "clean all text and expand acronyms in Dataframe" in {
    val tc = DsPipeline.getTextCleaner(cols)
    val result = tc.transform(dirty).drop(cols.text)

    clean.show(30, 300)
    result.show(30, 300)
    assertSmallDataFrameEquality(clean, result)
  }

  it must "clean all text and expand acronyms in Seq[Map[String, Any]]" in {
    val tc = DsPipeline.getTextCleaner(cols)

    val resultMap = tc.transform(dirty.toSeqOfMaps)
    val expected = mergeDataframes(Array(dirty, clean)).toSeqOfMaps

    resultMap shouldBe expected
  }
}

trait TextCleanerSpecFixture extends SparkSessionWrapper {
  import currentSparkSession.implicits._

  val cols = ColnamesText("text")

  val dirtySentences: Seq[String] = Seq(
    "I like cars,1..10M,and bikes:cool.  Very.\n\n\nMy name is John;",
    """I like cars(and only sport cars)but I'm poor.""",
    """The U.S. is a large country.It is in North America""",
    """My résumé.Åt is good.""",
    """It´s it‘s it’s A—B A–B A-B A/B""",
    """® ™ “ ” """",
    """I love this https://www.google.es/webhp?sourceid=chrome-instant&ion=1&espv=2&ie=""" +
      """UTF-8#q=haufe+umantis company yeaaahhhh""",
    """I love this http://www.google.es/webhp?sourceid=chrome-instant&ion=1&espv=2&ie=""" +
      """UTF-8#q=haufe+umantis company yeaaahhhh""",
    "siamo tutti\n\r\r\n bellí,ma non\r\r\n tu,150,000 volte",
    """hello 15.000,01 times. 0,02.""",
    """hello(iiii)hello (00) (i) hello_world hello___world""",

    // for acronyms
    "I like IT solutions. it is so HR, good.",
    "I like IT/HR solutions.",
    "I like its COST",
    "I LIKE ITS COLOUR",
    "IT is amazing. Also HR",
    "\tIT\tis amazing. Also HR\t",
    "\tCI$ test"
  )

  val cleanSentences: Seq[String] = Seq(
    """I like cars, 1..10M, and bikes: cool. Very. My name is John;""",
    """I like cars (and only sport cars) but I'm poor.""",
    """The U.S. is a large country. It is in North America""",
    """My résumé. Åt is good.""",
    """It's it's it's A B A B A B A B""",
    """ """,
    """I love this company yeaaahhhh""",
    """I love this company yeaaahhhh""",
    """siamo tutti bellí, ma non tu, 150,000 volte""",
    """hello 15,000.01 times. 0.02.""",
    """hello .  hello .  (i) hello world hello world""",

    // for acronyms
    "I like Information Technology solutions. it is so Human Resources, good.",
    "I like Information Technology Human Resources solutions.",
    "I like its COST",
    "I LIKE ITS COLOUR",
    "Information Technology is amazing. Also Human Resources",
    "\tInformation Technology\tis amazing. Also Human Resources\t",
    "\tContinuous Integration$ test"
  )

  val dirty: DataFrame = dirtySentences.toDF(cols.text)

  val clean: DataFrame = cleanSentences.toDF(cols.clean)
}
