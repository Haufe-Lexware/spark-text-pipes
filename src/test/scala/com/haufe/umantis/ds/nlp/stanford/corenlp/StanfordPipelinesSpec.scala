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

import org.apache.spark.sql.catalyst.ScalaReflection.schemaFor
import org.apache.spark.sql.types.StructField
import org.scalatest.tagobjects.Slow
import org.scalatest.{FlatSpec, Matchers}


class StanfordPipelinesSpec extends FlatSpec with Matchers {

  "Different language pipelines" should "generate merged columns" taggedAs Slow in {
    import StanfordPipelines._
    val pipelines = StanfordPipelines(
      ("en", Sentiment("en_columns")),
      ("de", Tokenize("de_columns"))
    )
    pipelines.columns shouldEqual Seq("en_columns", "de_columns")
  }

  it should "generate merged schema" taggedAs Slow in {
    import StanfordPipelines._
    val pipelines = StanfordPipelines(
      ("en", Tokenize("en_columns")),
      ("de", Tokenize("de_columns"))
    )
    pipelines.schema shouldEqual
      Seq(StructField("en_columns", schemaFor[Seq[String]].dataType, nullable = true),
        StructField("de_columns", schemaFor[Seq[String]].dataType, nullable = true))
  }

  it should "generate pipelines with null transformer" taggedAs Slow in {
    import StanfordPipelines._
    val pipelines = StanfordPipelines(
      ("en", Tokenize("en_columns")),
      ("de", Tokenize("de_columns"))
    )

    pipelines.pipelines shouldEqual Map(
      "en" -> Seq(Tokenize("en_columns"), Null("de_columns")),
      "de" -> Seq(Null("en_columns"), Tokenize("de_columns"))
    )
  }
}
