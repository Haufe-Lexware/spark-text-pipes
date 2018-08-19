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

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

object udffunctions {
  val snlp = StanfordCoreNLPHelper()

  def cleanxml: UserDefinedFunction = cleanxml()

  def cleanxml(language: String = "en"): UserDefinedFunction = udf {
    snlp.value.cleanxml(language)
  }

  def tokenize: UserDefinedFunction = tokenize()

  def tokenize(language: String = "en"): UserDefinedFunction = udf {
    snlp.value.tokenize(language)
  }

  def ssplit: UserDefinedFunction = ssplit()

  def ssplit(language: String = "en"): UserDefinedFunction = udf {
    snlp.value.ssplit(language)
  }

  def pos: UserDefinedFunction = pos()

  def pos(language: String = "en"): UserDefinedFunction = udf {
    snlp.value.pos(language)
  }

  def lemma: UserDefinedFunction = lemma()

  def lemma(language: String = "en"): UserDefinedFunction = udf {
    snlp.value.lemma(language)
  }

  def ner: UserDefinedFunction = ner()

  def ner(language: String = "en"): UserDefinedFunction = udf {
    snlp.value.ner(language)
  }

  def sentiment: UserDefinedFunction = ner()

  def sentiment(language: String = "en"): UserDefinedFunction = udf {
    snlp.value.sentiment(language)
  }
}
