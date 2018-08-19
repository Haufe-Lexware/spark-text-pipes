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

import com.carrotsearch.labs.langid.{DetectedLanguage, LangIdV3}
import com.haufe.umantis.ds.nlp.params._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.ScalaReflection.schemaFor
import org.apache.spark.ml.UnaryTransformer
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.types.DataType


//case class DetectedLanguage(langCode: String, confidence: Double)


class LangId extends Serializable {
  @transient private lazy val langid: LangIdV3 = new LangIdV3()

  // This has to be synchronized because LangIdV3 is not thread safe
  def classify(str: String, normalizeConfidence: Boolean): DetectedLanguage = {
    this.synchronized {
      langid.classify(str, normalizeConfidence)
    }
  }
}
object LangId {
  lazy val langid: LangId = new LangId()
  def apply(): LangId = {
    langid
  }
}


class LanguageDetector(override val uid: String)
  // first arg is input, second output, third is class name
  extends UnaryTransformer[String, String, LanguageDetector]
    with HasSupportedLanguages with HasDefaultLanguage with TransformerList {

  def this() = this(Identifiable.randomUID("LanguageDetector"))

  val langid: LangId = LangId()
  val langidB: Broadcast[LangId] =
    SparkSession.builder().getOrCreate().sparkContext.broadcast(langid)

  def detectLanguage(text: String): String = {
    // here we add a space because it help to increase the prediction accuracy
    // when the input text is just a single word
    // e.g. "Hund" -> "en " or "Praktikum" -> "en ", "cosa" -> "en" while
    // "Hund " -> "de " or "Praktikum " -> "de ", "cosa " -> "es"
    // NOTE 04/06/2018: we *always* need to lowercase the input otherwise we always get "en"
    val detectedLanguage =
      langidB.value.classify(s"$text ".toLowerCase, true).getLangCode

    if ($(supportedLanguages).contains(detectedLanguage))
      detectedLanguage
    else
      $(defaultLanguage)
  }

  override protected def createTransformFunc: String => String = detectLanguage

  override protected def validateInputType(inputType: DataType): Unit = {
    require(inputType == schemaFor[String].dataType)
  }

  override protected def outputDataType: DataType = {
    schemaFor[String].dataType
  }

  def transform(data: Seq[Map[String, Any]]): Seq[Map[String, Any]] = {
      val res = data.map(p => {
        p + (getOutputCol -> detectLanguage(p(getInputCol).asInstanceOf[String]))
      })
      res
  }
}
