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

import com.haufe.umantis.ds.nlp.params.{HasDefaultLanguage, HasLanguageCol}
import org.apache.spark.ml.feature.StopWordsRemover
import org.apache.spark.ml.util.{DefaultParamsReadable, Identifiable}
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.ml.param._


class MultiLanguageStopWordsRemover (override val uid: String)
  extends StopWordsRemover with HasLanguageCol with HasDefaultLanguage with TransformerList {

  def this() = this(Identifiable.randomUID("MultiLanguageStopWords"))

  /**
    * The words to be filtered out.
    * Default: English stop words
    * @see `StopWordsRemover.loadDefaultStopWords()`
    * @group param
    */
  val stopWordsMap: Param[Map[String, Set[String]]] =
    new Param[Map[String, Set[String]]](this, "stopWordsMap",
      "the words to be filtered out in multiple languages")

  /** @group setParam */
  def setStopWordsMap(value: Map[String, Set[String]]): this.type = set(stopWordsMap, value)

  /** @group getParam */
  def getStopWordsMap: Map[String, Set[String]] = $(stopWordsMap)

  private def transformFunction() = {
    val supportedLanguages = $(stopWordsMap).keySet
    if ($(caseSensitive)) {
      (language: String, terms: Seq[String]) =>
        if (language == null || terms == null) null else {
          val stopWordsLanguage =
            if (supportedLanguages.contains(language)) language else $(defaultLanguage)
          val stopWordsSet = $(stopWordsMap)(stopWordsLanguage)
          terms.filter(s => !stopWordsSet.contains(s))
        }
    } else {
      // TODO: support user locale (SPARK-15064)
      val toLower = (s: String) => if (s != null) s.toLowerCase else s
      (language: String, terms: Seq[String]) =>
        if (language == null || terms == null) null else {
          val stopWordsLanguage =
            if (supportedLanguages.contains(language)) language else $(defaultLanguage)
          val stopWordsSetLower = $(stopWordsMap)(stopWordsLanguage + "_lower")
          terms.filter(s => !stopWordsSetLower.contains(toLower(s)))
        }
    }
  }
  
  def transform(data: Seq[Map[String, Any]]): Seq[Map[String, Any]] = {
    val res = data.map(p => {
      p + ($(outputCol) -> transformFunction()(p($(languageCol))
        .asInstanceOf[String],p($(inputCol)).asInstanceOf[Seq[String]]))
    })
    res
  }
  
  override def transform(dataset: Dataset[_]): DataFrame = {
    val outputSchema = transformSchema(dataset.schema)
    val t = udf{transformFunction()}
    val metadata = outputSchema($(outputCol)).metadata
    dataset.select(
      col("*"), t(col($(languageCol)), col($(inputCol))).as($(outputCol), metadata))
  }
}


object MultiLanguageStopWordsRemover extends DefaultParamsReadable[StopWordsRemover] {

  val codeToNames: Map[String, String] = Seq(
    "da" -> "danish",
    "nl" -> "dutch",
    "en" -> "english",
    "fi" -> "finnish",
    "fr" -> "french",
    "de" -> "german",
    "hu" -> "hungarian",
    "it" -> "italian",
    "no" -> "norwegian",
    "pt" -> "portuguese",
    "ru" -> "russian",
    "es" -> "spanish",
    "sv" -> "swedish",
    "tr" -> "turkish"
  ).toMap

  val supportedLanguages = Set("danish", "dutch", "english", "finnish", "french", "german",
    "hungarian", "italian", "norwegian", "portuguese", "russian", "spanish", "swedish", "turkish")

  override def load(path: String): StopWordsRemover = super.load(path)

  /**
    * Loads the default stop words for the given language.
    * Supported languages: danish, dutch, english, finnish, french, german, hungarian,
    * italian, norwegian, portuguese, russian, spanish, swedish, turkish
    * @see <a href="http://anoncvs.postgresql.org/cvsweb.cgi/pgsql/src/backend/snowball/stopwords/">
    * here</a>
    */
  def loadDefaultStopWordsMap(languageCodes: Array[String]): Map[String, Set[String]] = {
//    require(languageCodes.forall(
//      languageCode => supportedLanguages.contains(codeToNames(languageCode))),
//      s"one of $languageCodes is not in the supported language " +
//        s"list: ${supportedLanguages.mkString(", ")}.")

    val supportedLanguageCodes = languageCodes.filter(lang => codeToNames.contains(lang))

    supportedLanguageCodes.flatMap(languageCode => {
      val toLower = (s: String) => if (s != null) s.toLowerCase else s

        val languageName = codeToNames(languageCode)
        val languageCodeLower = languageCode + "_lower"
        val is = getClass.getResourceAsStream(
          s"/org/apache/spark/ml/feature/stopwords/$languageName.txt")
        val words = scala.io.Source.fromInputStream(is)(scala.io.Codec.UTF8).getLines().toSet
        val wordsLower = words.map(toLower(_))
        Seq(languageCode -> words, languageCodeLower -> wordsLower)
    }).toMap
  }
}
