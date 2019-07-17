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

import com.haufe.umantis.ds.utils.ConfigGetter
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.ml.param.shared.{HasInputCol, HasOutputCol}
import org.apache.spark.ml.util._
import org.apache.spark.sql.catalyst.ScalaReflection.schemaFor
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.{DataType, StructField, StructType}
import org.apache.spark.sql.{AnalysisException, DataFrame, Dataset, SparkSession}

import scala.collection.immutable.ListMap
import scala.util.matching.Regex

case class Substitution(pattern: Regex, repl: String)
case class Acronym(acronym: String, expansion: String)

/**
  * A feature transformer that expands acronyms in the text in input.
  */
class TextCleaner (override val uid: String)
  extends Transformer with HasInputCol with HasOutputCol
    with DefaultParamsWritable with TransformerList {
  def this() = this(Identifiable.randomUID("textCleaner"))

  val bCleaningSubs: Broadcast[ListMap[String, Substitution]] =
    SparkSession.builder().getOrCreate().sparkContext.broadcast(TextCleaner.cleaningSubs)

  val bAcronymsSubs: Broadcast[Map[String, Substitution]] =
    SparkSession.builder().getOrCreate().sparkContext.broadcast(TextCleaner.acronymsSubs)

  /** @group setParam */
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  def setOutputCol(value: String): this.type = set(outputCol, value)

  final val expandAcronyms: Param[Boolean] =
    new Param[Boolean](this, "expandAcronyms", "expand acronyms?")

  /** @group getParam */
  final def getExpandAcronyms: Boolean = $(expandAcronyms)

  /** @group setParam */
  final def setExpandAcronyms(value: Boolean): this.type = {
    set(expandAcronyms, value)
  }

  setDefault(expandAcronyms, true)

  override def transform(dataset: Dataset[_]): DataFrame = {
    val outputSchema = transformSchema(dataset.schema)
    val t = udf { text: String =>
      if (text == null) null else {
        val subsToApply = if (useAcronyms(text))
          Array(bCleaningSubs.value, bAcronymsSubs.value)
        else
          Array(bCleaningSubs.value)

        applySubstitutionsSeq(text, subsToApply)
      }
    }
    val metadata = outputSchema($(outputCol)).metadata
    dataset.select(col("*"), t(col($(inputCol))).as($(outputCol), metadata))
  }

  val transformFunction: String => String = { text: String =>
    applySubstitutionsSeq(text, getSubstitutions(text))
  }

  def transform(data: Seq[Map[String, Any]]): Seq[Map[String, Any]] = {
    data.map(p => {
      p + (getOutputCol -> transformFunction(p(getInputCol).asInstanceOf[String]))
    })
  }

  @inline
  def useAcronyms(text: String): Boolean = {
    // if text is all uppercase we don't expand acronyms at all
    $(expandAcronyms) && ! text.forall(c => !c.isLetter || c.isUpper)
  }

  @inline
  def getSubstitutions[M <: Map[String, Substitution]](text: String)
  : Array[Map[String, Substitution]] = {

    if (useAcronyms(text))
      Array(TextCleaner.cleaningSubs, TextCleaner.acronymsSubs)
    else
      Array(TextCleaner.cleaningSubs)
  }

  def applySubstitutionsSeq[M <: Map[String, Substitution]](text: String, subsSeq: Seq[M])
  : String = {
    subsSeq.foldLeft(text)((res, substitutionsList) =>
      applySubstitutionsSafe(res, substitutionsList))
  }

  /**
    * Used in tests: we need to detect if a substitution throws an exception.
    *
    * @param text The text to be
    * @param subs The [[Map]] of [[Substitution]] to apply
    * @tparam M The type of [[Map]] to apply
    * @return The String with substitutions applied
    */
  def applySubstitutionsUnsafe[M <: Map[String, Substitution]](text: String, subs: M): String = {
    doApplySubstitutions(text, subs, safe = false)
  }

  /**
    * Used in production: does not throw exception if a substitution fails.
    *
    * @param text The text to be
    * @param subs The [[Map]] of [[Substitution]] to apply
    * @tparam M The type of [[Map]] to apply
    * @return The String with substitutions applied
    */
  def applySubstitutionsSafe[M <: Map[String, Substitution]](text: String, subs: M): String = {
    doApplySubstitutions(text, subs, safe = true)
  }

  private def doApplySubstitutions[M <: Map[String, Substitution]]
  (text: String, subs: M, safe: Boolean): String = {

    subs
      .foldLeft(text){case (res, (name, sub)) =>
        try {
          sub.pattern.replaceAllIn(res, sub.repl)
        } catch {
          case e: java.lang.IllegalArgumentException =>
            println(
              s"#### substitution with name $name " +
              s"failed with message: ${e.getMessage} " +
              s"when processing text: \n $text")
            if (safe)
              res
            else
              throw e
        }
      }
  }

  protected def validateInputType(inputType: DataType): Unit = {
    require(inputType == schemaFor[String].dataType)
  }

  protected def outputDataType: DataType = {
    schemaFor[String].dataType
  }

  override def transformSchema(schema: StructType): StructType = {
    val inputType = schema($(inputCol)).dataType
    validateInputType(inputType)
    if (schema.fieldNames.contains($(outputCol))) {
      throw new IllegalArgumentException(s"Output column ${$(outputCol)} already exists.")
    }
    val outputFields = schema.fields :+
      StructField($(outputCol), outputDataType, nullable = false)
    StructType(outputFields)
  }

  override def copy(extra: ParamMap): TextCleaner = defaultCopy(extra)
}


object TextCleaner extends Acronyms {

  val cleaningSubs: ListMap[String, Substitution] = {
    ListMap(
      // remove URLs
      ("urls", Substitution("""https?://[^\s]+""".r, "")),

      // any number of newlines or spaces -> one single space
      ("rep_spaces1", Substitution("""[\n\r ]+""".r, " ")),

      // any number of underscores -> one single space
      ("rep_underscore", Substitution("""_+""".r, " ")),

      // Hi.There -> Hi. There
      // 1..10M -> 1..10M
      // U.S. -> U.S.
      // \p{Ll} is any unicode lowercase letter
      // \p{Lu} is any unicode uppercase letter
      ("dots", Substitution("""(?<gb>[\p{Ll}0-9])\.(?<ge>[\p{Lu}])""".r, """${gb}. ${ge}""")),

      // Hi;there -> Hi; there
      // also apply to ':', ')'
      // we also don't match if the characters in the first group are followed by a '/'
      // which is useful if 'urls' is disabled (we don't want to match with http://)
      (";:)", Substitution("""(?<gb>[;:)])(?<ge>[^ /])""".r, """${gb} ${ge}""")),

      // For '(' we add a space before '(', not after
      ("(", Substitution("""(?<gb>[^ ])(?<ge>[(])""".r, """${gb} ${ge}""")),

      // floating point separator should be a dot
      ("floating", Substitution("""(?<gb>[0-9]),(?<ge>[0-9][0-9][^0-9])""".r, """${gb}.${ge}""")),

      // thousands separator should be a comma
      ("thousand", Substitution("""(?<gb>[0-9])\.(?<ge>[0-9][0-9][0-9])""".r, """${gb},${ge}""")),

      // Hi,there -> Hi, there
      // we add a space after the comma if not present,
      // except in the middle of a number (e.g. 150,000)
      ("comma1", Substitution("""(?<gb>\p{L},)(?<ge>[0-9])""".r, """${gb} ${ge}""")),
      ("comma2", Substitution("""(?<gb>[0-9],)(?<ge>\p{L})""".r, """${gb} ${ge}""")),
      ("comma3", Substitution("""(?<gb>\p{L},)(?<ge>\p{L})""".r, """${gb} ${ge}""")),

      // We remove these characters
      ("tm", Substitution("""[®™“”"]""".r, "")),

      // These characters are separators
      // ("separators", Substitution("""[—–\-/]""", " ")),
      ("separators", Substitution("""[—–\-]""".r, " ")),

      // also '/' is a separator but not if double ('//') because we could
      // have disabled 'urls'.
      ("separator/", Substitution("""(?<!/)/(?!/)""".r, " ")),

      // quotation marks or apostrophes
      ("quotation", Substitution("""[´‘’]""".r, "'")),

      // any number of newlines or spaces -> one single space
      // we do it again to stay on the safe side
      ("rep_spaces2", Substitution("""[\n\r ]+""".r, " ")),

      // repetitions of single characters within parentheses (ii), (iii), (iiii) as in lists
      ("lists", Substitution("""\(((.)\2+)\)""".r, ". "))
    )
  }

  // val separator = """([^\p{Ll}\p{Lu}0-9])"""
  // val separatorEnd   = """($|[\s\p{Punct}&&[^$]])""" // all in \p{Punct} except $

  val separatorBegin = """(?<gb>^|\s)"""
  val separatorEnd   = """(?<ge>$|[\s\p{Punct}])"""

  val replacementBegin = """${gb}"""
  val replacementEnd   = """${ge}"""

  import currentSparkSession.implicits._

  def acronymsToMap(acronyms: DataFrame): Map[String, Substitution] = {
    acronyms
      .as[Acronym]
      .collect()
      .map(x => (x.acronym,
        Substitution(
          s"$separatorBegin${x.acronym}$separatorEnd".r,
          s"$replacementBegin${x.expansion}$replacementEnd"
        )
      ))
      .toMap
  }

  val acronymsSubs: Map[String, Substitution] = {
    try {
      // reading DataFrame of acronyms
      acronymsToMap(readDataFrame("AcronymsExpansion"))
    } catch {
      // DataFrame not found; trying to read from csv
      case _: AnalysisException =>
        try {
          val acronymsDf = csvToDataFrame("AcronymsExpansion.csv")
          writeDataFrame(acronymsDf, "AcronymsExpansion")
          acronymsToMap(acronymsDf)
        } catch {
          // csv file not found, reading default acronyms
          case _: AnalysisException =>

            // getting resource file as list of String
            val csvString = ConfigGetter
              .getConfigString("/AcronymsExpansion.csv")
              .lines
              .toSeq

            // converting to Dataset[String]
            val csvData: Dataset[String] = currentSparkSession
              .sparkContext
              .parallelize(csvString)
              .toDS()

            // inferring schema and converting to DataFrame
            val acronyms = currentSparkSession
              .read
              .option("header", true)
              .option("delimiter", "\t")
              .option("inferSchema",true)
              .csv(csvData)
              .withColumn("expansion", sanitizeUDF(col("expansion")))

            acronymsToMap(acronyms)
        }
    }
  }
}