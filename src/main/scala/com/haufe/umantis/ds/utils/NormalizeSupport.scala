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

package com.haufe.umantis.ds.utils

import com.haufe.umantis.ds.nlp.Substitution

import com.ibm.icu.text.Transliterator

/**
  * Performs standard Java/unicode normalization on the trimmed and lowercased form
  * of the input String and then adds a few extra tricks for dealing with special
  * characters.
  *
  * JVM/Unicode normalization references (warning: learning curve black hole, beware!):
  *
  * - http://docs.oracle.com/javase/7/docs/api/java/text/Normalizer.html
  * - http://stackoverflow.com/questions/5697171/regex-what-is-incombiningdiacriticalmarks
  * - http://stackoverflow.com/questions/1453171/%C5%84-%C7%B9-%C5%88-%C3%B1-%E1%B9%85-%C5%86-%E1%B9%87-%E1%B9%8B-%E1%B9%89-%CC%88-%C9%B2-%C6%9E-%E1%B6%87-%C9%B3-%C8%B5-n-or-remove-diacritical-marks-from-unicode-cha
  * - http://lipn.univ-paris13.fr/~cerin/BD/unicode.html
  * - http://www.unicode.org/reports/tr15/tr15-23.html
  * - http://www.unicode.org/reports/tr44/#Properties
  *
  * Some special cases, like "ø" and "ß" are not being stripped/replaced by the
  * Java/Unicode normalizer so we have to replace them ourselves.
  */
trait NormalizeSupport extends Serializable {
  import java.text.Normalizer.{ normalize ⇒ jnormalize, _ }

  def replace(in: String, sub: Substitution): String =
    sub.pattern.replaceAllIn(in, sub.repl)

  def replace(in: String, substitutions: Array[Substitution]): String = {
    substitutions.foldLeft(in){
      case (result, substitution) =>
        replace(result, substitution)
    }
  }

  def cleanUp(in: String): String = {
    jnormalize(in.trim.toLowerCase, Form.NFKD)
  }

  @transient lazy val anyToLatinTrans: Transliterator =
    Transliterator.getInstance("Any-Latin")

  @transient lazy val latinToAsciiTrans: Transliterator =
    Transliterator.getInstance("Latin-ASCII")


  implicit class StringNormalizerHelper(in: String) {

    val InCombiningDiacriticalMarks =
      Substitution("\\p{InCombiningDiacriticalMarks}+".r, "")

    val InCombiningDiacriticalMarksPlus =
      Substitution("[\\p{InCombiningDiacriticalMarks}\\p{IsM}\\p{IsLm}\\p{IsSk}]+".r, "")

    val allBaseReplacements = Array(
      InCombiningDiacriticalMarksPlus,
      Substitution("ß".r, "ss"),
      Substitution("ø".r, "o")
    )

    val allReplacements: Array[Substitution] = {
      allBaseReplacements ++
        Seq(
          Substitution("'s".r, ""),
          Substitution("[^a-zA-Z0-9-]+".r, "-"),
          Substitution("-+".r, "-")
        )
    }

    def clean: String = cleanUp(in)

    def normalize: String = replace(cleanUp(in), InCombiningDiacriticalMarks)

    def normalizePlus: String = replace(cleanUp(in), InCombiningDiacriticalMarksPlus)

    def normalizeAll: String = replace(cleanUp(in), allBaseReplacements)

    def normalizeAlsoSigns: String = {
      replace(cleanUp(in), allReplacements)
        .stripSuffix("-")
    }

    val anyToLatin: String => String = anyToLatinTrans.transliterate
    val latinToAscii: String => String = latinToAsciiTrans.transliterate
    val anyToAscii: String = (anyToLatin andThen latinToAscii)(in)

    def transliterate(conversionString: String): String = {
      val trans = Transliterator.getInstance(conversionString)
      trans.transliterate(in)
    }

    def getConversionIDs: List[String] = {
      import scala.collection.JavaConversions._
      Transliterator.getAvailableIDs.toList
    }
  }
}

object NormalizeSupport extends NormalizeSupport
