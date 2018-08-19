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

  def normalize(in: String): String = {
    val cleaned = in.trim.toLowerCase
    val normalized = jnormalize(cleaned, Form.NFD).replaceAll("[\\p{InCombiningDiacriticalMarks}\\p{IsM}\\p{IsLm}\\p{IsSk}]+", "")

    normalized.replaceAll("'s", "")
      .replaceAll("ß", "ss")
      .replaceAll("ø", "o")
      .replaceAll("[^a-zA-Z0-9-]+", "-")
      .replaceAll("-+", "-")
      .stripSuffix("-")
  }
}

object NormalizeSupport extends NormalizeSupport
