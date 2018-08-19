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

package com.haufe.umantis.ds.nlp.params

import org.apache.spark.ml.param.{Param, Params}

/**
  * Trait for shared param supportedLanguages.
  */
trait HasDefaultLanguage extends Params {

  /**
    * Param for supported languages names.
    * @group param
    */
  final val defaultLanguage: Param[String] =
    new Param[String](this, "defaultLanguage", "default language")

  /** @group getParam */
  final def getDefaultLanguage: String = $(defaultLanguage)

  /** @group setParam */
  final def setDefaultLanguage(value: String): this.type = {
    set(defaultLanguage, value)
  }
}