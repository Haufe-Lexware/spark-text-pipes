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

import org.apache.spark.ml.param.{Params, StringArrayParam}

/**
  * Trait for shared param supportedLanguages.
  */
trait HasSupportedLanguages extends Params {

  /**
    * Param for supported languages names.
    * @group param
    */
  final val supportedLanguages: StringArrayParam =
    new StringArrayParam(this, "supportedLanguages", "supported languages")

  /** @group getParam */
  final def getSupportedLanguages: Array[String] = $(supportedLanguages)

  /** @group setParam */
  def setSupportedLanguages(values: Array[String]): this.type = {
    set(supportedLanguages, values)
  }
}