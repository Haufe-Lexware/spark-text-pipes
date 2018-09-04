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

import com.ibm.icu.text.Transliterator
import org.apache.spark.ml.param.{Param, Params}

/**
  * Trait for shared param transliteratorID.
  */
trait HasTransliterator extends Params {

  /**
    * Param for country column name.
    * @group param
    */
  final val transliteratorID: Param[String] =
    new Param[String](this, "transliteratorID", "ICU Transliterator ID")

  /** @group getParam */
  final def getTransliteratorID: String = $(transliteratorID)

  /** @group setParam */
  final def setTransliteratorID(value: String): this.type = {
    set(transliteratorID, value)
  }


  var additionalTransliteratorsAdded: Boolean = false

  final val additionalTransliterators: Param[List[Transliterator]] =
    new Param[List[Transliterator]](
      this,
      "additionalTransliteratorsList",
      "List of additional Transliterator to register"
    )

  final def getAdditionalTransliteratorsList: List[Transliterator] = $(additionalTransliterators)

  final def setAdditionalTransliteratorsList(value: List[Transliterator]): this.type = {
    additionalTransliteratorsAdded = false
    set(additionalTransliterators, value)
  }
}