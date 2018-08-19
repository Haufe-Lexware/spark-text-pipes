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
  * Trait for shared param distanceFactorCol.
  */
trait HasDistanceFactorCol extends Params {

  /**
    * Param for distance factor column parameter in score calculations.
    * @group param
    */
  final val distanceFactorCol: Param[String] =
    new Param[String](
      this,
      "distanceFactor",
      "distance factor parameter column"
    )

  /** @group getParam */
  final def getDistanceFactorCol: String = $(distanceFactorCol)

  /** @group setParam */
  final def setDistanceFactorCol(value: String): this.type = {
    set(distanceFactorCol, value)
  }
}
