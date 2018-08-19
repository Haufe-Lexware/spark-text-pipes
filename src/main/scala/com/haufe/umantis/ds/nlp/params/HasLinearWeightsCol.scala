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
  * Trait for shared param linearWeights.
  */
trait HasLinearWeightsCol extends Params {

  /**
    * Param for linear weights parameter.
    *
    * @group param
    */
  final val linearWeightsCol: Param[String] =
    new Param[String](
      this, "linearWeightsCol", "linear weights column name")

  /** @group getParam */
  final def getLinearWeightsCol: String = $(linearWeightsCol)

  /** @group setParam */
  final def setLinearWeightsCol(value: String): this.type = set(linearWeightsCol, value)
}
